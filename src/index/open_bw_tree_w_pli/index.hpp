/*
 *   Copyright (c) 2022 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef LINEAIRDB_INDEX_OPEN_BW_TREE_WITH_PRECISION_LOCKING_INDEX_INDEX_HPP
#define LINEAIRDB_INDEX_OPEN_BW_TREE_WITH_PRECISION_LOCKING_INDEX_INDEX_HPP

#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include "index/index_base.hpp"
#include "index/open_bw_tree/index.hpp"
#include "index/precision_locking_index/point_index/mpmc_concurrent_set_impl.hpp"
#include "util/lockfree_list.hpp"

namespace LineairDB {

namespace Index {

enum class BwOption { Optimistic, Pessimistic };

enum Status { Running, Committed, Aborted };

  struct Predicate {
    Status status;
    std::string begin;
    std::string end;
    Predicate(std::string_view b, std::string_view e)
        : status(Running), begin(b), end(e) {}
  };

  struct InsertOrDeleteEvent {
    Status status;
    std::string key;
    bool is_delete_event;
    InsertOrDeleteEvent(std::string_view k, bool i)
        : status(Running), key(k), is_delete_event(i) {}
  };

template <typename T, BwOption OPT = BwOption::Optimistic>
class OpenBwTreeWithPrecisionLockingIndex final : public IndexBase<T> {
 public:

  using PredicateList            = Util::LockfreeList<Predicate>;
  using InsertOrDeleteKeySet = Util::LockfreeList<InsertOrDeleteEvent>;

 private:
  std::atomic<bool> manager_stop_flag_;
  std::thread manager_;

  PredicateList predicate_list_;
  InsertOrDeleteKeySet insert_or_delete_key_set_;
  std::vector<void*> p_garbage_;
  std::vector<void*> u_garbage_;

  OpenBwTreeIndex<T> bw_tree_;
  MPMCConcurrentSetImpl<T> point_index_;

 public:
  OpenBwTreeWithPrecisionLockingIndex()
      : manager_stop_flag_(false), manager_([&]() {
          while (manager_stop_flag_.load() != true) {
            std::this_thread::yield();

            // remove committed or aborted predicates
            if constexpr (OPT == BwOption::Pessimistic) {
              auto* node = predicate_list_.head_.load();
              auto* prev = node;
              auto* deletable_prev = node;

              // Look for a node with "Status is not Running" for all subsequent
              // nodes.
              while (node != nullptr) {
                if (node->value.status == Running) {
                  deletable_prev = prev;
                }
                prev = node;
                node = node->next.load();
              }
              if (prev != deletable_prev) {
                auto* deletable_head = deletable_prev->next.load();
                if (predicate_list_.head_.load() == deletable_head) {
                  predicate_list_.head_.compare_exchange_strong(deletable_head,
                                                                nullptr);
                }
                deletable_prev->next.store(nullptr);  // removed
                // SPDLOG_ERROR("p_garbage {} to {}",
                // deletable_head->value.begin,
                //              deletable_head->value.end);
                p_garbage_.emplace_back(deletable_prev);  // TODO delete nodes
              }
            }

            // remove committed or aborted insertions and deletions, and apply
            {
              auto* node = insert_or_delete_key_set_.head_.load();
              auto* prev = node;
              auto* deletable_prev = node;

              // Look for a node with "Status is not Running" for all subsequent
              // nodes.
              while (node != nullptr) {
                if (node->value.status == Running) {
                  deletable_prev = prev;
                }
                prev = node;
                node = node->next.load();
              }

              if (prev != deletable_prev) {
                // apply insertion and deletions for all committed events
                for (auto* n = deletable_prev; n != nullptr;
                     n = n->next.load()) {
                  if (n->value.status == Committed) {
                    // SPDLOG_ERROR("u_garbage {} {}", n->value.key,
                    //             n->value.is_delete_event);
                    if (n->value.is_delete_event) {
                      T* data = point_index_.Get(n->value.key);
                      *data = T();  // initialize;
                    } else {
                      point_index_.Put(n->value.key, {});
                    }
                  }
                }
                if (insert_or_delete_key_set_.head_.load() == deletable_prev) {
                  insert_or_delete_key_set_.head_.compare_exchange_strong(
                      deletable_prev, nullptr);
                }
                deletable_prev->next.store(nullptr);      // removed
                u_garbage_.emplace_back(deletable_prev);  // TODO delete nodes
              }
            }
          }
        }) {}

  ~OpenBwTreeWithPrecisionLockingIndex() {
    manager_stop_flag_.store(true);
    manager_.join();
  }

  T* Get(const std::string_view key) override final {
    return point_index_.Get(key);
  }

  /**
   * @note return false if a phantom anomaly has detected.
   */
  bool Put(const std::string_view key, const T& rhs,
           PredicateSetType* predicate_set) override final {
    {
      if (IsInPredicateSet(key)) {
        return false;
      }
      auto* n = insert_or_delete_key_set_.Add({key, false});
      if (predicate_set) predicate_set->push_back(n);
      if (IsInPredicateSet(key)) {
        return false;
      }
    }
    auto* value = new T(rhs);
    bool p_success = point_index_.Put(key, value);
    if (!p_success) delete value;
    return true;
  }

  void ForcePutBlankEntry(const std::string_view key,
                          PredicateSetType* predicate_set) override final {
    auto* new_entry = new T();
    if (!point_index_.Put(key, new_entry)) {
      delete new_entry;  // already inserted
      return;
    }
    auto* n = insert_or_delete_key_set_.Add({key, false});
    if (predicate_set) predicate_set->push_back(n);
  }

  /**
   * @brief Scan with key and values
   *
   * @param begin Starting point of the range. Matching entry is included.
   * @param end Ending point of the range. Matching entry isn't included.
   * @param operation This callback function will be invoked for every entry
   * matching the range, The key/value pair will be given as an argument.
   * @return std::optional<size_t> returns std::nullopt if a phantom anomaly has
   * detected.
   */
  std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      PredicateSetType* predicate_set,
      std::function<bool(std::string_view, T&)> operation) override final {
    return Scan(begin, end, predicate_set, [&](std::string_view key) {
      auto* value = Get(key);
      return operation(key, *value);
    });
  }

  /**
   * @brief Scan without values; that is, an interface to collect only keys from
   * range index.
   */
  std::optional<size_t> Scan(
      const std::string_view b, const std::string_view e,
      PredicateSetType* predicate_set,
      std::function<bool(std::string_view)> operation) override final {
    const auto begin = std::string(b);
    const auto end   = std::string(e);
    if (end < begin) return std::nullopt;

    {
      if (IsOverlapWithInsertOrDelete(b, e)) { return std::nullopt; }
      if constexpr (OPT == BwOption::Pessimistic) {
        auto* n = predicate_list_.Add({b, e});
        if (predicate_set) predicate_set->push_back(n);
        if (IsOverlapWithInsertOrDelete(b, e)) {
          return std::nullopt;
        }
      }
    }
    return bw_tree_.Scan(begin, end, predicate_set, operation);
  }

  bool ReScan(const std::string_view begin,
              const std::string_view end) override final {
    return !IsOverlapWithInsertOrDelete(begin, end);
  }

  void ForEach(std::function<bool(std::string_view, T&)> f) override final {
    bw_tree_.ForEach(f);
  }

  bool IsInPredicateSet(const std::string_view key) {
    if constexpr (OPT == BwOption::Optimistic) return false;

    return !predicate_list_.Every([&](const auto& predicate) {
      return (key < predicate.begin || predicate.end < key);
    });
  }

  bool IsOverlapWithInsertOrDelete(const std::string_view begin,
                                   const std::string_view end) {
    return !insert_or_delete_key_set_.Every([&](const auto& event) {
      return (event.key < begin || end < event.key);
    });
  }
};

}  // namespace Index

}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_OPEN_BW_TREE_WITH_PRECISION_LOCKING_INDEX_INDEX_HPP \
        */
