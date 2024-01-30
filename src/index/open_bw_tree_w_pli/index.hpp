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

#include "index/index_base.hpp"
#include "index/open_bw_tree/index.hpp"
#include "index/precision_locking_index/point_index/mpmc_concurrent_set_impl.hpp"
#include "util/lockfree_list.hpp"

namespace LineairDB {

namespace Index {

enum class BwOption { Optimistic, Pessimistic };

template <typename T, BwOption OPT = BwOption::Optimistic>
class OpenBwTreeWithPrecisionLockingIndex final : public IndexBase<T> {
  enum Status { Committed, Aborted, Running };

  struct Predicate {
    EpochNumber epoch;
    std::string begin;
    std::string end;
    Status status;

    Predicate(EpochNumber ep, std::string_view b, std::string_view e)
        : epoch(ep), begin(b), end(e), status(Running) {}
  };

  struct InsertOrDeleteEvent {
    EpochNumber epoch;
    std::string key;
    bool is_delete_event;
    Status status;

    InsertOrDeleteEvent(EpochNumber ep, std::string_view k, bool i)
        : epoch(ep), key(k), is_delete_event(i), status(Running) {}
  };

  struct IndexItem {
    bool is_deleted;
  };

  using PredicateList            = Util::LockfreeList<Predicate>;
  using InsertOrDeleteKeySet     = Util::LockfreeList<InsertOrDeleteEvent>;
  using ROWEXRangeIndexContainer = std::map<std::string, IndexItem>;

 private:
  LineairDB::EpochFramework& epoch_manager_;
  std::atomic<bool> manager_stop_flag_;
  std::thread manager_;

  PredicateList predicate_list_;
  InsertOrDeleteKeySet insert_or_delete_key_set_;
  ROWEXRangeIndexContainer container_;

  OpenBwTreeIndex<T> bw_tree_;
  MPMCConcurrentSetImpl<T> point_index_;

 public:
  OpenBwTreeWithPrecisionLockingIndex(EpochFramework& e)
      : epoch_manager_(e), manager_stop_flag_(false), manager_([&]() {
          while (manager_stop_flag_.load() != true) {
            epoch_manager_.Sync();
            const auto current_epoch = epoch_manager_.GetGlobalEpoch();

            // Clear predicate list
            if constexpr (OPT == BwOption::Pessimistic) {
              // TODO: predicate_list_ のなかで， epoch+1 < current_epoch
              // なものをdeleteしていく

              // predicate_list_.Delete([](const auto& predicate) {
              // predicate.epoch + 1 < current_epoch; });
            }
            // Before deleting, we update the index container to apply
            // insertions and deletions.
            {
              insert_or_delete_key_set_.Every([&](const auto& event) {
                // TODO:  epoch+1 < current_epoch なものをdelete, applyしていく

                if (event.epoch + 1 < current_epoch) {
                  bw_tree_.Put(event.key, {});
                }
                // insert_or_delete_key_set_.Delete(event);
                return true;
              });
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
  bool Put(const std::string_view key, const T& rhs) override final {
    {
      const auto epoch = epoch_manager_.GetGlobalEpoch();
      if (IsInPredicateSet(key)) { return false; }
      insert_or_delete_key_set_.Add({epoch, key, false});
      if (IsInPredicateSet(key)) { return false; }
    }
    auto* value    = new T(rhs);
    bool p_success = point_index_.Put(key, value);
    if (!p_success) delete value;
    return true;
  }

  bool Put(const std::string_view key, T&& rhs) override final {
    return Put(key, rhs);
  }

  void ForcePutBlankEntry(const std::string_view key) override final {
    auto* new_entry = new T();
    if (!point_index_.Put(key, new_entry))
      delete new_entry;  // already inserted
    const auto epoch = epoch_manager_.GetGlobalEpoch();

    insert_or_delete_key_set_.Add({epoch, key, false});
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
      std::function<bool(std::string_view, T&)> operation) override final {
    return Scan(begin, end, [&](std::string_view key) {
      auto* value = Get(key);
      return operation(key, *value);
    });
  }

  bool ReScan(const std::string_view begin,
              const std::string_view end) override final {
    return !IsOverlapWithInsertOrDelete(begin, end);
  }

  /**
   * @brief Scan without values; that is, an interface to collect only keys from
   * range index.
   */
  std::optional<size_t> Scan(
      const std::string_view b, const std::string_view e,
      std::function<bool(std::string_view)> operation) override final {
    const auto begin = std::string(b);
    const auto end   = std::string(e);
    if (end < begin) return std::nullopt;

    const auto epoch = epoch_manager_.GetGlobalEpoch();

    {
      if (IsOverlapWithInsertOrDelete(b, e)) { return std::nullopt; }
      if constexpr (OPT == BwOption::Pessimistic) {
        predicate_list_.Add({epoch, b, e});
        if (IsOverlapWithInsertOrDelete(b, e)) { return std::nullopt; }
      }
    }
    return bw_tree_.Scan(begin, end, operation);
  }

  void ForEach(std::function<bool(std::string_view, T&)> f) override final {
    bw_tree_.ForEach(f);
  }

  bool IsInPredicateSet(const std::string_view key) {
    if constexpr (OPT == BwOption::Optimistic) return false;

    const auto epoch = epoch_manager_.GetGlobalEpoch();

    return !predicate_list_.Every([&](const auto& predicate) {
      if (predicate.epoch + 1 < epoch) return true;  // it's obsolete. okay.
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
