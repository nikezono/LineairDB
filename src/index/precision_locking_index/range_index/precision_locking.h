/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

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

#ifndef LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_H
#define LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <shared_mutex>
#include <string_view>

#include "types/definitions.h"
#include "util/lockfree_list.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Range-index with phantom avoidance via precision locking [1].
 * We named it PrecisionLockingIndex.
 * It consists of a sorted index, a insert/delete key set (L_u), and a predicate
 * set (L_p).
 * @note
 * The special thread updates the index periodically (per epoch). It
 * means that other worker threads fetch the (maybe stale) index. To prevent
 * anomalies caused by the stale index (i.e., to prevent phantoms), we use L_u
 * and L_p. All update operations (such as Insert/Delete) and scan operations
 * are grouped as L_u and L_p for each epoch, respectively. Updates in L_u will
 * be applied as a batch by the special thread. If a transaction detects that
 * the addition of an element to L_u satisfies with some predicate in L_p, or
 * vice versa, we will fail the transaction because a phantom may exist.
 *
 * @ref [1] https://dl.acm.org/doi/pdf/10.1145/582318.582340
 *
 */

enum class Option { Optimistic, Pessimistic };
template <Option OPT = Option::Optimistic>
class PrecisionLockingIndex {
  struct Predicate {
    std::string begin;
    std::string end;
    Predicate(std::string_view b, std::string_view e) : begin(b), end(e) {}
  };

  struct InsertOrDeleteEvent {
    std::string key;
    bool is_delete_event;
    InsertOrDeleteEvent(std::string_view k, bool i)
        : key(k), is_delete_event(i) {}
  };

  struct IndexItem {
    bool is_deleted;
  };

  using PredicateList            = Util::LockfreeList<Predicate>;
  using InsertOrDeleteKeySet     = Util::LockfreeList<InsertOrDeleteEvent>;
  using ROWEXRangeIndexContainer = std::map<std::string, IndexItem>;

 public:
  PrecisionLockingIndex();
  ~PrecisionLockingIndex();
  std::optional<size_t> Scan(const std::string_view begin,
                             const std::string_view end,
                             std::function<bool(std::string_view)> operation);
  bool Insert(const std::string_view key);
  void ForceInsert(const std::string_view key);
  bool Delete(const std::string_view key);

  bool IsInPredicateSet(const std::string_view);
  bool IsOverlapWithInsertOrDelete(const std::string_view,
                                   const std::string_view);

 private:
  std::atomic<bool> manager_stop_flag_;
  std::thread manager_;

  std::shared_mutex container_lock_;  // WANTFIX remove this locking
  PredicateList predicate_list_;
  InsertOrDeleteKeySet insert_or_delete_key_set_;

  ROWEXRangeIndexContainer container_;
};

/** Impl **/

template <Option OPT>
PrecisionLockingIndex<OPT>::PrecisionLockingIndex()
    : manager_stop_flag_(false), manager_([&]() {
        while (manager_stop_flag_.load() != true) {
          std::this_thread::sleep_for(std::chrono::milliseconds(40));

          std::lock_guard<decltype(container_lock_)> lock(container_lock_);

          // Clear predicate list
          if constexpr (OPT == Option::Pessimistic) { predicate_list_.Clear(); }

          // Before deleting, we update the index container to apply
          // insertions and deletions.
          {
            insert_or_delete_key_set_.Every([&](const auto& event) {
              container_[event.key].is_deleted = event.is_delete_event;
              return true;
            });
          }
          // Clear insert_or_delete_keys
          insert_or_delete_key_set_.Clear();
        }
      }) {}

template <Option OPT>
PrecisionLockingIndex<OPT>::~PrecisionLockingIndex() {
  manager_stop_flag_.store(true);
  manager_.join();
};

template <Option OPT>
std::optional<size_t> PrecisionLockingIndex<OPT>::Scan(
    const std::string_view b, const std::string_view e,
    std::function<bool(std::string_view)> operation) {
  size_t hit       = 0;
  const auto begin = std::string(b);
  const auto end   = std::string(e);
  if (end < begin) return std::nullopt;

  std::shared_lock<decltype(container_lock_)> lk(container_lock_);

  if (IsOverlapWithInsertOrDelete(b, e)) { return std::nullopt; }

  {
    auto it     = container_.lower_bound(begin);
    auto it_end = container_.upper_bound(end);
    for (; it != it_end; it++) {
      if (it->second.is_deleted) continue;
      hit++;
      auto cancel = operation(it->first);
      if (cancel) break;
    }
  }
  if constexpr (OPT == Option::Pessimistic) { predicate_list_.Add({b, e}); }
  return hit;
};

template <Option OPT>
bool PrecisionLockingIndex<OPT>::Insert(const std::string_view key) {
  std::shared_lock<decltype(container_lock_)> lk(container_lock_);

  if (IsInPredicateSet(key)) { return false; }
  insert_or_delete_key_set_.Add({key, false});
  return true;
};

template <Option OPT>
void PrecisionLockingIndex<OPT>::ForceInsert(const std::string_view key) {
  std::shared_lock<decltype(container_lock_)> lk(container_lock_);
  insert_or_delete_key_set_.Add({key, false});
}

template <Option OPT>
bool PrecisionLockingIndex<OPT>::Delete(const std::string_view key) {
  std::shared_lock<decltype(container_lock_)> lk(container_lock_);

  if (IsInPredicateSet(key)) { return false; }
  insert_or_delete_key_set_.Add({key, true});
  return true;
};

template <Option OPT>
bool PrecisionLockingIndex<OPT>::IsInPredicateSet(const std::string_view key) {
  if constexpr (OPT == Option::Optimistic) return false;

  return !predicate_list_.Every([&](const auto& predicate) {
    return (key < predicate.begin || predicate.end < key);
  });
}

template <Option OPT>
bool PrecisionLockingIndex<OPT>::IsOverlapWithInsertOrDelete(
    const std::string_view begin, const std::string_view end) {
  return !insert_or_delete_key_set_.Every([&](const auto& event) {
    return (event.key < begin || end < event.key);
  });
}

}  // namespace Index
}  // namespace LineairDB

#endif /*  LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_H*/
