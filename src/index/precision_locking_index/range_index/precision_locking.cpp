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

#include "precision_locking.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/logger.hpp"

namespace LineairDB {
namespace Index {

PrecisionLockingIndex::PrecisionLockingIndex()
    : manager_stop_flag_(false), manager_([&]() {
        while (manager_stop_flag_.load() != true) {
          std::this_thread::sleep_for(std::chrono::milliseconds(40));

          std::lock_guard<decltype(container_lock_)> lock(container_lock_);

          // Clear predicate list
          predicate_list_.Clear();

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

PrecisionLockingIndex::~PrecisionLockingIndex() {
  manager_stop_flag_.store(true);
  manager_.join();
};

std::optional<size_t> PrecisionLockingIndex::Scan(
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

  predicate_list_.Add({b, e});
  return hit;
};

bool PrecisionLockingIndex::Insert(const std::string_view key) {
  std::shared_lock<decltype(container_lock_)> lk(container_lock_);

  if (IsInPredicateSet(key)) { return false; }
  insert_or_delete_key_set_.Add({key, false});
  return true;
};

void PrecisionLockingIndex::ForceInsert(const std::string_view key) {
  std::shared_lock<decltype(container_lock_)> lk(container_lock_);
  insert_or_delete_key_set_.Add({key, false});
}

bool PrecisionLockingIndex::Delete(const std::string_view key) {
  std::shared_lock<decltype(container_lock_)> lk(container_lock_);

  if (IsInPredicateSet(key)) { return false; }
  insert_or_delete_key_set_.Add({key, true});
  return true;
};

bool PrecisionLockingIndex::IsInPredicateSet(const std::string_view key) {
  return !predicate_list_.Every([&](const auto& predicate) {
    if (predicate.begin <= key && key <= predicate.end) return false;
    return true;
  });
}

bool PrecisionLockingIndex::IsOverlapWithInsertOrDelete(
    const std::string_view begin, const std::string_view end) {
  return !insert_or_delete_key_set_.Every([&](const auto& event) {
    if (begin <= event.key && event.key <= end) return false;
    return true;
  });
}

}  // namespace Index
}  // namespace LineairDB
