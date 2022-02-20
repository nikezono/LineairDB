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

#include "epoch_based_rcu.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

EpochBasedRCU::EpochBasedRCU(LineairDB::EpochFramework& e)
    : RangeIndexBase(e),
      manager_stop_flag_(false),
      manager_([&]() {
        while (manager_stop_flag_.load() != true) {
          epoch_manager_ref_.Sync();
          const auto global = epoch_manager_ref_.GetGlobalEpoch();
          const auto stable_epoch = global-2llu;

          /**
           * The manager thread works the following jobs:
           * 1. update ordered index
           * 2. use free() to unnecessary entries (predicates/inserts/deletes) that belong to old epochs
           * **/

          // 1. update ordered index
          auto* new_container = new RangeIndexContainer();
          auto* old = container_.load();
          new_container->keys = old->keys;

          bool work_done = false;
          while (!work_done){
            auto ins_or_dels = insert_or_delete_key_set_.Get([stable_epoch](const auto& list){
              return list.epoch < 
              

            });
          }

        

          
          
          

          // 2. garbage collection

        }
      }){};
EpochBasedRCU::~EpochBasedRCU() {
  manager_stop_flag_.store(true);
  manager_.join();
};

std::optional<size_t> EpochBasedRCU::Scan(
    const std::string_view b, const std::string_view e,
    std::function<bool(std::string_view)> operation) {
  size_t hit       = 0;
  const auto begin = std::string(b);
  const auto end   = std::string(e);
  if (end < begin) return std::nullopt;

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

  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();
  predicate_list_.emplace_back(Predicate{begin, end, global_epoch});

  return hit;
};
bool EpochBasedRCU::Insert(const std::string_view key) {
  if (IsInPredicateSet(key)) { return false; }
  // NOTE:
  // The global epoch read here may be larger than the epoch of the transaction
  // which invokes this method.
  // It may cause unnecessary aborts(there are false positives),
  // but it won't miss any phantom anomaly (there are no false negatives).
  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();
  insert_or_delete_key_set_.push_back(
      InsertOrDeleteEvent{std::string(key), false, global_epoch});

  return true;
};

bool EpochBasedRCU::Delete(const std::string_view key) {
  if (IsInPredicateSet(key)) { return false; }
  // NOTE:
  // The global epoch read here may be larger than the epoch of the transaction
  // which invokes this method.
  // It may cause unnecessary aborts(there are false positives),
  // but it won't miss any phantom anomaly (there are no false negatives).
  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();
  insert_or_delete_key_set_.push_back(
      InsertOrDeleteEvent{std::string(key), true, global_epoch});

  return true;
};

bool EpochBasedRCU::IsInPredicateSet(const std::string_view key) {
  for (auto it = predicate_list_.begin(); it != predicate_list_.end(); it++) {
    if (it->begin <= key && key <= it->end) return true;
  }
  return false;
}

bool EpochBasedRCU::IsOverlapWithInsertOrDelete(const std::string_view begin,
                                                const std::string_view end) {
  for (auto it = insert_or_delete_key_set_.begin();
       it != insert_or_delete_key_set_.end(); it++) {
    if (begin <= it->key && it->key <= end) return true;
  }
  return false;
}

}  // namespace Index
}  // namespace LineairDB
