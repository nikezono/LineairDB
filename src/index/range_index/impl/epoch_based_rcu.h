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

#ifndef LINEAIRDB_INDEX_EPOCH_BASED_RCU_H
#define LINEAIRDB_INDEX_EPOCH_BASED_RCU_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <set>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include "index/range_index/range_index_base.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Epoch-based RCU Index.
 */
class EpochBasedRCU final : public RangeIndexBase {
 public:
  EpochBasedRCU(LineairDB::EpochFramework&);
  ~EpochBasedRCU() final override;
  std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view)> operation) final override;
  bool Insert(const std::string_view key) final override;
  bool Delete(const std::string_view key) final override;

 private:
  bool IsInPredicateSet(const std::string_view);
  bool IsOverlapWithInsertOrDelete(const std::string_view,
                                   const std::string_view);

  struct Predicate {
    std::string begin;
    std::string end;
    EpochNumber epoch;
  };

  struct InsertOrDeleteEvent {
    std::string key;
    bool is_delete_event;
    EpochNumber epoch;
  };

  struct IndexItem {
    bool is_deleted;
  };

  using PredicateList        = std::vector<Predicate>;
  using InsertOrDeleteKeySet = std::vector<InsertOrDeleteEvent>;
  using RangeIndexContainer  = std::map<std::string, IndexItem>;

  PredicateList predicate_list_;
  InsertOrDeleteKeySet insert_or_delete_key_set_;
  RangeIndexContainer container_;

  size_t indexed_epoch_;
  std::atomic<bool> manager_stop_flag_;
  std::thread manager_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /*  LINEAIRDB_INDEX_EPOCH_BASED_RCU_H*/
