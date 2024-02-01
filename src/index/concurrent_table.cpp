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

#include "concurrent_table.h"

#include <functional>
#include <memory>

#include "index/open_bw_tree/index.hpp"
#include "index/open_bw_tree_w_pli/index.hpp"
#include "index/precision_locking_index/index.hpp"
#include "lineairdb/config.h"
#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

ConcurrentTable::ConcurrentTable(Config config, WriteSetType recovery_set) {
  switch (config.index_structure) {
    case Config::IndexStructure::OpenBwTree:
      index_ = std::make_unique<OpenBwTreeIndex<DataItem>>();
      break;
    case Config::IndexStructure::OpenBwTreeWithPLI:
      index_ = std::make_unique<OpenBwTreeWithPrecisionLockingIndex<
          DataItem, BwOption::Pessimistic>>();
      break;
    case Config::IndexStructure::OpenBwTreeWithOPLI:
      index_ = std::make_unique<OpenBwTreeWithPrecisionLockingIndex<
          DataItem, BwOption::Optimistic>>();
      break;
    default:
      index_ =
          std::make_unique<OpenBwTreeWithPrecisionLockingIndex<DataItem>>();
      break;
  }

  if (recovery_set.empty()) return;
}

DataItem* ConcurrentTable::Get(const std::string_view key) {
  return index_->Get(key);
}

DataItem* ConcurrentTable::GetOrInsert(const std::string_view key,
                                       PredicateSetType* predicate_set) {
  auto* item = index_->Get(key);
  if (item == nullptr) {
    index_->ForcePutBlankEntry(key, predicate_set);
    item = index_->Get(key);
    assert(item != nullptr);
  }
  return item;
}

// return false if a corresponding entry already exists
bool ConcurrentTable::Put(const std::string_view key, const DataItem& rhs,
                          PredicateSetType* p) {
  return index_->Put(key, std::forward<decltype(rhs)>(rhs), p);
}

void ConcurrentTable::ForEach(
    std::function<bool(std::string_view, DataItem&)> f) {
  index_->ForEach(f);
};

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::string_view end,
    PredicateSetType* predicate_set,
    std::function<bool(std::string_view)> operation) {
  return index_->Scan(begin, end, predicate_set, operation);
};

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::string_view end,
    PredicateSetType* predicate_set,
    std::function<bool(std::string_view, DataItem&)> operation) {
  return index_->Scan(begin, end, predicate_set, operation);
};

bool ConcurrentTable::ReScan(const std::string_view begin,
                             const std::string_view end) {
  return index_->ReScan(begin, end);
};

}  // namespace Index
}  // namespace LineairDB
