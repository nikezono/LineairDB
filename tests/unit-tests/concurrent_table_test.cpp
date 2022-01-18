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

#include "index/concurrent_table.h"

#include <thread>

#include "gtest/gtest.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

TEST(ConcurrentTableInitialTest, Instantiate) {
  LineairDB::EpochFramework epoch;
  epoch.Start();
  ASSERT_NO_THROW(LineairDB::Index::ConcurrentTable table(epoch));
}

class ConcurrentTableTest
    : public ::testing::TestWithParam<LineairDB::Config::RangeIndex> {
  virtual void SetUp() {
    epoch.Start();
    table = new LineairDB::Index::ConcurrentTable(epoch);
  }
  virtual void TearDown() {
    epoch.Stop();
    delete table;
  }

 protected:
  LineairDB::EpochFramework epoch;
  LineairDB::Index::ConcurrentTable* table;
};

const std::array<std::string, 2> Impls{"LockBasedIndex", "EpochBasedRCU"};
INSTANTIATE_TEST_CASE_P(
    LineairDB, ConcurrentTableTest,
    ::testing::Values(LineairDB::Config::RangeIndex::LockBasedIndex,
                      LineairDB::Config::RangeIndex::EpochBasedRCU),
    [](const testing::TestParamInfo<LineairDB::Config::RangeIndex>& param) {
      return Impls[param.index];
    });

TEST_P(ConcurrentTableTest, Put) { table->Put("alice", LineairDB::DataItem{}); }

TEST_P(ConcurrentTableTest, Get) {
  ASSERT_EQ(nullptr, table->Get("alice"));
  table->Put("alice", {});
  ASSERT_NE(nullptr, table->Get("alice"));
}

TEST_P(ConcurrentTableTest, GetOrInsert) {
  ASSERT_NE(nullptr, table->GetOrInsert("alice"));
}

TEST_P(ConcurrentTableTest, ConcurrentInserting) {
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() { table->Put(std::to_string(i), {}); });
  }
  for (auto& thread : threads) { thread.join(); }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_NE(nullptr, table->Get(std::to_string(i)));
  }
}

TEST_P(ConcurrentTableTest, ConcurrentAndConflictedInserting) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem> items(10);

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&]() { table->Put("alice", {}); });
  }
  for (auto& thread : threads) { thread.join(); }
  bool some_item_were_inserted = false;
  auto* item                   = table->Get("alice");
  for (size_t i = 0; i < 10; i++) {
    if (item != nullptr) some_item_were_inserted = true;
  }

  ASSERT_TRUE(some_item_were_inserted);
}

TEST_P(ConcurrentTableTest, Scan) {
  ASSERT_TRUE(table->Put("alice", {}));
  ASSERT_TRUE(table->Put("bob", {}));
  ASSERT_TRUE(table->Put("carol", {}));

  auto count = table->Scan("alice", "carol", [](auto) { return false; });
  if (count.has_value()) { ASSERT_EQ(3, count.value()); }
  epoch.Sync();
  epoch.Sync();
  auto count_synced = table->Scan("alice", "carol", [](auto) { return false; });

  if (count_synced.has_value()) { ASSERT_EQ(3, count_synced.value()); }

  auto count_canceled =
      table->Scan("alice", "carol", [](auto) { return true; });
  if (count_canceled.has_value()) { ASSERT_EQ(1, count_canceled.value()); }
}

TEST_P(ConcurrentTableTest, TremendousPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table->Put(std::to_string(j), {});
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}

TEST_P(ConcurrentTableTest, TremendousGetAndPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table->Get(std::to_string(j - working_set_size));
        table->Put(std::to_string(j), {});
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}
