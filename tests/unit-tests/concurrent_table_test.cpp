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
#include "util/logger.hpp"

TEST(ConcurrentTableTest, Instantiate) {
  ASSERT_NO_THROW(LineairDB::Index::ConcurrentTable table);
}

TEST(ConcurrentTableTest, Put) {
  LineairDB::Index::ConcurrentTable table;
  table.Put("alice", LineairDB::DataItem{});
}

TEST(ConcurrentTableTest, Get) {
  LineairDB::Index::ConcurrentTable table;
  ASSERT_EQ(nullptr, table.Get("alice"));
  ASSERT_TRUE(table.Put("alice", {}));
  ASSERT_NE(nullptr, table.Get("alice"));
}

TEST(ConcurrentTableTest, GetOrInsert) {
  LineairDB::Index::ConcurrentTable table;
  ASSERT_NE(nullptr, table.GetOrInsert("alice"));
}

TEST(ConcurrentTableTest, ConcurrentInserting) {
  std::vector<std::thread> threads;
  LineairDB::Index::ConcurrentTable table;

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back(
        [&, i]() { ASSERT_TRUE(table.Put(std::to_string(i), {})); });
  }
  for (auto& thread : threads) { thread.join(); }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_NE(nullptr, table.Get(std::to_string(i)));
  }
}

TEST(ConcurrentTableTest, ConcurrentAndConflictedInserting) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem> items(10);
  LineairDB::Index::ConcurrentTable table;

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&]() { table.Put("alice", {}); });
  }
  for (auto& thread : threads) { thread.join(); }
  bool some_item_were_inserted = false;
  auto* item                   = table.Get("alice");
  for (size_t i = 0; i < 10; i++) {
    if (item != nullptr) some_item_were_inserted = true;
  }

  ASSERT_TRUE(some_item_were_inserted);
}

TEST(ConcurrentTableTest, Scan) {
  LineairDB::Util::SetUpSPDLog();
  LineairDB::Index::ConcurrentTable table;
  ASSERT_TRUE(table.Put("alice", {}));
  ASSERT_TRUE(table.Put("bob", {}));
  ASSERT_TRUE(table.Put("carol", {}));

  auto count = table.Scan("alice", "carol", [](auto) { return false; });
  if (count.has_value()) {
    ASSERT_TRUE(count.value() == 0 || count.value() == 3);
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));  // TODO define API

  auto count_synced = table.Scan("alice", "carol", [](auto) { return false; });

  if (count_synced.has_value()) { ASSERT_EQ(3llu, count_synced.value()); }

  auto count_canceled = table.Scan("alice", "carol", [](auto) { return true; });
  if (count_canceled.has_value()) { ASSERT_EQ(1llu, count_canceled.value()); }
}

TEST(ConcurrentTableTest, TremendousPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::Index::ConcurrentTable table;

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}

TEST(ConcurrentTableTest, TremendousGetAndPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::Index::ConcurrentTable table;

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Get(std::to_string(j - working_set_size));
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}
