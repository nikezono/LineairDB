/*
 *   Copyright (C) 2020-2022 Nippon Telegraph and Telephone Corporation.

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

#include "util/lockfree_list.hpp"

#include <future>
#include <random>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

TEST(LockfreeListTest, Instantiate) {
  ASSERT_NO_THROW(LineairDB::Util::LockfreeList<int> list);
}

TEST(LockfreeListTest, Add) {
  LineairDB::Util::LockfreeList<int> list;
  list.Add(1);
}

TEST(LockfreeListTest, AddNonTrivialType) {
  struct NonTrivialType {
    char* foo;
    int bar;
    NonTrivialType() : foo(nullptr), bar(0xDEADBEEF) {}
  };

  LineairDB::Util::LockfreeList<NonTrivialType> list;
  list.Add({});
}

TEST(LockfreeListTest, Every) {
  LineairDB::Util::LockfreeList<int> list;
  for (size_t i = 0; i < 10; ++i) { list.Add(i); }

  size_t i   = 9;
  bool every = list.Every([&](const auto& item) {
    EXPECT_EQ(i, item);
    --i;  // we assumes N2O: newest to oldest
    return true;
  });
  ASSERT_EQ(i, -1);  // decremented 10 times
  ASSERT_TRUE(every);
}

TEST(LockfreeListTest, Size) {
  LineairDB::Util::LockfreeList<int> list;
  for (size_t i = 0; i < 10; ++i) { list.Add(i); }
  ASSERT_EQ(10, list.Size());
}

TEST(LockfreeListTest, AddIfHeadSatisfies) {
  LineairDB::Util::LockfreeList<int> list;
  ASSERT_TRUE(list.AddIfHeadSatisfies(0, [](const auto&) { return true; }));
  ASSERT_TRUE(list.AddIfHeadSatisfies(1, [](const auto&) { return true; }));
  ASSERT_FALSE(
      list.AddIfHeadSatisfies(1, [](const auto& item) { return item != 1; }));
  ASSERT_EQ(2, list.Size());
}

TEST(LockfreeListTest, Add_ViaMultiThreads) {
  LineairDB::Util::LockfreeList<int> list;
  {
    std::vector<std::future<void>> futures;
    for (size_t i = 0; i < 1000; ++i) {
      futures.push_back(
          std::async(std::launch::async, [&, i]() { list.Add(i); }));
    }
  }
  ASSERT_EQ(1000, list.Size());
}

TEST(LockfreeListTest, AddIfHeadSatisfies_ViaMultiThreads) {
  LineairDB::Util::LockfreeList<int> list;
  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < 1000; ++i) {
    futures.push_back(std::async(std::launch::async, [&]() {
      list.AddIfHeadSatisfies(0, [](const auto& item) { return item != 0; });
    }));
  }
  ASSERT_EQ(1, list.Size());
}
