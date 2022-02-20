/*
 *   Copyright (c) 2021 Nippon Telegraph and Telephone Corporation
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

#include "util/lock_free_list.hpp"

#include "gtest/gtest.h"

using namespace LineairDB::Util;

TEST(LockFreeListTest, Instantiate) {
  ASSERT_NO_FATAL_FAILURE(LockFreeSinglyLinkedList<int>());
}
TEST(LockFreeListTest, AddToHead) {
  LockFreeSinglyLinkedList<int> list;
  ASSERT_NO_FATAL_FAILURE(list.AddToHead(1));
}
TEST(LockFreeListTest, Head) {
  LockFreeSinglyLinkedList<int> list;
  ASSERT_NO_FATAL_FAILURE(list.AddToHead(1));
  ASSERT_EQ(list.Head().entry, 1);
}
TEST(LockFreeListTest, Get) {
  auto value = 0xDEADBEEF;
  LockFreeSinglyLinkedList<decltype(value)> list;

  // does not have value
  {
    auto result = list.Get([&](const auto& stored) { return stored == value; });
    ASSERT_FALSE(result.has_value());
  }

  list.AddToHead(value);

  // has value
  {
    auto result = list.Get([&](const auto& stored) { return stored == value; });
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(*result.value(), value);
  }

  auto v2 = 0xDEAD;
  auto v3 = 0xBEEF;
  list.AddToHead(v2);
  list.AddToHead(v3);

  // skip unspecified value
  {
    auto result = list.Get([&](const auto& stored) { return stored == v3; });
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(*result.value(), v3);
  }
}
TEST(LockFreeListTest, ThreadUnsafeDelete) {
  auto value = 42;
  LockFreeSinglyLinkedList<decltype(value)> list;
  list.AddToHead(value - 1);
  list.AddToHead(value);
  list.AddToHead(value + 1);

  auto deleted =
      list.ThreadUnsafeDelete([&](const auto& stored) { return stored == value; });
  ASSERT_EQ(deleted, 2llu);
  ASSERT_FALSE(
      list.Get([&](const auto& s) { return s == value - 1; }).has_value());
  ASSERT_FALSE(list.Get([&](const auto& s) { return s == value; }).has_value());
  ASSERT_TRUE(
      list.Get([&](const auto& s) { return s == value + 1; }).has_value());
}
