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

#ifndef LINEAIRDB_INDEX_OPEN_BWTREE_INDEX_HPP
#define LINEAIRDB_INDEX_OPEN_BWTREE_INDEX_HPP

#include <functional>
#include <optional>
#include <string_view>

#include "bwtree.h"
#include "index/index_base.hpp"
#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

template <typename T>
class OpenBwTreeIndex final : public IndexBase<T> {
 public:
  OpenBwTreeIndex() : maximum(150) {
    // TODO FIXME we cannot know the number of threads that accesses this index
    // since LineairDB allows client-threads to manipulate database directly.
    // Therefore, if a user spawn threads more than the `maximum`, it will
    // causes the SIGABRT of OpenBwTree;
    bwtree_.UpdateThreadLocal(maximum);
    PreHook();
  }

  T* Get(const std::string_view key) override final {
    PreHook();
    const auto k   = std::string(key);
    auto value_set = bwtree_.GetValue(k);
    if (value_set.empty()) return nullptr;
    assert(value_set.size() == 1);
    return *value_set.begin();
  }

  /**
   * @note return false if a phantom anomaly has detected.
   */
  bool Put(const std::string_view key, const T& rhs) override final {
    PreHook();
    const auto k      = std::string(key);
    auto* new_item    = new DataItem(rhs);
    const auto result = bwtree_.Insert(k, new_item);
    if (!result) { delete new_item; }
    return result;
  }

  bool Put(const std::string_view key, T&& rhs) override final {
    PreHook();
    return Put(key, rhs);
  }

  void ForcePutBlankEntry(const std::string_view key) override final {
    PreHook();
    Put(key, DataItem{});
  }

  std::optional<size_t> Scan(
      const std::string_view begin, const std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation) override final {
    PreHook();
    return Scan(begin, end,
                [&](std::string_view key, T&) { return operation(key); });
  }

  std::optional<size_t> Scan(
      const std::string_view begin, const std::optional<std::string_view> end,
      std::function<bool(std::string_view, T&)> operation) override final {
    PreHook();
    const auto b = std::string(begin);
    auto e       = begin;
    if (end.has_value()) {
      e = std::string(end.value());
      if (e < begin) return std::nullopt;
    }

    size_t hit = 0;
    auto it    = bwtree_.Begin(b);
    for (;;) {
      if (it.IsEnd() == false && b <= it->first && it->first < e) {
        hit++;
        auto cancel = operation(it->first, *it->second);
        if (cancel) break;
        it++;
      } else {
        break;
      }
    }
  }

  void ForEach(std::function<bool(std::string_view, T&)> f) override final {
    PreHook();
    auto it = bwtree_.Begin();
    while (!it.IsEnd()) {
      const auto key = it->first;
      auto value     = Get(key);
      f(key, *value);
    }
  }

  void PreHook() {
    auto thread_id = wangziqi2013::bwtree::BwTreeBase::gc_id;
    if (thread_id == -1) {
      [[maybe_unused]] auto now = NumThreads.fetch_add(1);
      assert(now < maximum);
      bwtree_.RegisterThread();
    }
  }

 private:
  wangziqi2013::bwtree::BwTree<std::string, DataItem*> bwtree_;
  size_t maximum;
  static std::atomic<size_t> NumThreads;
};

template <typename T>
std::atomic<size_t> OpenBwTreeIndex<T>::NumThreads = 0;

}  // namespace Index

}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_OPEN_BWTREE_INDEX_HPP */
