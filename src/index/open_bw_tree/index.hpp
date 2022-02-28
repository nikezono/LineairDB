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

#include "index/index_base.hpp"
#include "bwtree.h"

namespace LineairDB {

namespace Index {

template <typename T>
class OpenBwTreeIndex final : public IndexBase<T> {
 public:
  OpenBwTreeIndex() {}

  T* Get(const std::string_view key) override final { return nullptr; }

  /**
   * @note return false if a phantom anomaly has detected.
   */
  bool Put(const std::string_view key, const T& rhs) override final {
    return true;
  }

  bool Put(const std::string_view key, T&& rhs) override final {
    return Put(key, rhs);
  }

  void ForcePutBlankEntry(const std::string_view key) override final {}

  std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view, T&)> operation) override final {
    return Scan(begin, end, [&](std::string_view key) {
      auto* value = Get(key);
      return operation(key, *value);
    });
  }

  /**
   * @brief Scan without values; that is, an interface to collect only keys from
   * range index.
   */
  std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view)> operation) override final {
    return 0;
  }

  void ForEach(std::function<bool(std::string_view, T&)> f) override final {}

 private:
};

}  // namespace Index

}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_OPEN_BWTREE_INDEX_HPP */
