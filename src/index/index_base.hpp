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

#ifndef LINEAIRDB_INDEX_INDEX_BASE_HPP
#define LINEAIRDB_INDEX_INDEX_BASE_HPP

#include <functional>
#include <optional>
#include <string_view>

namespace LineairDB {

namespace Index {

template <typename T>
class IndexBase {
 public:
  virtual ~IndexBase() {}
  virtual T* Get(const std::string_view)                            = 0;
  virtual bool Put(const std::string_view, T&&)                     = 0;
  virtual bool Put(const std::string_view, const T&)                = 0;
  virtual void ForcePutBlankEntry(const std::string_view)           = 0;
  virtual void ForEach(std::function<bool(std::string_view, T&)> f) = 0;

  /**
   * @brief Scan with key and values
   *
   * @param begin Starting point of the range. Matching entry is included.
   * @param end Ending point of the range. Matching entry isn't included.
   * @param operation This callback function will be invoked for every entry
   * matching the range, The key/value pair will be given as an argument.
   * @return std::optional<size_t> returns std::nullopt if a phantom anomaly has
   * detected.
   */
  virtual std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view)> operation) = 0;

  virtual std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view, T&)> operation) = 0;

  virtual bool ReScan(const std::string_view, const std::string_view) = 0;
};

}  // namespace Index

}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_INDEX_BASE_HPP */
