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

#ifndef LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H
#define LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <mutex>
#include <new>
#include <string_view>
#include <vector>

#include "libcuckoo/cuckoohash_map.hh"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Multi-Producer Multi-Consumer (MPMC) hash-table,
 * based on the open addressing & linear-probing strategy.
 * @note We focus on the performance for reads (gets), not for writes (puts).
 * In other words, we provide lock-free #Get and (maybe locking) #Put.
 * This is because LineairDB requires that point-indexes have to
 * hold only indirection pointer to each data item; once an indirection is
 * created and stored into the index, it will not be changed by #puts.
 */

template <typename T>
class MPMCConcurrentSetImpl {
 public:
  MPMCConcurrentSetImpl() {}
  ~MPMCConcurrentSetImpl() {}
  T* Get(const std::string_view key) {
    T* return_value_p = nullptr;
    container_.find(std::string{key}, return_value_p);
    return return_value_p;
  }

  bool Put(const std::string_view key, const T* const value_p) {
    return container_.insert(std::string{key}, const_cast<T*>(value_p));
  }
  void Clear() { container_.clear(); }
  void ForEach(std::function<bool(std::string_view, T&)> f) {
    auto lt = container_.lock_table();
    for (const auto& it : lt) { f(it.first, *it.second); }
  }

 private:
  libcuckoo::cuckoohash_map<std::string, T*> container_;
};

}  // namespace Index
}  // namespace LineairDB
#endif /* LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H */
