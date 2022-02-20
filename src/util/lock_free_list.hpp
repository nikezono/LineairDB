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

#ifndef LINEAIRDB_UTIL_LOCKFREE_LIST_HPP
#define LINEAIRDB_UTIL_LOCKFREE_LIST_HPP

#include <atomic>
#include <functional>
#include <mutex>
#include <optional>

namespace LineairDB {
namespace Util {

template <typename T>
class LockFreeSinglyLinkedList {
 public:
  struct Node {
    T entry;
    std::atomic<Node*> next;
    Node(const T& e, Node* p) : entry(e), next(p) {}
  };

 public:
  LockFreeSinglyLinkedList() : head_(nullptr) {}
  ~LockFreeSinglyLinkedList() {
    ThreadUnsafeDelete([]() { return true; })
  }
  void AddToHead(const T& entry) {
    auto* new_entry = new Node(entry, nullptr);
    for (;;) {
      auto* h = head_.load();
      new_entry->next.store(h, std::memory_order_relaxed);

      if (head_.compare_exchange_weak(h, new_entry)) { break; }
    }
  }

  // TODO: cooperative (multi-threaded) delete API
  /**
   * @brief deletes specified entry and its subsequent entries.
   * @note this function assumes that there is no thread which accesses the
   * specified entries (i.e., it is not thread-safe deletion).
   * @param deleter
   * @return size_t
   */
  size_t ThreadUnsafeDelete(const std::function<bool(const T&)> deleter) {
    auto* head                   = head_.load();
    auto* curr                   = head;
    auto* prev                   = head;
    decltype(curr) delete_target = nullptr;
    while (curr != nullptr) {
      if (deleter(curr->entry)) {
        if (curr == head) {
          head_.store(nullptr);
          delete_target = curr;
          break;
        } else {
          prev->next.store(nullptr);
          delete_target = curr;
          break;
        }
      }
      curr = curr->next.load();
    }
    size_t hit = 0;
    while (delete_target != nullptr) {
      auto* old     = delete_target;
      delete_target = delete_target->next.load();
      delete old;
      hit++;
    }
    return hit;
  }

  std::optional<T*> Get(const std::function<bool(const T&)> predicate) {
    auto* curr = head_.load();
    Node* hit  = nullptr;
    while (curr != nullptr) {
      if (predicate(curr->entry)) {
        hit = curr;
        break;
      }
      curr = curr->next.load();
    }
    if (hit) return &hit->entry;
    return std::nullopt;
  }

  Node& Head() { return *head_.load(); }

 private:
  std::atomic<Node*> head_;
  std::atomic<Node*> free_list_;
};

}  // namespace Util
}  // namespace LineairDB

#endif /* LINEAIRDB_UTIL_LOCKFREE_LIST_HPP */
