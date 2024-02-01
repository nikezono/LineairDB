#ifndef LINEAIRDB_UTIL_LOCKFREE_LIST_HPP
#define LINEAIRDB_UTIL_LOCKFREE_LIST_HPP

#include <atomic>
#include <cstddef>
#include <functional>
#include <vector>

#include "util/logger.hpp"

namespace LineairDB {
namespace Util {

template <typename T>
class LockfreeList {
 public:
  struct Node;
  using Functor     = std::function<bool(const T&)>;
  using NodeFunctor = std::function<bool(Node*)>;

  std::atomic<Node*> head_;
  std::vector<Node*> garbages_;

 public:
  LockfreeList() : head_(nullptr) {}

  ~LockfreeList() {
    for (auto* garbage : garbages_) {
      ForEach(garbage, [&](auto* node) {
        delete node;
        return true;
      });
    }
  }

  LockfreeList(const LockfreeList&) : head_(nullptr) {}

  T* Head() { return &(head_.load()->value); }

  // TODO: thread-unsafe. gc.
  void Clear() {
    auto* pre = head_.exchange(nullptr);
    garbages_.emplace_back(pre);
  }

  void* Add(const T& desired) {
    auto* n = new Node(desired, nullptr);

    while (true) {
      auto* h = head_.load();
      n->next.store(h, std::memory_order_relaxed);

      if (head_.compare_exchange_weak(h, n)) break;
    }
    return n;
  }

  bool AddIfHeadSatisfies(const T& desired, Functor f) {
    auto* n = new Node(desired, nullptr);

    while (true) {
      auto* h = head_.load();
      n->next.store(h, std::memory_order_relaxed);

      if (h != nullptr && !f(h->value)) {
        delete n;
        return false;
      }
      if (head_.compare_exchange_weak(h, n)) return true;
    }
  }

  T* Find(Functor f) {
    T* res = nullptr;
    ForEach([&](auto* node) {
      bool found = f != nullptr && f(node->value);
      if (found) {
        res = &node->value;
        return false;
      }
      return true;
    });
    return res;
  }

  size_t Size() {
    size_t size = 0;
    ForEach([&](const auto&) {
      size++;
      return true;
    });
    return size;
  }

  bool Every(const Functor f) {
    bool result = true;
    ForEach([&](auto* node) {
      bool partial_result = f(node->value);
      if (!partial_result) result = false;
      return partial_result;
    });

    return result;
  }

  // Thread-unsafe. FIXME
  bool DeleteAnItemIf(const Functor f) {
    auto* head = head_.load();
    auto* here = head;
    auto* prev = head;
    auto* next = head;

    while (here != nullptr) {
      next                = here->next.load();
      bool need_to_delete = f(here->value);
      if (need_to_delete) {
        if (head == here) {
          head_.store(next);
          delete here;
          return true;
        } else {
          if (prev->next.compare_exchange_weak(here, next)) {
            delete here;
            return true;
          } else {
            return false;
          }
        }
      } else {
        prev = here;
        here = next;
      }
    }

    return false;
  }

 private:
  void ForEach(const NodeFunctor f) {
    auto* h    = head_.load();
    auto* prev = h;
    while (h != nullptr) {
      prev        = h;
      h           = h->next.load();
      bool result = f(prev);
      if (!result) break;
    }
  }

  void ForEach(Node* n, const NodeFunctor f) {
    auto* h    = n;
    auto* prev = h;
    while (h != nullptr) {
      prev        = h;
      h           = h->next.load();
      bool result = f(prev);
      if (!result) break;
    }
  }

 public:
  struct Node {
    T value;
    std::atomic<Node*> next;
    Node(const T& v, Node* n = nullptr) : value(v), next(n) {}
    Node() : next(nullptr) {}
  };
};

}  // namespace Util
}  // namespace LineairDB

#endif /* LINEAIRDB_UTIL_LOCKFREE_LIST_HPP */
