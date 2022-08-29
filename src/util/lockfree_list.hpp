#ifndef LINEAIRDB_UTIL_LOCKFREE_LIST_HPP
#define LINEAIRDB_UTIL_LOCKFREE_LIST_HPP

#include <atomic>
#include <cstddef>
#include <functional>

#include "util/logger.hpp"

namespace LineairDB {
namespace Util {

template <typename T>
class LockfreeList {
  struct Node;
  using Functor     = std::function<bool(const T&)>;
  using NodeFunctor = std::function<bool(Node*)>;

 public:
  LockfreeList() : head_(nullptr) {}

  ~LockfreeList() {
    ForEach([&](auto* node) {
      delete node;
      return true;
    });
  }

  void Add(const T& desired) {
    auto* n = new Node(desired, nullptr);

    while (true) {
      auto* h = head_.load();
      n->next.store(h, std::memory_order_relaxed);

      if (head_.compare_exchange_weak(h, n)) break;
    }
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

  struct Node {
    T value;
    std::atomic<Node*> next;
    Node(const T& v, Node* n = nullptr) : value(v), next(n) {}
    Node() : next(nullptr) {}
  };

  std::atomic<Node*> head_;
};

}  // namespace Util
}  // namespace LineairDB

#endif /* LINEAIRDB_UTIL_LOCKFREE_LIST_HPP */
