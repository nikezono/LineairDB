#ifndef LINEAIRDB_TES_MANAGER_HPP
#define LINEAIRDB_TES_MANAGER_HPP

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <stdexcept>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/tx_status.h"
#include "spdlog/spdlog.h"
#include "types/snapshot.hpp"
#include "util/bloom_filter.hpp"
#include "util/thread_key_storage.h"

namespace LineairDB {

class Transaction;

/**
 * @brief TESManager: Transaction Epoch Shifting (TES) scheduling policy
 * management.
 *
 * Manages worker-local state (dirty_summary, suspended_set) and global drain
 * policy. Used by Database::Impl to defer shiftable transactions and flush them
 * at drain epochs or Fence().
 */
class TESManager {
 public:
  struct SuspendedTx {
    Transaction* tx;
    Database::CallbackType commit_clbk;
    Database::CallbackType precommit_clbk;
  };

  explicit TESManager(const Config& config) : config_(config) {
    if (config_.tes.enable && config_.tes.latency_bound == 0) {
      std::fprintf(stderr,
                   "LineairDB: config.tes.latency_bound must be at least 1.\n");
      SPDLOG_CRITICAL(
          "LineairDB: config.tes.latency_bound must be at least 1.");
      std::exit(EXIT_FAILURE);
    }
  }

  // =========================================================
  // Worker-thread-local operations: each thread operates only on its own local
  // state.
  // =========================================================

  /**
   * Determine whether a transaction is shiftable (referencing worker-local
   * state). Shiftable -> Defer() to buffer. NonShiftable -> Precommit
   * immediately.
   */
  bool IsShiftable(size_t read_set_size, const WriteSetType& write_set) {
    if (global_drain_) return false;
    if (write_set.empty()) return false;

    auto* state = worker_tls_.Get();
    if (state->warmup_write_count < config_.tes.warmup_count) return false;
    if (read_set_size > config_.tes.max_read_set_size) return false;

    for (const auto& snapshot : write_set) {
      if (state->dirty_summary.Hit(snapshot.index_cache)) return false;
    }
    return true;
  }

  /** Buffer a shiftable transaction into the suspended_set */
  void Defer(Transaction& tx, Database::CallbackType commit_clbk,
             Database::CallbackType precommit_clbk) {
    auto* state = worker_tls_.Get();
    state->suspended_set.push_back(
        {&tx, std::move(commit_clbk), std::move(precommit_clbk)});
  }

  /**
   * Update the worker-local dirty_summary after a successful immediate
   * precommit. Not called during the drain path (DrainSuspendedSet).
   */
  void UpdateDirtySummary(const WriteSetType& write_set) {
    auto* state = worker_tls_.Get();
    for (const auto& snapshot : write_set) {
      state->dirty_summary.Add(snapshot.index_cache);
    }
    state->warmup_write_count++;
  }

  /**
   * Pop (extract) and clear the worker-local deferred transactions if currently
   * draining.
   */
  std::vector<SuspendedTx> PopDeferredTransactionsIfDraining() {
    if (!IsDraining()) return {};
    auto* state = worker_tls_.Get();
    std::vector<SuspendedTx> popped;
    popped.swap(state->suspended_set);
    return popped;
  }

  /**
   * Reset worker-local state if the epoch has advanced.
   *
   * This method is thread-local and safe since each worker thread operates on
   * its own isolated thread-local state. The warmup count and dirty summary
   * Bloom filter are reset to ensure that transaction shiftability decisions
   * are fresh for the new epoch.
   *
   * - handler interface: called from BeginTransaction()
   * - thread pool workers: called from the lambda in EnqueueForAllThreads
   */
  void MaybeResetWorkerLocalState(EpochNumber current_epoch) {
    auto* state = worker_tls_.Get();
    if (state->last_reset_epoch != current_epoch) {
      state->warmup_write_count = 0;
      state->dirty_summary.Reset();
      state->last_reset_epoch = current_epoch;
    }
  }

  // =========================================================
  // Global operations: thread-safe operations on global_drain shared among
  // threads.
  // =========================================================

  /**
   * Called on epoch advancement: toggle the global drain policy every K epochs.
   * Active (Drain) when new_epoch % tes.latency_bound == 0, inactive (NoDrain)
   * otherwise.
   */
  void OnEpochAdvance(EpochNumber new_epoch) {
    global_drain_.store(new_epoch % config_.tes.latency_bound == 0);
  }

  /**
   * Pop (extract) and clear the deferred transactions from all worker threads.
   * Must be called after Fence() -> WaitForQueuesToBecomeEmpty() when exclusive
   * access is guaranteed.
   */
  std::vector<SuspendedTx> PopAllDeferredTransactions() {
    std::vector<SuspendedTx> popped;
    worker_tls_.ForEach([&](WorkerState* state) {
      popped.insert(popped.end(),
                    std::make_move_iterator(state->suspended_set.begin()),
                    std::make_move_iterator(state->suspended_set.end()));
      state->suspended_set.clear();
    });
    return popped;
  }

  /** Return true if the global drain status is active */
  bool IsDraining() const noexcept { return global_drain_; }

 private:
  struct WorkerState {
    // --- Epoch management ---
    EpochNumber last_reset_epoch = 0;

    // --- TES scheduling policy (reset every epoch) ---
    size_t warmup_write_count = 0;
    BloomFilter256 dirty_summary;

    // --- Buffered transactions ---
    std::vector<SuspendedTx> suspended_set;
  };

  const Config& config_;
  std::atomic<bool> global_drain_{false};
  ThreadKeyStorage<WorkerState> worker_tls_;
};

}  // namespace LineairDB

#endif  // LINEAIRDB_TES_MANAGER_HPP
