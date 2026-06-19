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
#ifndef LINEAIRDB_DATABASE_IMPL_H
#define LINEAIRDB_DATABASE_IMPL_H

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <functional>
#include <optional>
#include <stdexcept>
#include <utility>

#include "callback/callback_manager.h"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.h"
#include "table/table.h"
#include "table/table_dictionary.hpp"
#include "thread_pool/thread_pool.h"
#include "transaction_impl.h"
#include "util/backoff.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"
#include "util/tes_manager.hpp"

namespace LineairDB {
class Database::Impl {
  friend class Transaction::Impl;

 public:
  inline static Database::Impl* CurrentDBInstance;

  Impl(const Config& c = Config())
      : config_(c),
        logger_(config_),
        callback_manager_(config_),
        epoch_framework_(c.epoch_duration_ms, EventsOnEpochIsUpdated()),
        checkpoint_manager_(config_, table_dictionary_, epoch_framework_),
        thread_pool_(c.max_thread),
        tes_manager_(config_) {
    if (Database::Impl::CurrentDBInstance == nullptr) {
      Database::Impl::CurrentDBInstance = this;
      SPDLOG_INFO("LineairDB instance has been constructed.");
    } else {
      SPDLOG_ERROR(
          "It is prohibited to allocate two LineairDB::Database instance at "
          "the same time.");
      exit(EXIT_FAILURE);
    }
    if (!config_.anonymous_table_name.empty()) {
      CreateTable(config_.anonymous_table_name);
    } else {
      SPDLOG_ERROR("Anonymous table name is not set.");
      exit(EXIT_FAILURE);
    }
    if (config_.enable_recovery) {
      Recovery();
    }
    epoch_framework_.Start();
  }

  ~Impl() {
    destructing_.store(true);
    Fence();
    thread_pool_.StopAcceptingTransactions();
    epoch_framework_.Sync();
    checkpoint_manager_.Stop();
    epoch_framework_.Stop();
    while (!thread_pool_.IsEmpty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    thread_pool_.Shutdown();
    SPDLOG_DEBUG(
        "Epoch number and Durable epoch number are ended at {0}, and {1}, "
        "respectively.",
        epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
    SPDLOG_INFO("LineairDB instance has been destructed.");
    assert(Database::Impl::CurrentDBInstance == this);
    Database::Impl::CurrentDBInstance = nullptr;
  }

  Transaction& BeginTransaction() {
    epoch_framework_.MakeMeOnline();
    ProcessDeferredTransactions(epoch_framework_.GetMyThreadLocalEpoch(), true);
    return *(new Transaction(this));
  }

  bool EndTransaction(Transaction& tx, CallbackType commit_clbk,
                      std::optional<CallbackType> precommit_clbk) {
    // (A) TES enabled + no precommit callback -> abort tx and throw
    // std::runtime_error
    if (config_.tes.enable && !precommit_clbk.has_value()) {
      delete &tx;
      epoch_framework_.MakeMeOffline();
      throw std::runtime_error(
          "LineairDB: enable_tes=true requires the 3-argument EndTransaction "
          "with precommit_clbk.");
    }

    bool committed = TerminateTransaction(tx, std::move(commit_clbk),
                                          std::move(precommit_clbk), true);
    epoch_framework_.MakeMeOffline();
    if (config_.enable_checkpointing)
      logger_.TruncateLogs(checkpoint_manager_.GetCheckpointCompletedEpoch());
    return committed;
  }

  void ExecuteTransaction(ProcedureType proc, CallbackType clbk,
                          std::optional<CallbackType> prclbk) {
    // TES enabled + no precommit callback -> throw
    if (config_.tes.enable && !prclbk.has_value()) {
      throw std::runtime_error(
          "LineairDB: enable_tes=true requires precommit_clbk in "
          "ExecuteTransaction.");
    }

    for (;;) {
      bool success = thread_pool_.Enqueue([&, transaction_procedure = proc,
                                           callback = clbk,
                                           precommit_clbk = prclbk]() mutable {
        epoch_framework_.MakeMeOnline();

        ProcessDeferredTransactions(epoch_framework_.GetMyThreadLocalEpoch(),
                                    false);

        // Allocate Transaction on the heap to keep it alive outside the scope
        // during TES deferral.
        Transaction* tx_ptr = new Transaction(this);
        Transaction& tx = *tx_ptr;
        transaction_procedure(tx);

        TerminateTransaction(tx, std::move(callback), std::move(precommit_clbk),
                             false);
        epoch_framework_.MakeMeOffline();
      });
      if (success) break;
    }
  }

  void RequestCallbacks() {
    const auto current_epoch = epoch_framework_.GetGlobalEpoch();
    callback_manager_.ExecuteCallbacks(current_epoch);
  }

  EpochNumber GetMyThreadLocalEpoch() {
    return epoch_framework_.GetMyThreadLocalEpoch();
  }

  /**
   * Ensures that (1) all pending operations are completed, (2) all callbacks
   * have been executed, and (3) all index updates have been fully applied and
   * are visible to subsequent operations.
   *
   * Note: Due to the dependency on the implementation of
   * moodycamel::concurrentqueue, callbacks are executed **after**
   * try_dequeue(). This means the queue size can become zero even if some
   * callbacks have not yet been executed. As a result, the current
   * `WaitForAllCallbacksToBeExecuted()` does not strictly behave as its name
   * suggests (since it only checks if the queue length is zero).
   *
   * To address this problem, an atomic variable `latest_callbacked_epoch_` is
   * used as a workaround to ensure proper waiting.
   */
  void Fence() {
    const auto current_epoch = epoch_framework_.GetGlobalEpoch();

    epoch_framework_.Sync();
    thread_pool_.WaitForQueuesToBecomeEmpty();

    if (config_.tes.enable) {
      // Drain suspended_sets of all worker threads after guaranteeing that all
      // workers are idle.
      epoch_framework_.MakeMeOnline();
      DrainTransactions(tes_manager_.PopAllDeferredTransactions(), true);
      epoch_framework_.MakeMeOffline();
    }

    const bool skip_checkpoint_wait = destructing_.load() &&
                                      !config_.enable_logging &&
                                      config_.enable_checkpointing;
    if (!skip_checkpoint_wait) {
      callback_manager_.WaitForAllCallbacksToBeExecuted();
      // Spin-wait with yield for better performance in the critical path
      while (latest_callbacked_epoch_.load() < current_epoch) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
    // Wait for all index updates to be linearizable
    table_dictionary_.ForEachTable(
        [](Table& table) { table.WaitForIndexIsLinearizable(); });
  }

  const Config& GetConfig() const { return config_; }

  // NOTE: Called by a special thread managed by EpochFramework.
  std::function<void(EpochNumber)> EventsOnEpochIsUpdated() {
    return [&](EpochNumber old_epoch) {
      // Logging
      if (config_.enable_logging) {
        thread_pool_.EnqueueForAllThreads(
            [&, old_epoch]() { logger_.FlushLogs(old_epoch); });
      }

      EpochNumber durable = old_epoch;
      if (config_.enable_logging) {
        durable = logger_.FlushDurableEpoch();
      } else if (config_.enable_checkpointing) {
        durable = checkpoint_manager_.GetCheckpointCompletedEpoch();
      }

      // Execute Callbacks
      thread_pool_.EnqueueForAllThreads([&, durable, old_epoch]() {
        callback_manager_.ExecuteCallbacks(durable);
        // NOTE: Store old_epoch (not durable) so that Fence()'s spin-wait
        // `while (latest_callbacked_epoch_ < current_epoch)` terminates
        // correctly. durable = FlushDurableEpoch() may lag behind old_epoch
        // because FlushLogs(old_epoch) is enqueued asynchronously before this
        // point. Using durable would leave latest_callbacked_epoch_ stuck at a
        // stale value, causing Fence() to spin indefinitely.
        latest_callbacked_epoch_.store(old_epoch);
      });

      if (config_.enable_checkpointing) {
        auto checkpoint_completed =
            checkpoint_manager_.GetCheckpointCompletedEpoch();
        thread_pool_.EnqueueForAllThreads([&, checkpoint_completed]() {
          logger_.TruncateLogs(checkpoint_completed);
        });
      }

      // TES: Update drain policy and reset worker-local state on epoch advance.
      if (config_.tes.enable) {
        const EpochNumber new_epoch = old_epoch + 1;
        tes_manager_.OnEpochAdvance(new_epoch);
        thread_pool_.EnqueueForAllThreads([&, new_epoch]() {
          ProcessDeferredTransactions(new_epoch, false);
        });
      }
    };
  }

  void WaitForCheckpoint() {
    const auto start = checkpoint_manager_.GetCheckpointCompletedEpoch();
    Util::RetryWithExponentialBackoff([&]() {
      const auto current = checkpoint_manager_.GetCheckpointCompletedEpoch();
      return start != current;
    });
  }

  bool IsNeedToCheckpointing(const EpochNumber epoch) {
    return checkpoint_manager_.IsNeedToCheckpointing(epoch);
  }

  bool CreateTable(const std::string_view table_name) {
    return table_dictionary_.CreateTable(table_name, epoch_framework_, config_);
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    return table_dictionary_.GetTable(table_name);
  }

 private:
  void ProcessDeferredTransactions(EpochNumber epoch, bool entrusting) {
    if (config_.tes.enable) {
      tes_manager_.MaybeResetWorkerLocalState(epoch);
      DrainTransactions(tes_manager_.PopDeferredTransactionsIfDraining(),
                        entrusting);
    }
  }

  /**
   * DrainTransactions - Pop and finalize a list of deferred transactions.
   */
  void DrainTransactions(std::vector<TESManager::SuspendedTx>&& txs,
                         bool entrusting) {
    for (auto& stx : txs) {
      bool committed = stx.tx->Precommit();
      FinalizeTransaction(*stx.tx, std::move(stx.commit_clbk),
                          std::move(stx.precommit_clbk), committed, entrusting);
    }
  }

  /**
   * FinalizeTransaction - Perform final post-processing, durable registration,
   * and memory cleanup for a transaction after its precommit status has been
   * determined.
   */
  void FinalizeTransaction(Transaction& tx, CallbackType commit_clbk,
                           std::optional<CallbackType> precommit_clbk,
                           bool committed, bool entrusting) {
    if (committed) {
      tx.tx_pimpl_->PostProcessing(TxStatus::Committed);
    } else if (!tx.IsAborted()) {
      tx.tx_pimpl_->PostProcessing(TxStatus::Aborted);
    }
    if (precommit_clbk)
      precommit_clbk.value()(committed ? TxStatus::Committed
                                       : TxStatus::Aborted);

    if (committed) {
      tx.tx_pimpl_->current_status_ = TxStatus::Committed;
      const auto epoch = epoch_framework_.GetMyThreadLocalEpoch();
      callback_manager_.Enqueue(std::move(commit_clbk), epoch, entrusting);
      if (config_.enable_logging)
        logger_.Enqueue(tx.tx_pimpl_->write_set_, epoch, entrusting);
    } else {
      commit_clbk(TxStatus::Aborted);
    }
    delete &tx;
  }

  /**
   * TerminateTransaction - Main entry point to finalize a transaction from an
   * active execution thread. Handles immediate abort check and TES scheduling
   * before executing precommit.
   */
  bool TerminateTransaction(Transaction& tx, CallbackType commit_clbk,
                            std::optional<CallbackType> precommit_clbk,
                            bool entrusting) {
    if (tx.IsAborted()) {
      FinalizeTransaction(tx, std::move(commit_clbk), std::move(precommit_clbk),
                          false, entrusting);
      return false;
    }

    if (config_.tes.enable &&
        tes_manager_.IsShiftable(tx.tx_pimpl_->read_set_.size(),
                                 tx.tx_pimpl_->write_set_)) {
      tes_manager_.Defer(tx, std::move(commit_clbk),
                         std::move(precommit_clbk.value()));
      return false;
    }

    bool committed = tx.Precommit();
    if (committed && config_.tes.enable) {
      tes_manager_.UpdateDirtySummary(tx.tx_pimpl_->write_set_);
    }
    FinalizeTransaction(tx, std::move(commit_clbk), std::move(precommit_clbk),
                        committed, entrusting);
    return committed;
  }

  void Recovery() {
    SPDLOG_INFO("Start recovery process");
    EpochNumber highest_epoch = 1;
    const auto durable_epoch = logger_.GetDurableEpochFromLog();
    SPDLOG_DEBUG("  Durable epoch is resumed from {0}", highest_epoch);
    logger_.SetDurableEpoch(durable_epoch);
    [[maybe_unused]] auto enqueued = thread_pool_.EnqueueForAllThreads(
        [&]() { logger_.RememberMe(durable_epoch); });
    assert(enqueued);

    thread_pool_.WaitForQueuesToBecomeEmpty();

    epoch_framework_.MakeMeOnline();
    epoch_framework_.SetMyThreadLocalEpoch(durable_epoch);

    highest_epoch = std::max(highest_epoch, durable_epoch);
    auto&& recovery_sets = logger_.GetRecoverySetFromLogs(durable_epoch);

    for (auto& recovery_set : recovery_sets) {
      if (!recovery_set.data_item_copy.IsInitialized()) continue;
      CreateTable(recovery_set.table_name);
      auto table = GetTable(recovery_set.table_name);
      if (!table.has_value()) {
        SPDLOG_CRITICAL(
            "Recovery failed: Table {0} could not be found or created.",
            recovery_set.table_name);
        exit(EXIT_FAILURE);
      }

      highest_epoch =
          std::max(highest_epoch,
                   recovery_set.data_item_copy.transaction_id.load().epoch);
      table.value()->GetPrimaryIndex().Put(
          recovery_set.key, std::move(recovery_set.data_item_copy));
    }
    epoch_framework_.MakeMeOffline();

    SPDLOG_DEBUG("  Global epoch is resumed from {0}", highest_epoch);
    epoch_framework_.SetGlobalEpoch(highest_epoch);
    SPDLOG_INFO("Finish recovery process");
  }

 private:
  Config config_;
  Recovery::Logger logger_;
  Callback::CallbackManager callback_manager_;
  EpochFramework epoch_framework_;
  TableDictionary table_dictionary_;
  std::atomic<EpochNumber> latest_callbacked_epoch_{1};
  Recovery::CPRManager checkpoint_manager_;
  std::atomic<bool> destructing_{false};
  ThreadPool thread_pool_;
  TESManager tes_manager_;
};

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
