
#include <filesystem>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"

class IssueTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(IssueTest, FenceShouldWaitForAllCallbacks_ExecuteInterface) {
  int value_of_alice = 1;
  std::atomic<bool> callback_executed{false};
  for (size_t i = 0; i < 30; i++) {
    callback_executed.store(false);
    db_->ExecuteTransaction(
        [&](LineairDB::Transaction& tx) {
          tx.Write<int>("alice", value_of_alice);
        },
        [&](LineairDB::TxStatus) { callback_executed.store(true); });
    db_->Fence();
    ASSERT_TRUE(callback_executed.load());
  }
}

TEST_F(IssueTest, FenceShouldWaitForAllCallbacks_HandlerInterface) {
  int value_of_alice = 1;
  std::atomic<bool> callback_executed{false};

  for (size_t i = 0; i < 30; i++) {
    callback_executed.store(false);
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", value_of_alice);
    db_->EndTransaction(
        tx, [&](LineairDB::TxStatus) { callback_executed.store(true); });
    db_->Fence();
    ASSERT_TRUE(callback_executed.load());
  }
}

// Regression test for: latest_callbacked_epoch_ stale-write bug in
// EventsOnEpochIsUpdated().
//
// Root cause:
//   In EventsOnEpochIsUpdated(old_epoch), the code called
//   FlushDurableEpoch() synchronously to obtain `durable`, then stored
//   `durable` into latest_callbacked_epoch_ inside a thread-pool task:
//
//     EpochNumber durable = logger_.FlushDurableEpoch();   // may be stale
//     thread_pool_.EnqueueForAllThreads([&, durable]() {
//       callback_manager_.ExecuteCallbacks(durable);
//       latest_callbacked_epoch_.store(durable);           // BUG
//     });
//
//   Because FlushLogs(old_epoch) is enqueued asynchronously *before* this
//   point, FlushDurableEpoch() can return the *previous* epoch's durable
//   value (e.g., epoch 1) even though old_epoch has already advanced.
//   Storing that stale value caused Fence()'s spin-wait:
//
//     while (latest_callbacked_epoch_.load() < current_epoch) { ... }
//
//   to never terminate.
//
// Fix:
//   Store old_epoch (the epoch being processed) instead of durable.
//   latest_callbacked_epoch_ is a "we have processed epoch E" sentinel for
//   Fence(); it must reflect the lifecycle of old_epoch, not WAL progress.
//
// Reproduction conditions:
//   - enable_logging = true  (default)
//   - Handler interface (BeginTransaction / EndTransaction from caller thread)
//   - Fence() called immediately after EndTransaction
TEST_F(IssueTest, Regression_FenceHangsWhenDurableEpochLagsBehindCurrentEpoch) {
  // At DB startup, FlushDurableEpoch() often returns epoch 1 while the
  // global epoch may already be 2+. Before the fix, storing that stale
  // value caused Fence() to spin indefinitely.
  std::atomic<int> executed_count{0};
  constexpr int kIterations = 10;

  for (int i = 0; i < kIterations; i++) {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("regression_key", i);
    db_->EndTransaction(
        tx, [&](LineairDB::TxStatus) { executed_count.fetch_add(1); });

    // Without the fix, this call spins indefinitely because
    // latest_callbacked_epoch_ was set to durable (< current_epoch).
    db_->Fence();

    ASSERT_EQ(executed_count.load(), i + 1)
        << "Callback not executed after Fence() at iteration " << i
        << ". Possible cause: latest_callbacked_epoch_ was set to stale "
           "`durable` epoch instead of `old_epoch` in EventsOnEpochIsUpdated.";
  }
}
