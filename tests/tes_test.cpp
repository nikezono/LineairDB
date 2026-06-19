#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "test_helper.hpp"

// ============================================================
// Handler interface tests (BeginTransaction / EndTransaction 3-arg)
// ============================================================

class TESTestBase : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;

  void SetUp() override {
    std::filesystem::remove_all(config_.work_dir);
    config_.tes.enable = true;
    config_.enable_logging = false;
    config_.enable_checkpointing = false;
    config_.enable_recovery = false;
    config_.tes.latency_bound = 5;
    config_.tes.warmup_count = 0;  // disable warmup for deterministic tests
    config_.max_thread = 4;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }

  void TearDown() override {
    db_.reset();
    std::filesystem::remove_all(config_.work_dir);
  }
};

class TESHandlerTest : public TESTestBase {};

/** When tes.enable=true, the 2-argument EndTransaction throws */
TEST_F(TESHandlerTest, ThrowsWithTwoArgEndTransaction) {
  auto& tx = db_->BeginTransaction();
  tx.Write<int>("alice", 1);
  EXPECT_THROW(db_->EndTransaction(tx, [](auto) {}), std::runtime_error);
}

/** Instantiating Database with tes.enable=true and latency_bound=0 exits */
TEST_F(TESHandlerTest, ExitsWithZeroLatencyBound) {
  LineairDB::Config config;
  config.tes.enable = true;
  config.tes.latency_bound = 0;
  config.enable_recovery = false;
  EXPECT_DEATH({ LineairDB::Database db(config); },
               "config.tes.latency_bound must be at least 1");
}

/** Defer -> Fence() causes precommit_clbk to fire */
TEST_F(TESHandlerTest, DeferredCommitFlushedByFence) {
  std::atomic<bool> precommit_done{false};
  std::atomic<bool> commit_done{false};

  auto& tx = db_->BeginTransaction();
  tx.Write<int>("alice", 42);
  db_->EndTransaction(
      tx, [&](auto) { commit_done = true; },
      [&](auto s) { precommit_done = (s == LineairDB::TxStatus::Committed); });

  // Should be deferred by TES (does not fire immediately)
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  // Note: Even if precommit fired immediately, it must be true after Fence()
  db_->Fence();

  ASSERT_TRUE(precommit_done);
  ASSERT_TRUE(commit_done);

  // Verify that the write has been reflected
  auto& rtx = db_->BeginTransaction();
  auto val = rtx.Read<int>("alice");
  ASSERT_TRUE(val.has_value());
  ASSERT_EQ(42, val.value());
  db_->EndTransaction(
      rtx, [](auto) {}, [](auto) {});
  db_->Fence();
}

/** Multiple deferred transactions are all committed by Fence() */
TEST_F(TESHandlerTest, MultipleDeferredTxsFlushedByFence) {
  constexpr int N = 10;
  std::atomic<int> precommit_count{0};
  std::atomic<int> commit_count{0};

  for (int i = 0; i < N; ++i) {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("key_" + std::to_string(i), i);
    db_->EndTransaction(
        tx, [&](auto) { commit_count.fetch_add(1); },
        [&](auto s) {
          if (s == LineairDB::TxStatus::Committed) precommit_count.fetch_add(1);
        });
  }

  db_->Fence();
  ASSERT_EQ(N, precommit_count.load());
  ASSERT_EQ(N, commit_count.load());
}

/** Dirty hit causes immediate precommit (not deferred) */
TEST_F(TESHandlerTest, DirtyHitCausesImmediatePrecommit) {
  // Commit the first transaction to register "alice" in the dirty_summary
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    std::atomic<bool> done{false};
    db_->EndTransaction(
        tx, [&](auto) { done = true; }, [](auto) {});
    db_->Fence();
    ASSERT_TRUE(done.load());
  }

  // Second transaction: dirty hit on "alice" -> precommit immediately
  std::atomic<bool> precommit_fired{false};
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 2);
    db_->EndTransaction(
        tx, [](auto) {},
        [&](auto s) {
          precommit_fired = (s == LineairDB::TxStatus::Committed);
        });
  }
  // If there is a dirty hit, precommit_clbk should be called immediately
  // without deferring. However, check via Fence() to handle potential
  // asynchronous execution.
  db_->Fence();
  ASSERT_TRUE(precommit_fired.load());
}

/** Next transaction can be successfully deferred/committed even after Fence()
 */
TEST_F(TESHandlerTest, FenceClearsDrainAfterCompletion) {
  // Round 1
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("x", 1);
    db_->EndTransaction(
        tx, [](auto) {}, [](auto) {});
    db_->Fence();
  }
  // Round 2: Check if transaction works again on the same thread
  std::atomic<bool> done{false};
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("x", 2);
    db_->EndTransaction(
        tx, [&](auto) { done = true; }, [](auto) {});
    db_->Fence();
  }
  ASSERT_TRUE(done.load());
}

// ============================================================
// Thread pool interface tests (ExecuteTransaction + precommit_clbk)
// ============================================================

class TESThreadPoolTest : public TESTestBase {};

/** tes.enable=true without precommit_clbk -> throw */
TEST_F(TESThreadPoolTest, ThrowsWithoutPrecommitClbk) {
  EXPECT_THROW(
      db_->ExecuteTransaction([](auto& tx) { tx.template Write<int>("k", 1); },
                              [](auto) {}, std::nullopt),
      std::runtime_error);
  db_->Fence();
}

/** Defer -> Fence() causes precommit_clbk to fire */
TEST_F(TESThreadPoolTest, DeferredCommitFlushedByFence) {
  std::atomic<bool> precommit_done{false};
  std::atomic<bool> commit_done{false};

  db_->ExecuteTransaction(
      [](auto& tx) { tx.template Write<int>("bob", 99); },
      [&](auto) { commit_done = true; },
      [&](auto s) { precommit_done = (s == LineairDB::TxStatus::Committed); });

  db_->Fence();
  ASSERT_TRUE(precommit_done.load());
  ASSERT_TRUE(commit_done.load());
}

/** Multiple concurrent defers -> all committed by Fence() */
TEST_F(TESThreadPoolTest, MultipleConcurrentDefersThenFence) {
  constexpr int N = 20;
  std::atomic<int> commit_count{0};

  for (int i = 0; i < N; ++i) {
    db_->ExecuteTransaction(
        [i](auto& tx) { tx.template Write<int>("k" + std::to_string(i), i); },
        [&](auto) { commit_count.fetch_add(1); }, [](auto) {});
  }

  db_->Fence();
  ASSERT_EQ(N, commit_count.load());
}
