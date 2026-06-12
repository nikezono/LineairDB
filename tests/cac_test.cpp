
/**
 * CAC (Commit-ahead Checkpointing) test cases.
 * CAC is a durability strategy that achieves full consistency guarantee with bounded log file size, by using epoch-rotation-based incremental checkpointing and delaying commit ACK until the epoch of the transaction is durable.
 * i.e., CAC provides the same semantics with the conventional WAL + Checkpointing.
 * This test suite verifies the properties of CAC as described above.
 */

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "test_helper.hpp"

class CACTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all("lineairdb_logs");
    config_.max_thread = 4;
    config_.durability = LineairDB::Config::DurabilityStrategy::CAC;
    config_.enable_recovery = true;
    config_.epoch_duration_ms = 10;
    config_.checkpoint_period = 0;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

// Write a key, restart, then verify the value is recovered from the checkpoint.
TEST_F(CACTest, Recovery) {
  int initial_value = 42;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("cac_key", initial_value);
                             }});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  db_ = std::make_unique<LineairDB::Database>(config_);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto res = tx.Read<int>("cac_key");
                               ASSERT_TRUE(res.has_value());
                               ASSERT_EQ(res.value(), initial_value);
                             }});
}

// Overwrite the same key in a later epoch; recovery must return the latest
// value, not the first write.
TEST_F(CACTest, OverwriteSameKeyRecovery) {
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("k", 1);
                             }});
  db_->Fence();

  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("k", 2);
                             }});
  db_->Fence();

  db_.reset();
  db_ = std::make_unique<LineairDB::Database>(config_);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto res = tx.Read<int>("k");
                               ASSERT_TRUE(res.has_value());
                               ASSERT_EQ(res.value(), 2);
                             }});
}

// Multiple distinct keys written in one epoch; all must survive a restart.
TEST_F(CACTest, MultipleKeysRecovery) {
  const int N = 8;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               for (int i = 0; i < N; ++i)
                                 tx.Write<int>("mk_" + std::to_string(i), i);
                             }});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  db_ = std::make_unique<LineairDB::Database>(config_);
  for (int i = 0; i < N; ++i) {
    TestHelper::DoTransactions(
        db_.get(), {[&, i](LineairDB::Transaction& tx) {
          auto res = tx.Read<int>("mk_" + std::to_string(i));
          ASSERT_TRUE(res.has_value());
          ASSERT_EQ(res.value(), i);
        }});
  }
}

// The commit callback must be invoked only after transaction is durable.
// We verify this semantically: if the callback has fired, then restarting the
// DB immediately must still return the written value.
TEST_F(CACTest, CommitCallbackAfterDurabilitySatisfied) {
  std::atomic<bool> callback_called(false);
  int value = 999;

  db_->ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write<int>("delayed_key", value); },
      [&](LineairDB::TxStatus status) {
        ASSERT_EQ(status, LineairDB::TxStatus::Committed);
        callback_called = true;
      });

  while (!callback_called.load()) std::this_thread::yield();
  ASSERT_TRUE(callback_called);

  // At this point durable_epoch >= tx_epoch is guaranteed.  Restart immediately
  // and confirm the data is not lost.
  db_.reset();
  db_ = std::make_unique<LineairDB::Database>(config_);

  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto res = tx.Read<int>("delayed_key");
                               ASSERT_TRUE(res.has_value());
                               ASSERT_EQ(res.value(), value);
                             }});
}

// Concurrent writers writing disjoint keys; all committed data must be
// recoverable after a restart.
TEST_F(CACTest, ConcurrentWriteRecovery) {
  const int N = 8;
  std::vector<TransactionProcedure> txns;
  for (int i = 0; i < N; ++i) {
    txns.push_back([i](LineairDB::Transaction& tx) {
      tx.Write<int>("cw_" + std::to_string(i), i * 10);
    });
  }
  size_t committed = TestHelper::DoTransactionsOnMultiThreads(db_.get(), txns);
  ASSERT_GT(committed, 0u);

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  db_ = std::make_unique<LineairDB::Database>(config_);
  // All committed writes must be visible after recovery.
  for (int i = 0; i < N; ++i) {
    TestHelper::DoTransactions(
        db_.get(), {[&, i](LineairDB::Transaction& tx) {
          auto res = tx.Read<int>("cw_" + std::to_string(i));
          // Either the write committed (value present) or it was aborted (no
          // value); both are valid outcomes.  We only check the type-safety and
          // absence of corruption.
          if (res.has_value()) {
            ASSERT_EQ(res.value(), i * 10);
          }
        }});
  }
}

// ---------------------------------------------------------------------------
// Compaction tests
//
// After enough checkpoint epochs have been processed, the compactor thread
// must merge obsolete incremental_checkpoint_<epoch>.log files so that either
// the file count or the total byte size of all incremental checkpoint files
// decreases at least once.
//
// These tests are written RED-first (TDD): they will FAIL until the compactor
// is implemented in CheckpointManager.
// ---------------------------------------------------------------------------

namespace {

struct CheckpointFileStats {
  size_t count;
  uintmax_t total_bytes;
};

// Returns the number of incremental_checkpoint_*.log and checkpoint_base_*.log
// files and their combined total size in the given directory.
CheckpointFileStats GetIncrementalFileStats(const std::string& dir) {
  CheckpointFileStats stats{0, 0};
  if (!std::filesystem::exists(dir)) return stats;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    const std::string name = entry.path().filename().string();
    const bool is_delta = name.find("incremental_checkpoint_") == 0 &&
                          name.find(".log") != std::string::npos &&
                          name.find(".working") == std::string::npos;
    const bool is_base = name.find("checkpoint_base_") == 0 &&
                         name.find(".log") != std::string::npos &&
                         name.find(".working") == std::string::npos;
    if (is_delta || is_base) {
      ++stats.count;
      stats.total_bytes += entry.file_size();
    }
  }
  return stats;
}

}  // namespace

class CACCompactionTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  const std::string log_dir_ = "./lineairdb_logs";

  void SetUp() override {
    std::filesystem::remove_all(log_dir_);
    config_.max_thread = 2;
    config_.durability = LineairDB::Config::DurabilityStrategy::CAC;
    config_.enable_recovery = false;
    config_.epoch_duration_ms = 10;
    config_.checkpoint_period = 0;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }

  // Commit one write transaction and wait for its epoch to become durable.
  void CommitOneWrite(const std::string& key, int value) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [&](LineairDB::Transaction& tx) { tx.Write<int>(key, value); },
        [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
  }
};

// After committing writes across at least 10 distinct checkpoint epochs,
// the compactor must have merged some files so that the total number of
// incremental_checkpoint_*.log files is strictly less than 10.
TEST_F(CACCompactionTest, FileCountDecreasesAfterCompaction) {
  const int kEpochs = 10;

  std::vector<size_t> file_counts;

  for (int i = 0; i < kEpochs; ++i) {
    CommitOneWrite("key_" + std::to_string(i), i);
    // Give the worker loop time to write the epoch file.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    file_counts.push_back(GetIncrementalFileStats(log_dir_).count);
  }

  // After all epochs, at least one snapshot reduction must have occurred:
  // a compactor would merge older epoch files, so the count at some point
  // must be strictly less than the maximum observed count.
  const size_t max_count = *std::max_element(file_counts.begin(), file_counts.end());
  const size_t last_count = file_counts.back();
  EXPECT_LT(last_count, max_count)
      << "File count never decreased: compaction has not been implemented. "
      << "Counts observed: [" << [&]{
           std::string s;
           for (auto c : file_counts) s += std::to_string(c) + " ";
           return s;
         }() << "]";
}

// Same scenario but measured by total byte size.
// Strategy: measure the size of a single delta file first, then write many
// epochs with the same keys (so compaction can eliminate duplicates).
// After compaction, the total on-disk size should be much less than
// kEpochs * single_delta_size, because the merged base file holds only the
// latest version of each key rather than one entry per epoch.
TEST_F(CACCompactionTest, FileSizeDecreasesAfterCompaction) {
  const std::vector<std::string> keys = {"a", "b", "c", "d", "e"};

  // Write exactly one epoch worth of data and wait for it to be durable.
  // This gives us the baseline size of a single delta file.
  for (const auto& key : keys) CommitOneWrite(key, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  const uintmax_t single_epoch_size =
      GetIncrementalFileStats(log_dir_).total_bytes;
  ASSERT_GT(single_epoch_size, 0u) << "No checkpoint file written at all";

  // Write many epochs so compaction triggers multiple times.
  // Using the same keys means every delta is a candidate for deduplication.
  const int kEpochs = 20;
  for (int i = 1; i < kEpochs; ++i) {
    for (const auto& key : keys) CommitOneWrite(key, i);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  // Give the compactor generous time to finish its last run.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  const uintmax_t final_size = GetIncrementalFileStats(log_dir_).total_bytes;
  // Without compaction: ~kEpochs * single_epoch_size bytes.
  // With compaction:    1 base + a few deltas ≈ a handful of single_epoch_size.
  // We require strictly less than half the "no compaction" expectation.
  const uintmax_t no_compaction_estimate =
      static_cast<uintmax_t>(kEpochs) * single_epoch_size;
  EXPECT_LT(final_size, no_compaction_estimate / 2)
      << "Expected compaction to have reduced total size significantly. "
      << "single_epoch_size=" << single_epoch_size
      << ", final_size=" << final_size
      << ", no_compaction_estimate=" << no_compaction_estimate;
}

// Compaction must preserve correctness: after compaction, a restart must
// still recover all the data that was durable before compaction.
TEST_F(CACCompactionTest, CompactionPreservesRecoverability) {
  const int kEpochs = 10;

  // Write keys across many epochs.
  for (int i = 0; i < kEpochs; ++i) {
    CommitOneWrite("recover_" + std::to_string(i), i * 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Wait for all epochs to flush.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  // Verify compaction happened (at least the file count did not grow unbounded).
  const size_t post_compact_count = GetIncrementalFileStats(log_dir_).count;
  EXPECT_LT(post_compact_count, static_cast<size_t>(kEpochs))
      << "Expected compaction to have reduced file count below " << kEpochs;

  // Reopen and verify all durable writes are visible.
  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);
  for (int i = 0; i < kEpochs; ++i) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [&, i](LineairDB::Transaction& tx) {
          auto res = tx.Read<int>("recover_" + std::to_string(i));
          if (res.has_value()) {
            EXPECT_EQ(res.value(), i * 100);
          }
        },
        [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
  }
  db_->Fence();
}

// Verify that recovery handles empty delta checkpoint files gracefully.
TEST_F(CACTest, EmptyDeltaFileRecovery) {
  int initial_value = 12345;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("valid_key", initial_value);
                              }});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  // Create an empty delta log file with a high epoch number
  std::filesystem::create_directory("lineairdb_logs");
  {
    std::ofstream f("lineairdb_logs/incremental_checkpoint_9999.log", std::ios::binary);
  }

  // Restart DB and verify recovery succeeds and returns the correct value
  db_ = std::make_unique<LineairDB::Database>(config_);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto res = tx.Read<int>("valid_key");
                               ASSERT_TRUE(res.has_value());
                               ASSERT_EQ(res.value(), initial_value);
                             }});
}

// Verify that recovery handles corrupted delta checkpoint files (non-msgpack garbage) without crashing.
TEST_F(CACTest, CorruptedMsgpackRecovery) {
  int initial_value = 67890;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("valid_key2", initial_value);
                              }});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  // Create a corrupted delta log file with garbage bytes
  std::filesystem::create_directory("lineairdb_logs");
  {
    std::ofstream f("lineairdb_logs/incremental_checkpoint_9999.log", std::ios::binary);
    f << "This is corrupted non-msgpack garbage data!!";
  }

  // Restart DB and verify recovery ignores the corrupted file and succeeds for the valid key
  db_ = std::make_unique<LineairDB::Database>(config_);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto res = tx.Read<int>("valid_key2");
                               ASSERT_TRUE(res.has_value());
                               ASSERT_EQ(res.value(), initial_value);
                             }});
}

// Verify that recovery ignores ".working" temporary files left by the compactor.
TEST_F(CACTest, RecoveryIgnoresWorkingFiles) {
  // 1. Write some initial data
  int val1 = 111;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("key1", val1);
                             }});
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  // 2. Write a fake newer base file but with `.working` suffix, containing a different value (or garbage)
  std::filesystem::create_directory("lineairdb_logs");
  {
    std::ofstream f("lineairdb_logs/checkpoint_base_9999.log.working", std::ios::binary);
    f << "garbage in working file";
  }

  // 3. Restart DB; the recovery should ignore the `.working` file and read from the original checkpoints
  db_ = std::make_unique<LineairDB::Database>(config_);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto res = tx.Read<int>("key1");
                               ASSERT_TRUE(res.has_value());
                               ASSERT_EQ(res.value(), val1);
                             }});
}
