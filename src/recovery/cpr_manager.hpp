/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
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

#ifndef LINEAIRDB_CPR_MANAGER_HPP
#define LINEAIRDB_CPR_MANAGER_HPP

#include <lineairdb/config.h>
#include <lineairdb/transaction.h>

#include <atomic>
#include <chrono>
#include <msgpack.hpp>
#include <string_view>
#include <thread>

#include "recovery/logger.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

namespace LineairDB {

namespace Recovery {

class CPRManager {
 public:
  enum class Phase { REST, IN_PROGRESS, WAIT_FLUSH };
  const std::string CheckpointFileName;
  const std::string CheckpointWorkingFileName;

  CPRManager(const LineairDB::Config& c_ref, TableDictionary& d_ref,
             EpochFramework& e_ref)
      : CheckpointFileName(c_ref.work_dir + "/checkpoint.log"),
        CheckpointWorkingFileName(c_ref.work_dir + "/checkpoint.working.log"),
        config_ref_(c_ref),
        dict_ref_(d_ref),
        epoch_manager_ref_(e_ref),
        current_phase_(Phase::REST),
        checkpoint_epoch_(0),
        checkpoint_completed_epoch_(0),
        stop_(false),
        manager_thread_([&]() {
          const bool enable_checkpointing =
              config_ref_.durability ==
                  LineairDB::Config::DurabilityStrategy::Checkpoint ||
              config_ref_.durability ==
                  LineairDB::Config::DurabilityStrategy::CheckpointAndWAL;
          if (!enable_checkpointing) return;
          const auto checkpoint_period = config_ref_.checkpoint_period;
          for (;;) {
            {  // REST Phase: sleep
              auto start = std::chrono::high_resolution_clock::now();
              if (current_phase_.load() == Phase::REST) {
                bool got_stop = false;
                for (;;) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(10));
                  if (stop_.load()) {
                    got_stop = true;
                    break;
                  }
                  auto now = std::chrono::high_resolution_clock::now();
                  if (checkpoint_period <=
                      static_cast<size_t>(
                          std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - start)
                              .count()))
                    break;
                }
                if (got_stop) break;
              }
            }

            ExecuteCheckpointSequence();
          }

          if (config_ref_.enable_graceful_shutdown_checkpoint) {
            // Force a final checkpoint on shutdown (without Sync() to avoid
            // deadlock)
            checkpoint_epoch_.store(epoch_manager_ref_.GetGlobalEpoch());
            DoFinalCheckpoint();
          }
        }) {}

  void Stop() {
    stop_.store(true);
    if (manager_thread_.joinable()) manager_thread_.join();
  }

  EpochNumber GetCheckpointCompletedEpoch() {
    return checkpoint_completed_epoch_.load();
  }

  bool IsNeedToCheckpointing(EpochNumber my_epoch) {
    const auto global_phase = current_phase_.load();
    if (global_phase == Phase::REST) {
      return false;
    }
    return checkpoint_epoch_.load() <= my_epoch;
  }

 private:
  void ExecuteCheckpointSequence() {
    epoch_manager_ref_.MakeMeOnline();
    const auto current_epoch = epoch_manager_ref_.GetGlobalEpoch();
    SPDLOG_DEBUG("PREPARE to checkpointing. current {}", current_epoch);
    const auto checkpoint_epoch = current_epoch + 1;
    checkpoint_epoch_.store(checkpoint_epoch);
    current_phase_.store(Phase::IN_PROGRESS);
    assert(checkpoint_epoch != 0);
    epoch_manager_ref_.MakeMeOffline();

    // Wait for a stable epoch
    epoch_manager_ref_.Sync();

    DoFinalCheckpoint();
  }

  void DoFinalCheckpoint() {
    current_phase_.store(Phase::WAIT_FLUSH);

    // We now create the consistent snapshot of the end of the epoch `e+1`.
    Recovery::Logger::LogRecords records;
    Recovery::Logger::LogRecord record;
    record.epoch = checkpoint_epoch_.load() + 1;

    dict_ref_.ForEachTable([&](LineairDB::Table& table) {
      table.GetPrimaryIndex().ForEach(
          [&](std::string_view key, LineairDB::DataItem& data_item) {
            data_item.ExclusiveLock();

            // Skip deleted items (not initialized)
            if (!data_item.IsInitialized()) {
              data_item.ExclusiveUnlock();
              return true;
            }

            Logger::LogRecord::KeyValuePair kvp;
            kvp.table_name = table.GetTableName();
            kvp.key = key;
            if (data_item.checkpoint_buffer.IsEmpty()) {
              // this data item holds version which has written before
              // the point of consistency.
              kvp.buffer = data_item.buffer.toString();
            } else {
              kvp.buffer = data_item.checkpoint_buffer.toString();
              data_item.checkpoint_buffer.Reset(nullptr, 0);
            }
            kvp.tid.epoch = record.epoch;
            kvp.tid.tid = 0;
            record.key_value_pairs.emplace_back(std::move(kvp));

            data_item.ExclusiveUnlock();
            return true;
          });
    });
    records.emplace_back(std::move(record));

    std::ofstream new_file(CheckpointWorkingFileName,
                           std::ios_base::out | std::ios_base::binary);
    msgpack::pack(new_file, records);
    new_file.flush();
    new_file.close();
    SPDLOG_DEBUG("RENAME checkpoint workingfile from {0} to {1}",
                 CheckpointWorkingFileName, CheckpointFileName);

    // NOTE POSIX ensures that rename syscall provides atomicity
    if (rename(CheckpointWorkingFileName.c_str(), CheckpointFileName.c_str())) {
      SPDLOG_ERROR(
          "Durability Error: fail to rename checkpoint of the "
          "epoch "
          "{0:d}. "
          "errno: {1}",
          record.epoch, errno);
      exit(1);
    }
    SPDLOG_DEBUG("FLUSH consistent snapshot of epoch {}",
                 checkpoint_epoch_.load());
    checkpoint_completed_epoch_.store(checkpoint_epoch_.load());
    current_phase_.store(Phase::REST);
  }

  const LineairDB::Config& config_ref_;
  LineairDB::TableDictionary& dict_ref_;
  LineairDB::EpochFramework& epoch_manager_ref_;
  Logger::LogRecords log_records;
  std::atomic<Phase> current_phase_;
  std::atomic<EpochNumber> checkpoint_epoch_;  // 'v' in the CPR paper
  std::atomic<EpochNumber> checkpoint_completed_epoch_;
  // BloomFilter bloom_filter_for_recent_updates_;
  std::atomic<bool> stop_;
  std::thread manager_thread_;
  MSGPACK_DEFINE(log_records);
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_CPR_MANAGER_HPP */
