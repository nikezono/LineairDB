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

#ifndef LINEAIRDB_CHECKPOINT_MANAGER_HPP
#define LINEAIRDB_CHECKPOINT_MANAGER_HPP

#include <lineairdb/config.h>
#include <lineairdb/transaction.h>

#include <fstream>
#include <memory>
#include <msgpack.hpp>

#include "recovery/cac_manager.hpp"
#include "recovery/cpr_manager.hpp"
#include "recovery/logger.h"
#include "types/definitions.h"
#include "types/snapshot.hpp"

namespace LineairDB {
namespace Recovery {

class CheckpointManager {
 public:
  CheckpointManager(const Config& config, TableDictionary& dict,
                    EpochFramework& epoch)
      : config_(config),
        checkpoint_file_(config.work_dir + "/checkpoint.log") {
    if (IsFullScanCheckpoint()) {
      cpr_ = std::make_unique<CPRManager>(config, dict, epoch);
    } else if (IsCAC()) {
      cac_ = std::make_unique<CACManager>(config, dict, epoch);
    }
  }

  void Stop() {
    if (cpr_) cpr_->Stop();
    if (cac_) cac_->Stop();
  }

  bool IsCAC() const {
    return config_.durability == Config::DurabilityStrategy::CAC;
  }
  bool IsFullScanCheckpoint() const {
    return config_.durability == Config::DurabilityStrategy::Checkpoint ||
           config_.durability == Config::DurabilityStrategy::CheckpointAndWAL;
  }
  bool IsWAL() const {
    return config_.durability == Config::DurabilityStrategy::WAL ||
           config_.durability == Config::DurabilityStrategy::CheckpointAndWAL;
  }

  EpochNumber GetCheckpointCompletedEpoch() {
    if (cpr_) return cpr_->GetCheckpointCompletedEpoch();
    return 0;
  }

  void RotateDirtySets(EpochNumber next_checkpoint_epoch) {
    if (cac_) cac_->RotateDirtySets(next_checkpoint_epoch);
  }

  // --- Checkpoint hooks called by the concurrency control layer ---
  // Handles both full-scan (CPR-style) snapshotting and CAC dirty-set
  // registration in one call, removing duplicated logic from each CC impl.
  void OnPrecommit(const WriteSetType& write_set, EpochNumber current_epoch) {
    if (cpr_ && cpr_->IsNeedToCheckpointing(current_epoch)) {
      for (auto& snapshot : write_set) {
        snapshot.index_cache->CopyLiveVersionToStableVersion();
      }
    }
    if (cac_) cac_->PrecommitWriteSet(write_set, current_epoch);
  }

  void OnAbort(const WriteSetType& write_set) {
    if (cac_) cac_->AbortWriteSet(write_set);
  }

  // --- Recovery ---
  EpochNumber GetDurableEpoch() const {
    if (cac_) return cac_->GetDurableEpoch();
    return 0;
  }

  EpochNumber RecoverDurableEpoch() {
    if (cac_) return cac_->RecoverDurableEpoch();
    return 0;
  }

  WriteSetType GetRecoverySetFromLogs() {
    if (IsFullScanCheckpoint()) return GetRecoverySetFromFullScanLog();
    if (cac_) return cac_->GetRecoverySetFromIncrementalLogs();
    return {};
  }

 private:
  WriteSetType GetRecoverySetFromFullScanLog() {
    WriteSetType recovery_set;
    std::ifstream file(checkpoint_file_, std::ios::binary);
    if (!file.good()) return recovery_set;
    std::string buf((std::istreambuf_iterator<char>(file)),
                    std::istreambuf_iterator<char>());
    if (buf.empty()) return recovery_set;
    try {
      Logger::LogRecords records;
      msgpack::unpack(buf.data(), buf.size()).get().convert(records);
      for (auto& record : records) {
        for (auto& kvp : record.key_value_pairs) {
          const std::byte* vp =
              kvp.buffer.empty()
                  ? nullptr
                  : reinterpret_cast<const std::byte*>(kvp.buffer.data());
          recovery_set.emplace_back(Snapshot(kvp.key, vp, kvp.buffer.size(),
                                             nullptr, kvp.table_name, kvp.tid));
        }
      }
    } catch (...) {
    }
    return recovery_set;
  }

  const Config& config_;
  const std::string checkpoint_file_;
  std::unique_ptr<CPRManager> cpr_;
  std::unique_ptr<CACManager> cac_;
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_CHECKPOINT_MANAGER_HPP */
