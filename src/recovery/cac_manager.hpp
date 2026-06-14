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

#ifndef LINEAIRDB_CAC_MANAGER_HPP
#define LINEAIRDB_CAC_MANAGER_HPP

#include <lineairdb/config.h>
#include <lineairdb/transaction.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <msgpack.hpp>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "recovery/logger.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"
#include "util/thread_key_storage.h"

namespace LineairDB {
namespace Recovery {

/**
 * @brief Threading and Concurrent Access Design in CACManager
 *
 * This manager coordinates incremental checkpointing (Continuous Adaptive
 * Checkpointing - CAC). The core data structure is the thread-local `DirtySet`
 * (`tls_`), containing:
 *   - `dirty_live`: A buffer for active writes by transaction threads.
 *   - `dirty_stable`: A staging buffer for writes that are ready to be
 * persisted.
 *   - `latch`: A std::mutex protecting the local `DirtySet` from concurrent
 * operations.
 *
 * --- Thread Roles & Access Patterns ---
 *
 * 1. Transaction Threads (Multiple threads):
 *    - Function: PrecommitWriteSet
 *    - Behavior: When a transaction commits, it appends updated data items to
 * its own thread-local `dirty_live` list.
 *    - Locks: Acquires `ds->latch` to safely modify `ds->dirty_live`.
 *    - Note: Since `tls_.Get()` resolves to a thread-local instance, different
 * transaction threads do not compete with each other for the same
 * `DirtySet->latch`.
 *
 * 2. Epoch Progress (EpochManager / System thread):
 *    - Function: RotateDirtySets
 *    - Behavior: Invoked on global epoch changes. Iterates over all active
 * thread-local `DirtySet`s (via `tls_.ForEach`). Moves all entries from
 * `dirty_live` to `dirty_stable` and clears `dirty_live`.
 *    - Locks: Acquires `ds->latch` for each thread's `DirtySet` to safely
 * transfer data.
 *    - Contention: Competes with the corresponding transaction thread accessing
 * `ds->dirty_live` at that moment.
 *
 * 3. Logging Worker Thread (Background thread):
 *    - Function: IncrementalWorkerLoop / WriteRemainingDirtyEntries
 *    - Behavior: Iterates over all thread-local `DirtySet`s, moving entries
 * from `dirty_stable` to a local buffer for persistence.
 *    - Locks: Acquires `ds->latch` to safely drain `ds->dirty_stable`.
 *    - Contention: Competes with the Epoch thread (`RotateDirtySets`) when it
 * attempts to push entries from `dirty_live` to `dirty_stable`.
 */
class CACManager {
  // Internal implementation types - not part of the public API.
  struct DirtyEntry {
    std::string table_name;
    std::string key;
    DataItem* item;
  };
  struct DirtySet {
    std::vector<DirtyEntry> bufs[2];
    std::atomic<size_t> active_idx{0};
    std::vector<DirtyEntry>* dirty_stable{nullptr};

    DirtySet() {
      bufs[0].reserve(4096);
      bufs[1].reserve(4096);
    }
  };

 public:
  CACManager(const Config& config, TableDictionary& /*dict*/,
             EpochFramework& epoch)
      : config_(config),
        epoch_manager_ref_(epoch),
        dirty_stable_empty_(true),
        checkpoint_epoch_(1),
        durable_epoch_(0),
        stop_(false),
        has_dirty_writes_(false),
        dep_file_(config.work_dir + "/incremental_durable_epoch.log"),
        dep_working_(config.work_dir +
                     "/incremental_durable_epoch.working.log") {
    std::filesystem::create_directory(config_.work_dir);
    manager_thread_ = std::thread(&CACManager::IncrementalWorkerLoop, this);
    compactor_thread_ = std::thread(&CACManager::CompactorLoop, this);
  }

  ~CACManager() { Stop(); }

  void Stop() {
    if (stop_.exchange(true)) return;
    {
      std::lock_guard<std::mutex> lock(worker_latch_);
      worker_cv_.notify_all();
    }
    {
      std::lock_guard<std::mutex> lock(compactor_latch_);
      compactor_cv_.notify_all();
    }
    if (manager_thread_.joinable()) manager_thread_.join();
    if (compactor_thread_.joinable()) compactor_thread_.join();
  }

  void RotateDirtySets(EpochNumber next_checkpoint_epoch) {
    if (next_checkpoint_epoch <= checkpoint_epoch_.load()) {
      return;
    }
    if (!has_dirty_writes_.load()) {
      durable_epoch_.store(next_checkpoint_epoch);
      return;
    }
    has_dirty_writes_.store(false);

    checkpoint_epoch_.store(next_checkpoint_epoch);
    bool has_dirty = false;
    tls_.ForEach([&](DirtySet* ds) {
      const auto current_idx = ds->active_idx.load(std::memory_order_relaxed);
      const auto next_idx = 1 - current_idx;
      ds->active_idx.store(next_idx, std::memory_order_release);
      if (!ds->bufs[current_idx].empty()) {
        ds->dirty_stable = &ds->bufs[current_idx];
        has_dirty = true;
      } else {
        ds->dirty_stable = nullptr;
      }
    });
    if (has_dirty) {
      epoch_manager_ref_.Sync();
      dirty_stable_empty_.store(false);
    } else {
      durable_epoch_.store(next_checkpoint_epoch);
    }
  }

  void PrecommitWriteSet(const WriteSetType& write_set,
                         EpochNumber /*current_epoch*/) {
    if (write_set.empty()) return;
    const auto cp_epoch = checkpoint_epoch_.load();
    auto* ds = tls_.Get();
    const auto idx = ds->active_idx.load(std::memory_order_acquire);
    auto* live_vec = &ds->bufs[idx];
    for (auto& snapshot : write_set) {
      auto* item = snapshot.index_cache;
      if (item->stable_epoch.load() < cp_epoch) {
        if (item->checkpoint_buffer.IsEmpty()) {
          item->checkpoint_buffer.Reset(item->buffer);
          item->stable_epoch.store(cp_epoch);
        }
      } else if (item->stable_epoch.load() == cp_epoch) {
        item->checkpoint_buffer.Reset(item->buffer);
        has_dirty_writes_.store(true);
      }
      if (item->dirty_epoch.load() < cp_epoch) {
        item->dirty_epoch.store(cp_epoch);
        live_vec->push_back({snapshot.table_name, snapshot.key, item});
        has_dirty_writes_.store(true);
      }
    }
  }

  void AbortWriteSet(const WriteSetType& write_set) {
    for (auto& snapshot : write_set) {
      auto* item = snapshot.index_cache;
      item->checkpoint_buffer.Reset(nullptr, 0);
      item->stable_epoch.store(0);
    }
  }

  EpochNumber GetDurableEpoch() const { return durable_epoch_.load(); }

  EpochNumber RecoverDurableEpoch() {
    std::ifstream file(dep_file_, std::ios::binary);
    EpochNumber epoch = 0;
    if (file.good()) {
      try {
        std::string buf((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
        if (!buf.empty())
          msgpack::unpack(buf.data(), buf.size()).get().convert(epoch);
      } catch (...) {
      }
    }
    durable_epoch_.store(epoch);
    return epoch;
  }

  WriteSetType GetRecoverySetFromIncrementalLogs() {
    WriteSetType recovery_set;
    if (!std::filesystem::exists(config_.work_dir)) return recovery_set;

    std::unordered_map<std::string, size_t> index_map;

    auto [base_epoch, base_file_path] = FindLatestBase();
    if (base_epoch > 0) {
      if (auto records = ReadLogFile(base_file_path)) {
        for (auto& record : *records) {
          for (auto& kvp : record.key_value_pairs) {
            const std::byte* vp =
                kvp.buffer.empty()
                    ? nullptr
                    : reinterpret_cast<const std::byte*>(kvp.buffer.data());
            const std::string map_key = kvp.table_name + '\0' + kvp.key;
            index_map[map_key] = recovery_set.size();
            recovery_set.emplace_back(Snapshot(kvp.key, vp, kvp.buffer.size(),
                                               nullptr, kvp.table_name,
                                               kvp.tid));
          }
        }
      }
    }

    for (const auto& [e, path] : FindDeltaFiles()) {
      if (e <= base_epoch) continue;
      for (auto& log_record : ReadDeltaFile(path)) {
        for (auto& kvp : log_record.key_value_pairs) {
          const std::byte* vp =
              kvp.buffer.empty()
                  ? nullptr
                  : reinterpret_cast<const std::byte*>(kvp.buffer.data());
          const size_t vs = kvp.buffer.size();
          const std::string map_key = kvp.table_name + '\0' + kvp.key;
          auto it = index_map.find(map_key);
          if (it != index_map.end()) {
            auto& existing = recovery_set[it->second];
            if (existing.data_item_copy.transaction_id.load() < kvp.tid)
              existing.data_item_copy.Reset(vp, vs, kvp.tid);
          } else {
            index_map[map_key] = recovery_set.size();
            recovery_set.emplace_back(
                Snapshot(kvp.key, vp, vs, nullptr, kvp.table_name, kvp.tid));
          }
        }
      }
    }
    return recovery_set;
  }

 private:
  std::string DeltaFilePath(EpochNumber epoch) const {
    return config_.work_dir + "/" + std::string(kDeltaFilePrefix) +
           std::to_string(epoch) + ".log";
  }
  std::string BaseFilePath(EpochNumber epoch) const {
    return config_.work_dir + "/" + std::string(kBaseFilePrefix) +
           std::to_string(epoch) + ".log";
  }
  std::string BaseWorkingFilePath(EpochNumber epoch) const {
    return config_.work_dir + "/" + std::string(kBaseFilePrefix) +
           std::to_string(epoch) + ".log.working";
  }

  std::vector<std::pair<EpochNumber, std::string>> FindDeltaFiles(
      EpochNumber max_epoch = std::numeric_limits<EpochNumber>::max()) const {
    namespace fs = std::filesystem;
    std::vector<std::pair<EpochNumber, std::string>> result;
    if (!fs::exists(config_.work_dir)) return result;
    for (const auto& entry : fs::directory_iterator(config_.work_dir)) {
      const std::string name = entry.path().filename().string();
      if (name.find(kDeltaFilePrefix) != 0) continue;
      if (name.find(".log") == std::string::npos) continue;
      try {
        const size_t len = kDeltaFilePrefix.size();
        EpochNumber e = std::stoul(name.substr(len, name.find(".log") - len));
        if (e <= max_epoch) result.emplace_back(e, entry.path().string());
      } catch (...) {
      }
    }
    std::sort(result.begin(), result.end());
    return result;
  }

  std::pair<EpochNumber, std::string> FindLatestBase() const {
    namespace fs = std::filesystem;
    EpochNumber best_epoch = 0;
    std::string best_path;
    if (!fs::exists(config_.work_dir)) return {0, ""};
    for (const auto& entry : fs::directory_iterator(config_.work_dir)) {
      const std::string name = entry.path().filename().string();
      if (name.find(kBaseFilePrefix) != 0) continue;
      if (name.find(".log") == std::string::npos) continue;
      if (name.find(".working") != std::string::npos) continue;
      try {
        const size_t len = kBaseFilePrefix.size();
        EpochNumber e = std::stoul(name.substr(len, name.find(".log") - len));
        if (e > best_epoch) {
          best_epoch = e;
          best_path = entry.path().string();
        }
      } catch (...) {
      }
    }
    return {best_epoch, best_path};
  }

  void IncrementalWorkerLoop() {
    while (!stop_.load()) {
      {
        std::unique_lock<std::mutex> lock(worker_latch_);
        worker_cv_.wait_for(
            lock, std::chrono::milliseconds(config_.checkpoint_period),
            [&] { return stop_.load(); });
      }
      if (stop_.load()) break;

      EpochNumber next_cp_epoch = epoch_manager_ref_.GetGlobalEpoch();
      RotateDirtySets(next_cp_epoch);

      if (dirty_stable_empty_.load()) {
        continue;
      }

      EpochNumber cp_epoch = checkpoint_epoch_.load();
      Logger::LogRecord record;
      record.epoch = cp_epoch;

      tls_.ForEach([&](DirtySet* ds) {
        auto* stable_vec = ds->dirty_stable;
        ds->dirty_stable = nullptr;
        if (stable_vec) {
          for (auto& entry : *stable_vec) {
            entry.item->ExclusiveLock();
            if (!entry.item->IsInitialized()) {
              entry.item->ExclusiveUnlock();
              continue;
            }
            Logger::LogRecord::KeyValuePair kvp;
            kvp.table_name = entry.table_name;
            kvp.key = entry.key;
            if (entry.item->stable_epoch.load() == cp_epoch &&
                !entry.item->checkpoint_buffer.IsEmpty()) {
              kvp.buffer = entry.item->checkpoint_buffer.toString();
              entry.item->checkpoint_buffer.Reset(nullptr, 0);
            } else {
              kvp.buffer = entry.item->buffer.toString();
            }
            kvp.tid = {cp_epoch, 0};
            record.key_value_pairs.emplace_back(std::move(kvp));
            entry.item->ExclusiveUnlock();
          }
          stable_vec->clear();
        }
      });
      dirty_stable_empty_.store(true);

      if (!record.key_value_pairs.empty()) {
        std::string filename = DeltaFilePath(cp_epoch);
        std::ofstream f(filename, std::ios_base::out | std::ios_base::binary);
        Logger::LogRecords records{std::move(record)};
        msgpack::pack(f, records);
        f.flush();
        if (++incremental_file_count_ >= kCompactionThreshold) {
          std::lock_guard<std::mutex> compactor_lock(compactor_latch_);
          compaction_requested_.store(true);
          compactor_cv_.notify_one();
        }
      }

      durable_epoch_.store(cp_epoch);
      {
        std::ofstream f(dep_working_,
                        std::ios_base::out | std::ios_base::binary);
        msgpack::pack(f, cp_epoch);
        f.flush();
      }
      if (rename(dep_working_.c_str(), dep_file_.c_str()) != 0) {
        SPDLOG_ERROR("durable epoch rename failed, errno: {}", errno);
        exit(1);
      }
    }
    WriteRemainingDirtyEntries();
  }

  void WriteRemainingDirtyEntries() {
    checkpoint_epoch_.store(epoch_manager_ref_.GetGlobalEpoch());
    RotateDirtySets(checkpoint_epoch_.load());

    EpochNumber cp_epoch = checkpoint_epoch_.load();
    Logger::LogRecord record;
    record.epoch = cp_epoch;

    tls_.ForEach([&](DirtySet* ds) {
      auto* stable_vec = ds->dirty_stable;
      ds->dirty_stable = nullptr;
      if (stable_vec) {
        for (auto& entry : *stable_vec) {
          entry.item->ExclusiveLock();
          if (!entry.item->IsInitialized()) {
            entry.item->ExclusiveUnlock();
            continue;
          }
          Logger::LogRecord::KeyValuePair kvp;
          kvp.table_name = entry.table_name;
          kvp.key = entry.key;
          if (entry.item->stable_epoch.load() == cp_epoch &&
              !entry.item->checkpoint_buffer.IsEmpty()) {
            kvp.buffer = entry.item->checkpoint_buffer.toString();
            entry.item->checkpoint_buffer.Reset(nullptr, 0);
          } else {
            kvp.buffer = entry.item->buffer.toString();
          }
          kvp.tid = {cp_epoch, 0};
          record.key_value_pairs.emplace_back(std::move(kvp));
          entry.item->ExclusiveUnlock();
        }
        stable_vec->clear();
      }
    });

    if (!record.key_value_pairs.empty()) {
      std::string filename = DeltaFilePath(cp_epoch);
      std::ofstream f(filename, std::ios_base::out | std::ios_base::binary);
      Logger::LogRecords records{std::move(record)};
      msgpack::pack(f, records);
      f.flush();
    }
    durable_epoch_.store(cp_epoch);
    {
      std::ofstream f(dep_working_, std::ios_base::out | std::ios_base::binary);
      msgpack::pack(f, cp_epoch);
      f.flush();
    }
    if (rename(dep_working_.c_str(), dep_file_.c_str()) != 0) {
      // ignore or log failure on shutdown
    }
  }

  void CompactorLoop() {
    while (!stop_.load()) {
      std::unique_lock<std::mutex> lock(compactor_latch_);
      compactor_cv_.wait(
          lock, [&] { return stop_.load() || compaction_requested_.load(); });
      if (stop_.load()) break;
      compaction_requested_.store(false);
      lock.unlock();
      RunCompaction();
    }
  }

  void RunCompaction() {
    namespace fs = std::filesystem;
    const EpochNumber compact_up_to = durable_epoch_.load();
    if (compact_up_to == 0) return;

    auto [base_epoch, old_base_file] = FindLatestBase();
    auto delta_files = FindDeltaFiles(compact_up_to);
    if (delta_files.empty()) return;

    std::unordered_map<std::string, Logger::LogRecord::KeyValuePair> merged;

    if (!old_base_file.empty()) {
      if (auto records = ReadLogFile(old_base_file)) {
        for (auto& record : *records) {
          for (auto& kvp : record.key_value_pairs) {
            merged[kvp.table_name + '\0' + kvp.key] = std::move(kvp);
          }
        }
      }
    }

    for (const auto& [epoch, filename] : delta_files) {
      for (auto& log_record : ReadDeltaFile(filename)) {
        for (auto& kvp : log_record.key_value_pairs) {
          auto& slot = merged[kvp.table_name + '\0' + kvp.key];
          if (slot.key.empty() || slot.tid < kvp.tid) {
            slot = std::move(kvp);
          }
        }
      }
    }

    const std::string new_base_working = BaseWorkingFilePath(compact_up_to);
    const std::string new_base = BaseFilePath(compact_up_to);
    {
      Logger::LogRecord new_record;
      new_record.epoch = compact_up_to;
      for (auto& [k, kvp] : merged) {
        new_record.key_value_pairs.push_back(std::move(kvp));
      }
      std::ofstream f(new_base_working,
                      std::ios_base::out | std::ios_base::binary);
      Logger::LogRecords records{std::move(new_record)};
      msgpack::pack(f, records);
      f.flush();
    }
    if (rename(new_base_working.c_str(), new_base.c_str()) != 0) {
      SPDLOG_ERROR("Compaction rename failed, errno: {}", errno);
      fs::remove(new_base_working);
      return;
    }

    for (const auto& [epoch, filename] : delta_files) {
      fs::remove(filename);
    }
    if (!old_base_file.empty() && old_base_file != new_base) {
      fs::remove(old_base_file);
    }

    incremental_file_count_.fetch_sub(delta_files.size());
  }

  const Config& config_;
  EpochFramework& epoch_manager_ref_;
  ThreadKeyStorage<DirtySet> tls_;
  std::atomic<bool> dirty_stable_empty_;
  std::mutex worker_latch_;
  std::condition_variable worker_cv_;
  std::atomic<size_t> incremental_file_count_{0};
  std::mutex compactor_latch_;
  std::condition_variable compactor_cv_;
  std::atomic<bool> compaction_requested_{false};
  std::atomic<EpochNumber> checkpoint_epoch_;
  std::atomic<EpochNumber> durable_epoch_;
  std::atomic<bool> stop_;
  std::thread manager_thread_;
  std::thread compactor_thread_;

  std::atomic<bool> has_dirty_writes_;

  static constexpr std::string_view kDeltaFilePrefix =
      "incremental_checkpoint_";
  static constexpr std::string_view kBaseFilePrefix = "checkpoint_base_";
  static constexpr size_t kCompactionThreshold = 5;

  // Reads a single-pack msgpack file (base checkpoint files).
  static std::optional<Logger::LogRecords> ReadLogFile(
      const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.good()) return std::nullopt;
    std::string buf((std::istreambuf_iterator<char>(f)),
                    std::istreambuf_iterator<char>());
    if (buf.empty()) return std::nullopt;
    try {
      Logger::LogRecords records;
      msgpack::unpack(buf.data(), buf.size()).get().convert(records);
      return records;
    } catch (...) {
      return std::nullopt;
    }
  }

  // Reads a streaming msgpack file (delta checkpoint files) and returns all
  // LogRecord entries flattened into a single vector.
  static std::vector<Logger::LogRecord> ReadDeltaFile(const std::string& path) {
    std::vector<Logger::LogRecord> result;
    std::ifstream f(path, std::ios::binary);
    if (!f.good()) return result;
    std::string buf((std::istreambuf_iterator<char>(f)),
                    std::istreambuf_iterator<char>());
    if (buf.empty()) return result;
    size_t offset = 0;
    while (offset < buf.size()) {
      try {
        Logger::LogRecords records;
        auto oh = msgpack::unpack(buf.data(), buf.size(), offset);
        oh.get().convert(records);
        for (auto& r : records) result.push_back(std::move(r));
      } catch (...) {
        break;
      }
    }
    return result;
  }

  const std::string dep_file_;
  const std::string dep_working_;
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_CAC_MANAGER_HPP */
