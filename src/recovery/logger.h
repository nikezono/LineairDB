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
#ifndef LINEAIRDB_RECOVERY_LOGGER_H
#define LINEAIRDB_RECOVERY_LOGGER_H

#include <lineairdb/config.h>

#include <fstream>
#include <memory>
#include <msgpack.hpp>

#include "logger_base.h"
#include "types.h"

namespace LineairDB {
namespace Recovery {

class Logger {
 public:
  constexpr static EpochNumber NumberIsNotUpdated = 0;
  constexpr static auto DurableEpochNumberFileName =
      "lineairdb_logs/durable_epoch.json";
  constexpr static auto DurableEpochNumberWorkingFileName =
      "lineairdb_logs/durable_epoch_working.json";

  Logger(const Config&);
  ~Logger();

  // Methods that pass (delegate) to LoggerBase
  void RememberMe(const EpochNumber);
  void Enqueue(const WriteSetType& ws_ref_, EpochNumber epoch,
               bool entrusting = false);
  void FlushLogs(const EpochNumber stable_epoch);

  EpochNumber FlushDurableEpoch();
  EpochNumber GetDurableEpoch();
  void SetDurableEpoch(const EpochNumber);
  static EpochNumber GetDurableEpochFromLog();
  static WriteSetType GetRecoverySetFromLogs(const EpochNumber durable_epoch);

  struct LogRecord {
    struct KeyValuePair {
      std::string key;
      std::array<std::byte, 256> value;
      size_t size;
      TransactionId tid;
      MSGPACK_DEFINE(key, value, size, tid);
    };

    EpochNumber epoch;
    std::vector<KeyValuePair> key_value_pairs;
    MSGPACK_DEFINE(epoch, key_value_pairs);

    LogRecord() : epoch(0), key_value_pairs(0) {}
  };
  typedef std::vector<LogRecord> LogRecords;

 private:
  std::unique_ptr<LoggerBase> logger_;
  EpochNumber durable_epoch_;
  std::ofstream durable_epoch_working_file_;
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H */
