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

#include <lineairdb/lineairdb.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cassert>
#include <cxxopts.hpp>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <variant>

#include "util/logger.hpp"

size_t benchmark(const size_t db_size, const size_t buffer_size,
                 const size_t number_of_updates_per_data_item,
                 const LineairDB::Config::ConcurrencyControl cc_protocol,
                 const LineairDB::Config::DurabilityStrategy durability_strategy) {
  assert(0 < db_size);
  LineairDB::Config config;
  config.concurrency_control_protocol = cc_protocol;
  config.durability = durability_strategy;
  config.enable_recovery = true;

  {  // Populate database
    LineairDB::Database db(config);
    std::vector<std::future<void>> futures;
    const size_t thread_size = std::thread::hardware_concurrency();
    const size_t per_worker_insert_size = db_size / thread_size;
    std::vector<std::byte> buffer(buffer_size);

    for (size_t i = 0; i < thread_size; i++) {
      futures.push_back(std::async(std::launch::async, [&, i]() {
        for (size_t n = 0; n < number_of_updates_per_data_item; n++) {
          db.ExecuteTransaction(
              [&, i](LineairDB::Transaction& tx) {
                const size_t from = i * per_worker_insert_size;
                const size_t to = (i + 1) * per_worker_insert_size - 1;
                for (size_t j = from; j < to; j++) {
                  tx.Write(std::to_string(j), buffer.data(), buffer.size());
                }
              },
              []([[maybe_unused]] LineairDB::TxStatus result) {
                assert(result == LineairDB::TxStatus::Committed);
              });
        }
      }));
    }
    for (auto& fut : futures) {
      fut.wait();
    }
    SPDLOG_INFO("Finish database population for all {0} data items.", db_size);
    db.Fence();
    SPDLOG_INFO("DB Fence.");
  }

  auto begin = std::chrono::high_resolution_clock::now();
  LineairDB::Database db(config);
  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = end - begin;

  uint64_t milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

  return milliseconds;
}

int main(int argc, char** argv) {
  cxxopts::Options options("recoverytime_bench",
                           "Microbenchmark of recovery time");

  options.add_options()          //
      ("h,help", "Print usage")  //
      ("d,dbsize", "The number of data items in LineairDB",
       cxxopts::value<size_t>()->default_value("100000"))  //
      ("u,updates", "The number of update operations per data item",
       cxxopts::value<size_t>()->default_value("1"))  //
      ("b,buffersize", "Buffer size (bytes) for each data item",
       cxxopts::value<size_t>()->default_value("8"))  //
      ("s,strategy", "Durability strategy: none, wal, checkpoint, checkpoint_wal, cac",
       cxxopts::value<std::string>()->default_value("wal"))  //
      ("c,cc", "Concurrency control protocol: Silo, SiloNWR, 2PL",
       cxxopts::value<std::string>()->default_value("Silo"))  //
      ("o,output", "Output JSON filename",
       cxxopts::value<std::string>()->default_value(
           "recoverytime_bench_result.json"))  //
      ;

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  const size_t db_size = result["dbsize"].as<size_t>();
  const size_t buffer_size = result["buffersize"].as<size_t>();
  const size_t updates = result["updates"].as<size_t>();

  static const std::map<std::string, LineairDB::Config::ConcurrencyControl> Protocols = {
      {"Silo", LineairDB::Config::ConcurrencyControl::Silo},
      {"SiloNWR", LineairDB::Config::ConcurrencyControl::SiloNWR},
      {"2PL", LineairDB::Config::ConcurrencyControl::TwoPhaseLocking},
  };

  static const std::unordered_map<std::string, LineairDB::Config::DurabilityStrategy> Durabilities = {
      {"none",           LineairDB::Config::DurabilityStrategy::None},
      {"wal",            LineairDB::Config::DurabilityStrategy::WAL},
      {"checkpoint",     LineairDB::Config::DurabilityStrategy::Checkpoint},
      {"checkpoint_wal", LineairDB::Config::DurabilityStrategy::CheckpointAndWAL},
      {"cac",            LineairDB::Config::DurabilityStrategy::CAC},
  };

  auto protocol_str = result["cc"].as<std::string>();
  auto protocol_it = Protocols.find(protocol_str);
  if (protocol_it == Protocols.end()) {
    std::cerr << "Unknown concurrency control protocol: " << protocol_str << std::endl;
    exit(1);
  }
  auto cc_protocol = protocol_it->second;

  auto durability_str = result["strategy"].as<std::string>();
  auto durability_it = Durabilities.find(durability_str);
  if (durability_it == Durabilities.end()) {
    std::cerr << "Unknown durability strategy: " << durability_str << std::endl;
    exit(1);
  }
  auto durability_strategy = durability_it->second;

  std::filesystem::remove_all("lineairdb_logs");

  /** run benchmark **/
  uint64_t elapsed_ms = benchmark(db_size, buffer_size, updates, cc_protocol, durability_strategy);

  SPDLOG_INFO("RecoveryTimeBench: measurement has finisihed.");
  SPDLOG_INFO("elapsed time: {0} milliseconds", elapsed_ms);

  /** Output result as json format **/
  rapidjson::Document result_json(rapidjson::kObjectType);
  auto& allocator = result_json.GetAllocator();
  result_json.AddMember("elapsed_ms", rapidjson::Value(elapsed_ms), allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  result_json.Accept(writer);
  writer.Flush();

  auto result_string = buffer.GetString();
  auto output_filename = result["output"].as<std::string>();
  std::ofstream output_f(output_filename,
                         std::ofstream::out | std::ofstream::trunc);
  output_f << result_string;
  if (!output_f.good()) {
    std::cerr << "Unable to write output file" << output_filename << std::endl;
    exit(1);
  }
  std::cout << "This benchmark result is saved into " << output_filename
            << std::endl;
  return 0;
}
