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

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cxxopts.hpp>
#include <experimental/filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <thread>

#include "workload.h"

namespace YCSB {

void PopulateDatabase(LineairDB::Database&, YCSB::Workload&, size_t);
rapidjson::Document RunBenchmark(LineairDB::Database&, YCSB::Workload&, bool);

}  // namespace YCSB

const std::map<std::string, LineairDB::Config::ConcurrencyControl> Protocols = {
    {"Silo", LineairDB::Config::ConcurrencyControl::Silo},
    {"SiloNWR", LineairDB::Config::ConcurrencyControl::SiloNWR},
    {"2PL", LineairDB::Config::ConcurrencyControl::TwoPhaseLocking},
};

int main(int argc, char** argv) {
  cxxopts::Options options(
      "ycsb",
      "YCSB: Yahoo! Cloud serving benchmark for multi-key transactions");

  options.add_options()          //
      ("h,help", "Print usage")  //
      ("R,records", "Scale factor of YCSB: the number of records in the table",
       cxxopts::value<size_t>()->default_value("100000"))  //
      ("C,contention", "Skew parameter for zipfian distribution",
       cxxopts::value<double>()->default_value("0.5"))  //
      ("w,workload", "Workload",
       cxxopts::value<std::string>()->default_value("a"))  //
      ("c,cc", "Concurrency control protocol",
       cxxopts::value<std::string>()->default_value("SiloNWR"))  //
      ("l,log", "Enable logging",
       cxxopts::value<bool>()->default_value("false"))  //
      ("s,ws", "Size of working set for each transaction",
       cxxopts::value<size_t>()->default_value("4"))  //
      ("i,index", "Index",
       cxxopts::value<std::string>()->default_value("default"))  //
      ("e,epoch", "Size of epoch duration",
       cxxopts::value<size_t>()->default_value("40"))  //
      ("p,payload", "Size (bytes) of each record",
       cxxopts::value<size_t>()->default_value("8"))  //
      ("t,thread", "The number of threads working on LineairDB",
       cxxopts::value<size_t>()->default_value("1"))  //
      ("q,clients", "The number of threads queueing the jobs into LineairDB",
       cxxopts::value<size_t>()->default_value("1"))  //
      // std::to_string(std::thread::hardware_concurrency())))  //
      ("H,handler",
       "Use handler interface: queueing threads also execute transactions",
       cxxopts::value<bool>()->default_value("false"))  //
      ("d,duration", "Measurement duration of this benchmark (milliseconds)",
       cxxopts::value<size_t>()->default_value("2000"))  //
      ("o,output", "Output JSON filename",
       cxxopts::value<std::string>()->default_value("ycsb_result.json"))  //
      ;

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  std::experimental::filesystem::remove_all("lineairdb_logs");

  /** Initialize LineairDB **/
  LineairDB::Config config;
  auto protocol                       = result["cc"].as<std::string>();
  auto index                          = result["index"].as<std::string>();
  config.concurrency_control_protocol = Protocols.find(protocol)->second;
  config.enable_recovery              = false;
  config.enable_checkpointing         = false;
  config.enable_logging               = result["log"].as<bool>();
  config.max_thread                   = result["thread"].as<size_t>();
  config.epoch_duration_ms            = result["epoch"].as<size_t>();

  if (index == "OpenBwTree") {
    config.index_structure = decltype(config)::IndexStructure::OpenBwTree;
  } else if (index == "OpenBwPLI") {
    config.index_structure = decltype(config)::IndexStructure::OpenBwTreeWithPLI;
  } else if (index == "PLI") {
    config.index_structure = decltype(config)::IndexStructure::HashTableWithPrecisionLockingIndex;
  } else {
    std::cout << "There is no index named" << index << std::endl;
    std::cout << options.help() << std::endl;
    exit(0);
  }

  LineairDB::Database db(config);

  const auto use_handler = result["handler"].as<bool>();

  /** Configure the workload **/
  auto workload_type = result["workload"].as<std::string>();
  YCSB::Workload workload =
      YCSB::Workload::GeneratePredefinedWorkload(workload_type);

  workload.recordcount          = result["records"].as<size_t>();
  workload.zipfian_theta        = result["contention"].as<double>();
  workload.reps_per_txn         = result["ws"].as<size_t>();
  workload.payload_size         = result["payload"].as<size_t>();
  workload.client_thread_size   = result["clients"].as<size_t>();
  workload.measurement_duration = result["duration"].as<size_t>();

  /** Populate the table **/
  YCSB::PopulateDatabase(db, workload, std::thread::hardware_concurrency());

  /** Run the benchmark **/
  auto result_json = YCSB::RunBenchmark(db, workload, use_handler);
  auto& allocator  = result_json.GetAllocator();

  result_json.AddMember("workload",
                        rapidjson::Value(workload_type.c_str(), allocator),
                        allocator);
  result_json.AddMember(
      "contention",
      rapidjson::Value(std::to_string(workload.zipfian_theta).c_str(),
                       allocator),
      allocator);
  result_json.AddMember(
      "epoch",
      rapidjson::Value(std::to_string(config.epoch_duration_ms).c_str(),
                       allocator),
      allocator);
  result_json.AddMember("index", rapidjson::Value(index.c_str(), allocator),
                        allocator);
  result_json.AddMember(
      "protocol", rapidjson::Value(protocol.c_str(), allocator), allocator);
  result_json.AddMember("threads", static_cast<uint64_t>(config.max_thread),
                        allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  result_json.Accept(writer);
  writer.Flush();

  auto result_string   = buffer.GetString();
  auto output_filename = result["output"].as<std::string>();
  std::ofstream output_f(output_filename,
                         std::ofstream::out | std::ofstream::trunc);
  output_f << result_string;
  if (!output_f.good()) {
    std::cerr << "Unable to write output file" << output_filename << std::endl;
    exit(1);
  }

  auto throughput = result_json["tps"].GetInt64();
  auto etime      = result_json["etime"].GetInt64();
  auto aborts     = result_json["aborts"].GetInt64();
  auto commits    = result_json["commits"].GetInt64();

  size_t threads =
      use_handler ? workload.client_thread_size : config.max_thread;

  std::cerr << workload_type << "," << workload.payload_size << "," << config.epoch_duration_ms << ","
            << threads << "," << workload.zipfian_theta << "," << index << ","
            << protocol << "," << throughput << "," << etime << "," << commits
            << "," << aborts << std::endl;

  return 0;
}
