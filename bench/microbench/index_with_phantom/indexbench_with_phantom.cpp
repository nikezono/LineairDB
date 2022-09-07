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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <chrono>
#include <cxxopts.hpp>
#include <experimental/filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <thread>
#include <unordered_set>
#include <variant>

#include "index/concurrent_table.h"
#include "lineairdb/config.h"
#include "spdlog/spdlog.h"

const std::string CHARACTERS =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

constexpr auto PopulationSize = 100000;

template <typename T>
void Population(T& index) {
  for (auto i = 0; i < PopulationSize; i++) {
    index.GetOrInsert(std::to_string(i));
  }
}
struct Result {
  double cps           = 0;
  double insert_aborts = 0;
  double scan_aborts   = 0;
};

template <typename T>
Result Benchmark(T& index, std::string benchmark_type, std::string structure,
                 size_t threads, size_t proportion, bool populated,
                 size_t duration) {
  std::atomic<size_t> count_down_latch(0);
  std::atomic<bool> end_flag(false);
  std::atomic<size_t> total_succeed(0);
  std::atomic<size_t> total_scan_aborts(0);
  std::atomic<size_t> total_insert_aborts(0);
  std::vector<std::future<void>> futures;
  const auto is_scan_bench = benchmark_type == "scan";

  for (size_t i = 0; i < threads; i++) {
    futures.push_back(std::async(std::launch::async, [&]() {
      size_t operation_succeed       = 0;
      size_t operation_scan_aborts   = 0;
      size_t operation_insert_aborts = 0;

      std::random_device seed_gen;
      std::mt19937 engine(seed_gen());
      std::uniform_int_distribution<> dist(0, 99);
      std::uniform_int_distribution<> dist_for_populated(0, PopulationSize);
      std::uniform_int_distribution<> random_string(0, CHARACTERS.size() - 1);

      count_down_latch++;

      for (;;) {
        if (end_flag.load()) {
          total_succeed.fetch_add(operation_succeed);
          total_scan_aborts.fetch_add(operation_scan_aborts);
          total_insert_aborts.fetch_add(operation_insert_aborts);
          break;
        };

        const auto r                 = static_cast<size_t>(dist(engine));
        const bool is_scan_operation = r < proportion;
        if (is_scan_bench && is_scan_operation) {
          std::string begin;
          std::string end;

          for (;;) {
            if (populated) {
              auto b = dist_for_populated(engine);
              if (PopulationSize <= b) b = PopulationSize - 1;
              begin  = std::to_string(b);
              auto e = b + dist(engine);
              if (PopulationSize <= e) e = PopulationSize - 1;
              end = std::to_string(e);
            } else {
              for (auto i = 0; i < 5; i++) {
                begin += CHARACTERS[random_string(engine)];
                end += CHARACTERS[random_string(engine)];
              }
            }

            if (begin < end) break;
            begin.clear();
            end.clear();
          }

          size_t hit    = 0;
          auto last_key = end;
          if (structure == "PLI" || structure == "OpenBw+PLI") {
            auto result = index.Scan(begin, end, [&](auto key) {
              hit++;
              if (100 <= hit) {
                last_key = key;
                return true;
              }
              return false;
            });
            if (result.has_value()) {
              operation_succeed++;
            } else {
              operation_scan_aborts++;
            }
          } else if (structure == "OPLI" || structure == "OpenBw+OPLI") {
            auto result = index.Scan(begin, end, [&](auto key) {
              hit++;
              if (100 <= hit) {
                last_key = key;
                return true;
              }
              return false;
            });
            if (index.ReScan(begin, last_key)) {
              operation_succeed++;
            } else {
              operation_scan_aborts++;
            }

          } else if (structure == "OpenBwTree") {
            std::unordered_set<decltype(begin)> hit_keys;
            index.Scan(begin, end, [&](auto key) {
              hit++;
              hit_keys.emplace(key);
              if (100 <= hit) {
                last_key = key;
                return true;
              }
              return false;
            });

            // Fence;
            size_t h     = 0;
            bool phantom = false;
            index.Scan(begin, last_key, [&](auto key) {
              if (hit_keys.count(std::string(key)) == 0) {
                phantom = true;
                return true;
              }
              h++;
              if (h == hit) { return true; }
              return false;
            });

            if (!phantom) {
              operation_succeed++;
            } else {
              operation_scan_aborts++;
            }
          } else {
            exit(EXIT_FAILURE);
          }

        } else {
          std::string key;
          if (populated) {
            auto i = dist_for_populated(engine);
            if (PopulationSize <= i) i = PopulationSize - 1;
            key = std::to_string(i);

          } else {
            for (auto i = 0; i < 5; i++) {
              key += CHARACTERS[random_string(engine)];
            }
          }

          bool success = true;
          auto* item   = index.Get(key);
          if (item == nullptr) {
            if (populated) {
              SPDLOG_ERROR("Populated but not exist {}", key);
              exit(EXIT_FAILURE);
            }
            success = index.Put(key, {});
          }
          if (success) {
            operation_succeed++;
          } else {
            if (!populated) operation_insert_aborts++;
          }
        }
      }
    }));
  }
  count_down_latch++;
  for (;;) {
    if (count_down_latch.load() == threads + 1) break;
    std::this_thread::yield();
  }
  const auto begin = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(duration));
  end_flag.store(true);
  for (auto& fut : futures) { fut.wait(); }
  const auto end = std::chrono::high_resolution_clock::now();

  const auto duration_ns =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
          .count();
  auto success_ops = (double(total_succeed.load()) / duration_ns) * 1000;
  auto insert_aborts_ops =
      (double(total_insert_aborts.load()) / duration_ns) * 1000;
  auto scan_aborts_ops =
      (double(total_scan_aborts.load()) / duration_ns) * 1000;

  return {success_ops, insert_aborts_ops, scan_aborts_ops};
}

int main(int argc, char** argv) {
  cxxopts::Options options("indexbench",
                           "Microbenchmark of various index structures");

  options.add_options()          //
      ("h,help", "Print usage")  //
      ("t,thread", "The number of worker threads",
       cxxopts::value<size_t>()->default_value(
           std::to_string(std::thread::hardware_concurrency())))  //
      ("T,type", "Type of benchmark",
       cxxopts::value<std::string>()->default_value("scan"))  //
      ("s,structure", "Index data structure",
       cxxopts::value<std::string>()->default_value("PLI"))  //
      ("p,proportion", "Proportion of 'scan' operation",
       cxxopts::value<size_t>()->default_value("10"))  //
      ("P,populated", "All data items are populated before benchmarking",
       cxxopts::value<bool>()->default_value("false"))  //
      ("d,duration", "Measurement duration of this benchmark (milliseconds)",
       cxxopts::value<size_t>()->default_value("2000"))  //
      ("o,output", "Output JSON filename",
       cxxopts::value<std::string>()->default_value(
           "indexbench_result.json"))  //
      ;

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  const uint64_t threads          = result["thread"].as<size_t>();
  const auto benchmark_type       = result["type"].as<std::string>();
  const auto measurement_duration = result["duration"].as<size_t>();
  const auto proportion           = result["proportion"].as<size_t>();
  const auto populated            = result["populated"].as<bool>();
  const auto structure            = result["structure"].as<std::string>();

  /** run benchmark **/
  double ops           = 0;
  double aps           = 0;
  double insert_aborts = 0;
  double scan_aborts   = 0;
  double abort_rate    = 0;

  {
    using namespace LineairDB::Index;

    LineairDB::Config config;
    if (structure == "PLI") {
      config.index_structure =
          decltype(config)::IndexStructure::HashTableWithPrecisionLockingIndex;
    } else if (structure == "OPLI") {
      config.index_structure = decltype(config)::IndexStructure::
          HashTableWithOptimisticPrecisionLockingIndex;
    } else if (structure == "OpenBwTree") {
      config.index_structure = decltype(config)::IndexStructure::OpenBwTree;
    } else if (structure == "OpenBw+PLI") {
      config.index_structure =
          decltype(config)::IndexStructure::OpenBwTreeWithPLI;
    } else if (structure == "OpenBw+OPLI") {
      config.index_structure =
          decltype(config)::IndexStructure::OpenBwTreeWithOPLI;
    } else {
      std::cout << "invalid structure name." << std::endl
                << options.help() << std::endl;
      return EXIT_FAILURE;
    }

    ConcurrentTable index(config);
    if (populated) {
      SPDLOG_INFO("IndexBench: index population starts.");
      Population<decltype(index)>(index);
      SPDLOG_INFO("IndexBench: population has finished.");
    }

    auto res =
        Benchmark<decltype(index)>(index, benchmark_type, structure, threads,
                                   proportion, populated, measurement_duration);
    ops           = res.cps;
    insert_aborts = res.insert_aborts;
    scan_aborts   = res.scan_aborts;
    aps           = insert_aborts + scan_aborts;
    abort_rate    = (aps / (ops + aps) * 100);
  }
  SPDLOG_INFO("IndexBench: measurement has finisihed.");
  SPDLOG_INFO(
      "Structure;CommitPS;InsertAbortsPS;ScanAbortsPS;AbortPS;AbortRate");
  SPDLOG_INFO("{0};{1};{2};{3};{4};{5}", structure, ops, insert_aborts,
              scan_aborts, aps, abort_rate);

  /** Output result as json format **/
  rapidjson::Document result_json(rapidjson::kObjectType);
  auto& allocator = result_json.GetAllocator();
  result_json.AddMember(
      "structure", rapidjson::Value(structure.c_str(), allocator), allocator);
  result_json.AddMember("threads", threads, allocator);
  result_json.AddMember("cps", ops, allocator);
  result_json.AddMember("aps", aps, allocator);
  result_json.AddMember("abort_insert_ps", insert_aborts, allocator);
  result_json.AddMember("abort_scan_ps", scan_aborts, allocator);
  result_json.AddMember("abort_rate", abort_rate, allocator);

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
  std::cout << "This benchmark result is saved into " << output_filename
            << std::endl;
  return 0;
}
