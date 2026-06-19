#ifndef LINEAIRDB_BLOOM_FILTER_HPP
#define LINEAIRDB_BLOOM_FILTER_HPP

#include <array>
#include <cstddef>
#include <cstdint>

namespace LineairDB {

/**
 * @brief Bloom filter for Dirty Summary in TES.
 *
 * Template parameter Words controls the size (Words * 64 bits).
 * Default Words=4 gives 256 bits.
 *
 * No dependency on LineairDB internal types.
 * NOT thread-safe; designed for worker-local use only.
 */
template <size_t Words = 4>
class BloomFilter {
  static_assert(Words >= 1, "BloomFilter requires at least 1 word");
  std::array<uint64_t, Words> bits_{};

  static uint64_t Hash0(const void* p) noexcept {
    uint64_t h = reinterpret_cast<uint64_t>(p);
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return h;
  }
  static uint64_t Hash1(const void* p) noexcept {
    uint64_t h = Hash0(p) ^ 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
  }

 public:
  void Reset() noexcept { bits_.fill(0); }

  void Add(const void* key) noexcept {
    auto h0 = Hash0(key);
    bits_[(h0 >> 6) % Words] |= (uint64_t(1) << (h0 & 63));
    auto h1 = Hash1(key);
    bits_[(h1 >> 6) % Words] |= (uint64_t(1) << (h1 & 63));
  }

  bool Hit(const void* key) const noexcept {
    auto h0 = Hash0(key);
    if (!(bits_[(h0 >> 6) % Words] & (uint64_t(1) << (h0 & 63)))) return false;
    auto h1 = Hash1(key);
    return bits_[(h1 >> 6) % Words] & (uint64_t(1) << (h1 & 63));
  }
};

// Default: 256-bit (4 words)
using BloomFilter256 = BloomFilter<>;

}  // namespace LineairDB

#endif  // LINEAIRDB_BLOOM_FILTER_HPP
