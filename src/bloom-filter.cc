//
// Created by Sam Hui on 2023-12-01.
//
#include "bloom-filter.hh"

#include <iostream>

#include "MurmurHash3.hh"

BloomFilter::BloomFilter(int64_t num_entries, int64_t custom_bits_per_entry)
    : num_entries(num_entries), M(custom_bits_per_entry) {
  init();
}

BloomFilter::BloomFilter(vector<int64_t> filter, int64_t num_entries,
                         int64_t bits_per_entry, vector<int64_t> seeds)
    : num_entries(num_entries),
      M(bits_per_entry),
      num_bits(num_entries * bits_per_entry),
      filter(filter),
      num_hash_functions(seeds.size()),
      seeds(seeds) {}

void BloomFilter::insert(int64_t key) {
  uint32_t hash_output;
  for (int i = 0; i < num_hash_functions; i++) {
    MurmurHash3_x86_32(&key, INT64_T_SIZE, seeds[i], &hash_output);
    int index = hash_output % num_bits;
    if (index < 0 || index >= num_bits)
      throw invalid_argument("Index in BloomFilter::insert out of bounds");
    setBit(index);
  }
}

bool BloomFilter::includes(int64_t key) {
  uint32_t hash_output;
  for (int i = 0; i < num_hash_functions; ++i) {
    MurmurHash3_x86_32(&key, INT64_T_SIZE, seeds[i], &hash_output);
    int index = hash_output % num_bits;
    if (index < 0 || index >= num_bits)
      throw invalid_argument("Index in BloomFilter::includes out of bounds");
    size_t filter_index = index / 64;
    if (filter_index >= filter.size() || filter_index < 0)
      throw invalid_argument(
          "filter_index in BloomFilter::includes out of bounds");
    int bit_index = index % 64;
    if (bit_index >= 64 || bit_index < 0)
      throw invalid_argument(
          "bit_index in in BloomFilter::includes out of bounds");

    if (!(filter[filter_index] & (1LL << bit_index))) {
      // If any bit is not set, the item is definitely not in the set
      return false;
    }
  }
  // If all bits are set, the item is probably in the set
  return true;
}
