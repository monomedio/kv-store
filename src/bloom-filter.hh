

#ifndef CSC443_PROJECT_BLOOM_FILTER_HH
#define CSC443_PROJECT_BLOOM_FILTER_HH

#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>

#include "constants.hh"

using namespace std;

class BloomFilter {
 private:
  int64_t num_entries;
  int64_t M;  // Bits per entry
  int64_t num_bits;
  vector<int64_t> filter;
  int64_t num_hash_functions;
  vector<int64_t> seeds;

  void init() {
    if (num_entries <= 0) throw invalid_argument("num_entries must be > 0");
    if (M <= 0) throw invalid_argument("Bits per entry must be > 0");
    num_bits = num_entries * M;
    filter.resize(ceil((double)num_bits / 64), 0);
    num_hash_functions = (int64_t)ceil((log(2)) * M);
    seeds.resize(num_hash_functions);

    random_device rd;
    mt19937 gen(rd());
    for (int i = 0; i < num_hash_functions; ++i) {
      seeds[i] = gen();
    }
    assert(num_hash_functions == (int)seeds.size());
  }

  void setBit(int index) {
    int filter_index = index / 64;
    if (filter_index >= (int)filter.size() || filter_index < 0)
      throw invalid_argument("filter_index in setBit out of bounds");
    int bit_index = index % 64;
    if (bit_index >= 64 || bit_index < 0)
      throw invalid_argument("bit_index in in setBit out of bounds");
    filter[filter_index] |= (1LL << bit_index);
  }

 public:
  // Constructor for flushing to SST
  explicit BloomFilter(int64_t num_entries, int64_t custom_bits_per_entry = 10);

  // Constructor for reading filter from SST
  BloomFilter(vector<int64_t> filter, int64_t num_entries,
              int64_t bits_per_entry, vector<int64_t> seeds);

  // Getter methods
  int64_t get_num_entries() const { return num_entries; }

  int64_t get_num_bits() const { return num_bits; }

  int64_t get_bits_per_entry() const { return M; }

  int64_t get_num_hash_functions() const { return num_hash_functions; }

  // Returns number of uint64_t (8-byte) integers used to store bits
  int64_t get_filter_size() { return (int64_t)filter.size(); }

  vector<int64_t> get_seeds() { return seeds; }

  vector<int64_t> get_filter() { return filter; }

  void insert(int64_t key);

  bool includes(int64_t key);
};

#endif  // CSC443_PROJECT_BLOOM_FILTER_HH
