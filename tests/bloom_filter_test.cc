//
// Created by Sam Hui on 2023-12-02.
//
#include "bloom_filter_test.hh"

#include <cassert>

#include "../src/bloom-filter.hh"

bool allKeysPresent() {
  int num_entries = 256;
  BloomFilter test_filter(num_entries);
  assert(test_filter.get_bits_per_entry() == 10);
  assert(test_filter.get_filter_size() == 40);
  assert(test_filter.get_num_bits() == num_entries * 10);
  assert(test_filter.get_num_entries() == num_entries);
  assert(test_filter.get_num_hash_functions() ==
         (int)ceil(log(2) * test_filter.get_bits_per_entry()));

  for (int i = 0; i < num_entries; i++) {
    test_filter.insert(i + 1);
    if (!test_filter.includes(i + 1)) {
      return false;
    }
  }

  return true;
}

bool duplicateElements() {
  int num_entries = 2;
  BloomFilter test_filter(num_entries);
  test_filter.insert(1);
  test_filter.insert(1);
  return test_filter.includes(1);
}

bool numEntriesOne() {
  int num_entries = 1;
  BloomFilter test_filter(num_entries);
  assert(test_filter.get_bits_per_entry() == 10);
  assert(test_filter.get_filter_size() == 1);
  assert(test_filter.get_num_bits() == num_entries * 10);
  assert(test_filter.get_num_entries() == num_entries);
  assert(test_filter.get_num_hash_functions() ==
         (int)ceil(log(2) * test_filter.get_bits_per_entry()));

  test_filter.insert(1);
  return test_filter.includes(1);
}

bool testConstructor() {
  int num_entries = 256;
  BloomFilter test_filter(num_entries);

  for (int i = 0; i < num_entries; i++) {
    test_filter.insert(i + 1);
  }

  BloomFilter second_constructor(
      test_filter.get_filter(), test_filter.get_num_entries(),
      test_filter.get_bits_per_entry(), test_filter.get_seeds());

  for (int i = 0; i < num_entries; i++) {
    if (!second_constructor.includes(i + 1)) {
      return false;
    }
  }

  if (test_filter.get_bits_per_entry() !=
      second_constructor.get_bits_per_entry()) {
    return false;
  } else if (test_filter.get_num_entries() !=
             second_constructor.get_num_entries()) {
    return false;
  } else if (test_filter.get_num_bits() != second_constructor.get_num_bits()) {
    return false;
  } else if (test_filter.get_filter_size() !=
             second_constructor.get_filter_size()) {
    return false;
  } else if (test_filter.get_num_hash_functions() !=
             second_constructor.get_num_hash_functions()) {
    return false;
  }

  vector<int64_t> test_filter_filter = test_filter.get_filter();
  vector<int64_t> second_constructor_filter = second_constructor.get_filter();

  for (int i = 0; i < second_constructor.get_filter_size(); i++) {
    if (test_filter_filter[i] != second_constructor_filter[i]) {
      return false;
    }
  }

  vector<int64_t> test_filter_seeds = test_filter.get_seeds();
  vector<int64_t> second_constructor_seeds = second_constructor.get_seeds();

  for (int i = 0; i < second_constructor.get_num_hash_functions(); i++) {
    if (test_filter_seeds[i] != second_constructor_seeds[i]) {
      return false;
    }
  }

  return true;
}

bool runBloomFilterTests() {
  int tests_passed = 0;
  int num_tests = 0;

  cout << "Running allKeysPresent\n";
  if (allKeysPresent()) {
    cout << "allKeysPresent passed.\n";
    tests_passed += 1;
  } else {
    cout << "allKeysPresent failed.\n";
  }
  num_tests += 1;

  cout << "Running duplicateElements\n";
  if (duplicateElements()) {
    cout << "duplicateElements passed.\n";
    tests_passed += 1;
  } else {
    cout << "duplicateElements failed.\n";
  }
  num_tests += 1;

  cout << "Running numEntriesOne\n";
  if (numEntriesOne()) {
    cout << "numEntriesOne passed.\n";
    tests_passed += 1;
  } else {
    cout << "numEntriesOne failed.\n";
  }
  num_tests += 1;

  cout << "Running testConstructor\n";
  if (testConstructor()) {
    cout << "testConstructor passed.\n";
    tests_passed += 1;
  } else {
    cout << "testConstructor failed.\n";
  }
  num_tests += 1;

  cout << "\nTotal of " << tests_passed << "/" << num_tests
       << " passed in BLOOM FILTER TESTS\n";
  return tests_passed == num_tests;
}
