#include "../src/memtable.hh"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

#include "memtable_test.hh"
using namespace std;

bool testPut(Memtable& memtable, int64_t key, const int64_t& value) {
  memtable.put(key, value);
  int64_t val = memtable.get(key);
  if (val == value) {
    return true;
  } else {
    return false;
  }
}

bool testLeftRotation(Memtable& memtable) {
  memtable.put(4, 4);
  memtable.put(6, 6);
  memtable.put(8, 8);

  int64_t val = memtable.get(4);
  if (val == 4) {
    return true;
  } else {
    return false;
  }
}

bool testSSTFlush(Memtable& memtable) {
  // When the memtable hits 10 entries we need to flush to SST
  memtable.put(9, 9);
  memtable.put(28, 28);
  memtable.put(2, 2);
  memtable.put(16, 16);
  memtable.put(18, 18);
  memtable.put(5, 5);

  int64_t val = memtable.get(5);
  if (val == -1) {
    return true;
  } else {
    return false;
  }
}

bool testValidSST(const std::string& sst_file) {
  std::ifstream file(sst_file, std::ios::binary);
  if (!file.is_open()) {
    std::cerr << "Could not open file: " << sst_file << std::endl;
    return false;
  }

  int64_t currentKey, currentValue;
  int64_t previousKey = std::numeric_limits<int64_t>::min();

  while (
      file.read(reinterpret_cast<char*>(&currentKey), sizeof(currentKey)) &&
      file.read(reinterpret_cast<char*>(&currentValue), sizeof(currentValue))) {
    if (currentKey == 0) {
      break;
    }

    if (currentKey <= previousKey) {
      std::cerr << "File is not sorted. Previous key: " << previousKey
                << ", Current key: " << currentKey << std::endl;
      return false;
    }

    previousKey = currentKey;
  }

  return true;
}

bool testPutSameKey(Memtable& memtable) {
  memtable.put(12, 12);
  memtable.put(13, 13);
  memtable.put(12, 44);

  int64_t val = memtable.get(12);
  if (val == 44) {
    return true;
  } else {
    return false;
  }
}

bool testScan(Memtable& memtable) {
  memtable.put(16, 16);
  memtable.put(17, 17);
  memtable.put(21, 21);
  memtable.put(6, 6);
  memtable.put(2, 2);
  memtable.put(10, 10);

  vector<KeyValuePair> values = memtable.scan(5, 16);

  vector<KeyValuePair> correctVector = {
      {6, 6}, {10, 10}, {12, 44}, {13, 13}, {16, 16}};
  if (values.size() != correctVector.size()) {
    return false;
  }

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScan2(Memtable& memtable) {
  memtable.put(78, 78);
  memtable.put(22, 22);
  memtable.put(29, 29);
  memtable.put(4, 4);
  memtable.put(11, 11);
  memtable.put(3, 3);

  vector<KeyValuePair> values = memtable.scan(20, 100);
  vector<KeyValuePair> correctVector = {{21, 21}, {22, 22}, {29, 29}, {78, 78}};

  if (values.size() != correctVector.size()) {
    return false;
  }

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScan3(Memtable& memtable) {
  vector<KeyValuePair> values = memtable.scan(2, 10);
  vector<KeyValuePair> correctVector = {
      {2, 2}, {3, 3}, {4, 4}, {6, 6}, {10, 10}};
  if (values.size() != correctVector.size()) {
    return false;
  }

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testCloseDatabase(Memtable& memtable) {
  // This test shold close the memtable and will do the same in the database
  memtable.close();

  return memtable.get_size() == 0;
}

bool runMemtableTests() {
  struct stat info;
  if (stat("tests/ssts/memtable_test", &info) != 0) {
    // We need to create the directory
    mkdir("tests/ssts/memtable_test", 0777);
  }

  Memtable memtable(10);
  memtable.set_db_name("tests/ssts/memtable_test");

  int test_pass_counter = 0;
  int total_tests = 0;

  cout << "Running testPut\n";
  if (testPut(memtable, 3, 3)) {
    cout << "testPut passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testPut failed.\n";
  }
  total_tests += 1;

  cout << "Testing left rotation on tree\n";
  if (testLeftRotation(memtable)) {
    cout << "testLeftRotation passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testLeftRotation failed\n";
  }
  total_tests += 1;

  cout << "Test memtable flush to SST\n";
  if (testSSTFlush(memtable)) {
    cout << "testSSTFlush passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testSSTFlush failed\n";
  }
  total_tests += 1;

  cout << "Test SST is valid file\n";
  if (testValidSST("tests/ssts/memtable_test/SST_0.bin")) {
    cout << "testValidSST passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testValidSST failed\n";
  }
  total_tests += 1;

  Memtable memtable2(20);
  memtable2.set_db_name("tests/ssts/memtable_test2");

  cout << "Test put for same key\n";
  if (testPutSameKey(memtable2)) {
    cout << "testValidSST passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testValidSST failed\n";
  }
  total_tests += 1;

  cout << "Test scan range\n";
  if (testScan(memtable2)) {
    cout << "testScan passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScan failed\n";
  }
  total_tests += 1;

  cout << "Test scan range 2\n";
  if (testScan2(memtable2)) {
    cout << "testScan2 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScan2 failed\n";
  }
  total_tests += 1;

  cout << "Test scan range 3\n";
  if (testScan3(memtable2)) {
    cout << "testScan3 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScan3 failed\n";
  }
  total_tests += 1;

  cout << "Test close\n";
  if (testCloseDatabase(memtable2)) {
    cout << "testCloseDatabase passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testCloseDatabase failed\n";
  }
  total_tests += 1;

  cout << "\nTotal of " << test_pass_counter << "/" << total_tests
       << " passed in memtable.cc\n";
  return test_pass_counter == total_tests;
}