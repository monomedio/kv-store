#include "database_update_delete_test.hh"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <iostream>

#include "../src/database.hh"
#include "constants.hh"
#include "database_test.hh"
using namespace std;

void createLargeSSTWithUpdate() {
  string filename =
      "tests/ssts/database_test/SST_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count()) +
      ".bin";
  ofstream file(filename, ios::binary);

  if (!file.is_open()) {
    cerr << "Could not open file for writing.\n";
    return;
  }

  const int NUM_PAGES = 5;
  const int NUM_PAIRS_PER_PAGE = PAGE_SIZE / (sizeof(int64_t) * 2);
  const int TOTAL_PAIRS = NUM_PAGES * NUM_PAIRS_PER_PAGE;

  for (int i = 1; i <= TOTAL_PAIRS; ++i) {
    int64_t key = i;
    int64_t value = i;

    file.write(reinterpret_cast<const char*>(&key), sizeof(key));
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  file.close();
  cout << "File generated.\n";
}

void createSmallSSTWithUpdate() {
  string filename =
      "tests/ssts/database_test/SST_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count()) +
      ".bin";
  ofstream file(filename, ios::binary);

  if (!file.is_open()) {
    cerr << "Could not open file for writing.\n";
    return;
  }

  const int NUM_PAGES = 2;
  const int NUM_PAIRS_PER_PAGE = PAGE_SIZE / (sizeof(int64_t) * 2);
  const int TOTAL_PAIRS = NUM_PAGES * NUM_PAIRS_PER_PAGE;

  for (int i = 600; i <= TOTAL_PAIRS + 601; ++i) {
    int64_t key = i;
    int64_t value = 999;

    file.write(reinterpret_cast<const char*>(&key), sizeof(key));
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  file.close();
  cout << "File generated.\n";
}

bool testUpdateAndGetMemtable(Database& database) {
  /* Test Get from Memtable */
  database.Put(2, 2);
  database.Put(3, 10);
  database.Put(4, 10);
  database.Put(5, 10);
  database.Update(2, 10);
  int64_t value = database.Get(2);
  if (value == 10) {
    return true;
  } else {
    cerr << "Update and Get from Memtable Failed: Expected value: " << 10
         << " Actual: " << value << "\n";
    return false;
  }
  database.Put(6, 10);
  /* Database memtable size = 5 entries(2,3,4,5,6), flushed to SST */
  database.Update(2, 5);
  database.Update(3, 10);
  database.Update(4, 10);
  database.Update(5, 10);
  value = database.Get(2);
  if (value == 5) {
    return true;
  } else {
    cerr << "Update and Get from Memtable Failed: Expected value: " << 5
         << " Actual: " << value << "\n";
    return false;
  }
  database.Update(6, 10);
  /* Database memtable size = 5 entries(2,3,4,5,6), flushed to SST */
  database.Put(7, 10);
  database.Put(8, 10);
  database.Put(9, 10);
  database.Put(10, 10);
  database.Put(11, 10);
  value = database.Get(2);
  if (value == 5) {
    return true;
  } else {
    cerr << "Update and Get from Memtable Failed: Expected value: " << 5
         << " Actual: " << value << "\n";
    return false;
  }
}

bool testDeleteAndGetSST(Database& database) {
  database.Put(7, 70);
  int64_t value = database.Get(7);
  if (value == 70) {
    return true;
  } else {
    cerr << "Update and Get from Memtable Failed: Expected value: " << 70
         << " Actual: " << value << "\n";
    return false;
  }
  database.Delete(7);
  if (value == -1) {
    return true;
  } else {
    cerr << "Key should be deleted but has a value: " << value << "\n";
    return false;
  }
  database.Put(8, 10);
  database.Put(5, 10);
  database.Put(3, 10);
  database.Put(2, 10);
  /* Database memtable size = 5 entries, flushed to SST */
  value = database.Get(7);
  if (value == -1) {
    return true;
  } else {
    cerr << "Key should be deleted but has a value: " << value << "\n";
    return false;
  }
  return true;
}

bool testScanQueryAfterUpdate(Database& database) {
  ScanResponse scanResponse = database.Scan(1, 5);

  vector<KeyValuePair> correctVector = {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}};
  if (scanResponse.size != 5) {
    cerr << "scanResponse.size wrong. Expected: " << 5
         << " Actual: " << scanResponse.size;
    return false;
  }

  vector<KeyValuePair> values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  /* Scan with entries updated */

  database.Put(1, 10);
  database.Put(2, 10);
  database.Put(3, 10);
  database.Put(4, 10);
  database.Put(5, 10);

  scanResponse = database.Scan(1, 5);
  correctVector = {{1, 10}, {2, 10}, {3, 10}, {4, 10}, {5, 10}};
  if (scanResponse.size != 5) {
    cerr << "scanResponse.size wrong. Expected: " << 5
         << " Actual: " << scanResponse.size;
    return false;
  }

  values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  /* Scan with entries over pages */

  database.Update(260, 1);
  scanResponse = database.Scan(254, 260);
  correctVector = {{254, 254}, {255, 255}, {256, 256}, {257, 257},
                   {258, 258}, {259, 259}, {260, 1}};
  if (scanResponse.size != 7) {
    cerr << "scanResponse.size wrong. Expected: " << 7
         << " Actual: " << scanResponse.size;
    return false;
  }

  values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      cerr << "Value wrong. Expected: " << correctVector[i].key << " "
           << correctVector[i].value << " Actual: " << values[i].key << " "
           << values[i].value << endl;
      return false;
    }
  }

  database.Update(259, 1);
  database.Update(258, 1);
  database.Update(256, 1);
  database.Update(255, 1);
  database.Update(254, 2);

  /* Scan with entries over pages, SST, memtable*/
  database.Update(260, 2);
  scanResponse = database.Scan(253, 260);
  correctVector = {{253, 253}, {254, 2}, {255, 1}, {256, 1},
                   {257, 257}, {258, 1}, {259, 1}, {260, 2}};
  if (scanResponse.size != 8) {
    cerr << "scanResponse.size wrong. Expected: " << 8
         << " Actual: " << scanResponse.size;
    return false;
  }

  values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      cerr << "Value wrong. Expected: " << correctVector[i].key << " "
           << correctVector[i].value << " Actual: " << values[i].key << " "
           << values[i].value << endl;
      return false;
    }
  }
  return true;
}

bool testScanQueryAfterDelete(Database& database) {
  /* Scan with entries deleted */

  database.Delete(1);
  database.Delete(2);
  database.Delete(3);
  database.Delete(4);
  database.Delete(5);

  ScanResponse scanResponse = database.Scan(1, 5);
  vector<KeyValuePair> values = scanResponse.result;
  if (scanResponse.size != 0) {
    cerr << "scanResponse.size wrong. Expected: " << 0
         << " Actual: " << scanResponse.size;
    for (int i = 0; i < scanResponse.size; ++i) {
      cerr << "; " << values[i].key << " " << values[i].value;
    }
    cerr << endl;
    return false;
  }

  database.Delete(260);
  database.Delete(257);
  scanResponse = database.Scan(253, 260);
  vector<KeyValuePair> correctVector = {{253, 253}, {254, 2}, {255, 1},
                                        {256, 1},   {258, 1}, {259, 1}};

  values = scanResponse.result;
  if (scanResponse.size != 6) {
    cerr << "scanResponse.size wrong. Expected: " << 7
         << " Actual: " << scanResponse.size;
    return false;
  }
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      cerr << "Value wrong. Expected: " << correctVector[i].key << " "
           << correctVector[i].value << " Actual: " << values[i].key << " "
           << values[i].value << endl;
      return false;
    }
  }
  return true;
}

bool runDatabaseUpdateDeleteTests() {
  deleteAllFilesInDirectory("tests/ssts/database_test");

  Database database(5, 5);
  database.set_bufferpool_enabled(true);
  database.Open("tests/ssts/database_test", SORTED_SST);
  int test_pass_counter = 0;
  int total_tests = 0;

  cout << "Running testUpdateAndGetMemtable\n";
  if (testUpdateAndGetMemtable(database)) {
    cout << "testUpdateAndGetMemtable passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testUpdateAndGetMemtable failed.\n";
  }
  total_tests += 1;

  cout << "Running testDeleteAndGetSST\n";
  if (testDeleteAndGetSST(database)) {
    cout << "testDeleteAndGetSST passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testDeleteAndGetSST failed.\n";
  }
  total_tests += 1;
  createSmallSSTWithUpdate();
  createLargeSSTWithUpdate();
  database.get_ssts_from_db("tests/ssts/database_test");

  cout << "Running testScanQueryAfterUpdate\n";
  if (testScanQueryAfterUpdate(database)) {
    cout << "testScanQueryAfterUpdate passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanQueryAfterUpdate failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanQueryAfterDelete\n";
  if (testScanQueryAfterDelete(database)) {
    cout << "testScanQueryAfterDelete passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanQueryAfterDelete failed.\n";
  }
  total_tests += 1;

  cout << "\nTotal of " << test_pass_counter << "/" << total_tests
       << " passed in UPDATE DELETE TEST \n";
  return test_pass_counter == total_tests;
}
