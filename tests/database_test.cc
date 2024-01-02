#include "../src/database.hh"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <iostream>

#include "constants.hh"
#include "database_test.hh"
using namespace std;

void deleteAllFilesInDirectory(const string& directoryPath) {
  DIR* dir = opendir(directoryPath.c_str());
  if (dir == nullptr) {
    cerr << "Database doesn't exist. Will create later" << directoryPath
         << endl;
    return;
  }

  struct dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (string(entry->d_name) == "." || string(entry->d_name) == "..") {
      continue;
    }

    string filePath = directoryPath + "/" + string(entry->d_name);

    struct stat path_stat;
    stat(filePath.c_str(), &path_stat);
    if (S_ISREG(path_stat.st_mode)) {
      if (unlink(filePath.c_str()) != 0) {
        cerr << "Failed to delete file: " << filePath << endl;
      }
    }
  }

  closedir(dir);
}

void createLargeSST() {
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

void createSmallSST() {
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

bool testPutAndGetMemtable(Database& database) {
  database.Put(2, 10);
  database.Put(3, 10);
  database.Put(4, 10);
  database.Put(6, 10);

  int64_t value = database.Get(4);
  if (value == 10) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testPutAndGetSST(Database& database) {
  database.Put(7, 10);
  database.Put(8, 10);
  database.Put(9, 10);
  database.Put(10, 10);

  int64_t value = database.Get(7);
  if (value == 10 || value == 7) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetValue(Database& database) {
  int64_t value = database.Get(7);
  if (value == 7) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetValue2(Database& database) {
  int64_t value = database.Get(890);
  if (value == 890) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetValue3(Database& database) {
  int64_t value = database.Get(357);
  if (value == 357) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetValue4(Database& database) {
  int64_t value = database.Get(1079);
  if (value == 1079) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetValue5(Database& database) {
  int64_t value = database.Get(659);
  if (value == 659) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetNotFound(Database& database) {
  int64_t value = database.Get(1281);
  if (value == -1) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetNotFound2(Database& database) {
  int64_t value = database.Get(2000);
  if (value == -1) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testGetNotFound3(Database& database) {
  int64_t value = database.Get(-20);
  if (value == -1) {
    return true;
  } else {
    cout << "Expected value: " << value << "\n";
    return false;
  }
}

bool testScanQuery(Database& database) {
  ScanResponse scanResponse = database.Scan(1, 5);

  vector<KeyValuePair> correctVector = {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}};
  if (scanResponse.size != 5) {
    return false;
  }

  vector<KeyValuePair> values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScanOverPages(Database& database) {
  ScanResponse scanResponse = database.Scan(254, 260);

  vector<KeyValuePair> correctVector = {{254, 254}, {255, 255}, {256, 256},
                                        {257, 257}, {258, 258}, {259, 259},
                                        {260, 260}};
  if (scanResponse.size != 7) {
    return false;
  }

  vector<KeyValuePair> values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScanMultipleSSTs(Database& database) {
  ScanResponse scanResponse = database.Scan(700, 704);

  vector<KeyValuePair> correctVector = {
      {700, 700}, {701, 701}, {702, 702}, {703, 703}, {704, 704}};
  if (scanResponse.size != 5) {
    return false;
  }

  vector<KeyValuePair> values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScanMemtableAndSST(Database& database) {
  ScanResponse scanResponse = database.Scan(8, 11);

  vector<KeyValuePair> correctVector = {{8, 10}, {9, 10}, {10, 10}, {11, 11}};
  if (scanResponse.size != 4) {
    return false;
  }

  vector<KeyValuePair> values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScanOutOfRange(Database& database) {
  // This scans out of the range of the database, Should not get any values for
  // 1281 and 1282
  ScanResponse scanResponse = database.Scan(1277, 1282);

  vector<KeyValuePair> correctVector = {
      {1277, 1277}, {1278, 1278}, {1279, 1279}, {1280, 1280}};
  if (scanResponse.size != 4) {
    return false;
  }

  vector<KeyValuePair> values = scanResponse.result;

  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].key != correctVector[i].key ||
        values[i].value != correctVector[i].value) {
      return false;
    }
  }

  return true;
}

bool testScanOutOfRange2(Database& database) {
  // This scans out of the range of the database, Should not get any values
  ScanResponse scanResponse = database.Scan(99995, 99999);

  if (scanResponse.size != 0) {
    return false;
  }

  return true;
}

bool runDatabaseTests() {
  deleteAllFilesInDirectory("tests/ssts/database_test");

  Database database(5, 5);
  database.set_bufferpool_enabled(false);
  database.Open("tests/ssts/database_test", SORTED_SST);
  int test_pass_counter = 0;
  int total_tests = 0;

  cout << "RUNNING DATABASE.CC TESTS\n\n";

  cout << "Running testPutAndGetMemtable\n";
  if (testPutAndGetMemtable(database)) {
    cout << "testPutAndGetMemtable passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testPutAndGetMemtable failed.\n";
  }
  total_tests += 1;

  cout << "Running testPutAndGetSST\n";
  if (testPutAndGetSST(database)) {
    cout << "testPutAndGetSST passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testPutAndGetSST failed.\n";
  }
  total_tests += 1;

  createLargeSST();
  database.get_ssts_from_db("tests/ssts/database_test");

  cout << "Running testGetValue\n";
  if (testGetValue(database)) {
    cout << "testGetValue passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetValue failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetValue2\n";
  if (testGetValue2(database)) {
    cout << "testGetValue2 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetValue2 failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetValue3\n";
  if (testGetValue3(database)) {
    cout << "testGetValue3 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetValue3 failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetValue4\n";
  if (testGetValue4(database)) {
    cout << "testGetValue4 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetValue4 failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetValue5\n";
  if (testGetValue5(database)) {
    cout << "testGetValue5 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetValue5 failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetNotFound\n";
  if (testGetNotFound(database)) {
    cout << "testGetNotFound passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetNotFound failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetNotFound2\n";
  if (testGetNotFound2(database)) {
    cout << "testGetNotFound2 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetNotFound2 failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetNotFound3\n";
  if (testGetNotFound3(database)) {
    cout << "testGetNotFound3 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetNotFound3 failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanQuery\n";
  if (testScanQuery(database)) {
    cout << "testScanQuery passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanQuery failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanOverPages\n";
  if (testScanOverPages(database)) {
    cout << "testScanOverPages passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanOverPages failed.\n";
  }
  total_tests += 1;

  createSmallSST();

  cout << "Running testScanMultipleSSTs\n";
  if (testScanMultipleSSTs(database)) {
    cout << "testScanMultipleSSTs passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanMultipleSSTs failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanMemtableAndSST\n";
  if (testScanMemtableAndSST(database)) {
    cout << "testScanMemtableAndSST passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanMemtableAndSST failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanOutOfRange\n";
  if (testScanOutOfRange(database)) {
    cout << "testScanOutOfRange passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanOutOfRange failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanOutOfRange2\n";
  if (testScanOutOfRange2(database)) {
    cout << "testScanOutOfRange2 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanOutOfRange2 failed.\n";
  }
  total_tests += 1;

  cout << "\nTotal of " << test_pass_counter << "/" << total_tests
       << " passed in database.cc\n";
  return test_pass_counter == total_tests;
}
