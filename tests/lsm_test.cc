#include "lsm_test.hh"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

#include "../src/database.hh"
#include "constants.hh"
using namespace std;

void createFirstSST(Database& database) {
  database.Put(1, 1);
  database.Put(3, 3);
  database.Put(5, 5);
  database.Put(7, 7);
  database.Put(9, 9);
}

void createSecondSST(Database& database) {
  database.Put(2, 2);
  database.Put(4, 4);
  database.Put(6, 6);
  database.Put(8, 8);
  database.Put(10, 10);
}

void createThirdSST(Database& database) {
  database.Put(11, 11);
  database.Put(13, 13);
  database.Put(15, 15);
  database.Put(17, 17);
  database.Put(19, 19);
}

void createFourthSST(Database& database) {
  database.Put(12, 12);
  database.Put(14, 14);
  database.Put(16, 16);
  database.Put(18, 18);
  database.Put(20, 20);
}

void createDeletedData(Database& database) {
  database.Put(1, 0);
  database.Put(3, 0);
  database.Put(5, 0);
  database.Put(7, 0);
  database.Put(11, 0);
  database.Put(13, 0);
  database.Put(15, 0);
  database.Put(17, 99);
  database.Put(19, 0);
  database.Put(9, 0);
  database.Put(2, 0);
  database.Put(4, 0);
  database.Put(6, 0);
  database.Put(8, 0);
  database.Put(10, 0);
  database.Put(12, 0);
  database.Put(14, 0);
  database.Put(16, 0);
  database.Put(18, 0);
  database.Put(20, 0);
}

bool testCompactionFileCreated(const string& directoryPath) {
  DIR* dir = opendir(directoryPath.c_str());
  if (dir == nullptr) {
    std::cerr << "Error: Unable to open directory." << std::endl;
    return false;
  }

  size_t fileCount = 0;
  struct dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (entry->d_type == DT_REG) {
      fileCount++;
    }
  }
  closedir(dir);

  return fileCount == 1;
}

bool testGetAfterCompaction(Database& database) {
  int64_t value1 = database.Get(4);
  if (value1 != 4) {
    cout << "Expected value: " << value1 << "\n";
    return false;
  }

  int64_t value2 = database.Get(1);
  if (value2 != 1) {
    cout << "Expected value: " << value2 << "\n";
    return false;
  }

  int64_t value3 = database.Get(9);
  if (value3 != 9) {
    cout << "Expected value: " << value3 << "\n";
    return false;
  }
  return true;
}

bool testScanAfterCompaction(Database& database) {
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

  ScanResponse scanResponse2 = database.Scan(3, 7);

  vector<KeyValuePair> correctVector2 = {
      {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}};
  if (scanResponse2.size != 5) {
    return false;
  }

  vector<KeyValuePair> values2 = scanResponse2.result;

  for (size_t i = 0; i < values2.size(); ++i) {
    if (values2[i].key != correctVector2[i].key ||
        values2[i].value != correctVector2[i].value) {
      return false;
    }
  }

  ScanResponse scanResponse3 = database.Scan(9, 12);

  vector<KeyValuePair> correctVector3 = {{9, 9}, {10, 10}};
  if (scanResponse3.size != 2) {
    return false;
  }

  vector<KeyValuePair> values3 = scanResponse3.result;

  for (size_t i = 0; i < values3.size(); ++i) {
    if (values3[i].key != correctVector3[i].key ||
        values3[i].value != correctVector3[i].value) {
      return false;
    }
  }

  return true;
}

bool testGetAfterCompactionLvl3(Database& database) {
  int64_t value1 = database.Get(4);
  if (value1 != 4) {
    cout << "Expected value: " << value1 << "\n";
    return false;
  }

  int64_t value2 = database.Get(1);
  if (value2 != 1) {
    cout << "Expected value: " << value2 << "\n";
    return false;
  }

  int64_t value3 = database.Get(9);
  if (value3 != 9) {
    cout << "Expected value: " << value3 << "\n";
    return false;
  }

  int64_t value4 = database.Get(16);
  if (value4 != 16) {
    cout << "Expected value: " << value4 << "\n";
    return false;
  }

  int64_t value5 = database.Get(20);
  if (value5 != 20) {
    cout << "Expected value: " << value5 << "\n";
    return false;
  }
  return true;
}

bool testScanAfterCompactionLvl3(Database& database) {
  ScanResponse scanResponse = database.Scan(8, 12);

  vector<KeyValuePair> correctVector = {
      {8, 8}, {9, 9}, {10, 10}, {11, 11}, {12, 12}};
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

  ScanResponse scanResponse2 = database.Scan(3, 7);

  vector<KeyValuePair> correctVector2 = {
      {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}};
  if (scanResponse2.size != 5) {
    return false;
  }

  vector<KeyValuePair> values2 = scanResponse2.result;

  for (size_t i = 0; i < values2.size(); ++i) {
    if (values2[i].key != correctVector2[i].key ||
        values2[i].value != correctVector2[i].value) {
      return false;
    }
  }

  ScanResponse scanResponse3 = database.Scan(19, 24);

  vector<KeyValuePair> correctVector3 = {{19, 19}, {20, 20}};
  if (scanResponse3.size != 2) {
    return false;
  }

  vector<KeyValuePair> values3 = scanResponse3.result;

  for (size_t i = 0; i < values3.size(); ++i) {
    if (values3[i].key != correctVector3[i].key ||
        values3[i].value != correctVector3[i].value) {
      return false;
    }
  }

  return true;
}

bool testSSTAfterClose(Database& database) {
  int64_t value1 = database.Get(4);
  if (value1 != 4) {
    cout << "Expected value: " << value1 << "\n";
    return false;
  }

  int64_t value5 = database.Get(20);
  if (value5 != 20) {
    cout << "Expected value: " << value5 << "\n";
    return false;
  }

  ScanResponse scanResponse = database.Scan(8, 12);

  vector<KeyValuePair> correctVector = {
      {8, 8}, {9, 9}, {10, 10}, {11, 11}, {12, 12}};
  if (scanResponse.size != 5) {
    return false;
  }

  return true;
}

bool testSSTAfterUpdateDelete(Database& database) {
  int64_t value1 = database.Get(8);
  if (value1 != -1) {
    cout << "Expected value: " << value1 << "\n";
    return false;
  }

  int64_t value5 = database.Get(17);
  if (value5 != 99) {
    cout << "Expected value: " << value5 << "\n";
    return false;
  }

  ScanResponse scanResponse = database.Scan(13, 20);

  vector<KeyValuePair> correctVector = {{17, 99}};
  if (scanResponse.size != 1) {
    return false;
  }

  return true;
}

bool runLSMTests() {
  string dir_path = "tests/ssts/lsm_test";
  deleteAllFilesInDirectory(dir_path);
  if (rmdir(dir_path.c_str()) != 0) {
    cerr << "Error removing directory: " << endl;
  }

  Database database(5, 5);
  database.set_bufferpool_enabled(true);
  database.Open(dir_path, LSM_TREE);
  int test_pass_counter = 0;
  int total_tests = 0;

  cout << "RUNNING LSM TESTS\n\n";

  createFirstSST(database);

  // After this runs, LSM compaction should run because there are 2 SSTS at
  // level 1
  createSecondSST(database);

  cout << "Running testCompactionFileCreated\n";
  if (testCompactionFileCreated(dir_path)) {
    cout << "testCompactionFileCreated passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testCompactionFileCreated failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetAfterCompaction\n";
  if (testGetAfterCompaction(database)) {
    cout << "testGetAfterCompaction passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetAfterCompaction failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanAfterCompaction\n";
  if (testScanAfterCompaction(database)) {
    cout << "testScanAfterCompaction passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanAfterCompaction failed.\n";
  }
  total_tests += 1;

  createThirdSST(database);

  // After this runs, LSM compaction should run because there are 2 SSTS at
  // level 1 And After that compaction there are going to be 2 SSTs at level 2
  // so there should Only be 1 SST at level 3
  createFourthSST(database);

  cout << "Running testCompactionFileCreated for level 3\n";
  if (testCompactionFileCreated(dir_path)) {
    cout << "testCompactionFileCreated level 3 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testCompactionFileCreated level 3 failed.\n";
  }
  total_tests += 1;

  cout << "Running testGetAfterCompactionLvl3\n";
  if (testGetAfterCompactionLvl3(database)) {
    cout << "testGetAfterCompactionLvl3 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testGetAfterCompactionLvl3 failed.\n";
  }
  total_tests += 1;

  cout << "Running testScanAfterCompactionLvl3\n";
  if (testScanAfterCompactionLvl3(database)) {
    cout << "testScanAfterCompactionLvl3 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testScanAfterCompactionLvl3 failed.\n";
  }
  total_tests += 1;

  // Now testing the close and open database to see if the memtable is still
  // structured
  database.Close();

  database.Open(dir_path, LSM_TREE);

  // Check to see if the database kepts its contents after close()
  cout << "Running testSSTAfterClose\n";
  if (testSSTAfterClose(database)) {
    cout << "testSSTAfterClose passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testSSTAfterClose failed.\n";
  }
  total_tests += 1;

  createDeletedData(database);

  // all of the keys should have value 0 now (deleted)
  cout << "Running testCompactionFileCreated for level 4\n";
  if (testCompactionFileCreated(dir_path)) {
    cout << "testCompactionFileCreated level 4 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testCompactionFileCreated level 4 failed.\n";
  }
  total_tests += 1;

  cout << "Running testSSTAfterUpdateDelete\n";
  if (testSSTAfterUpdateDelete(database)) {
    cout << "testSSTAfterUpdateDelete passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testSSTAfterUpdateDelete failed.\n";
  }
  total_tests += 1;

  database.Close();

  cout << "\nTotal of " << test_pass_counter << "/" << total_tests
       << " passed in lsm_test.cc\n";
  return test_pass_counter == total_tests;
}