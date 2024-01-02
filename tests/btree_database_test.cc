//
// Created by Sam Hui on 2023-11-24.
//
#include "btree_database_test.hh"

#include "database_test.hh"

bool testGetAndPut(Database &database) {
  // Put 65536 into memtable and flush it into an SST
  // Test to see if getting each 65536 keys returns the correct value
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    database.Put(i + 1, i + 1);
  }
  bool error_found = false;
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    if (database.Get(i + 1) != i + 1) {
      error_found = true;
      break;
    }
  }
  return !error_found;
}

bool testGetInvalidKeys(Database &database) {
  bool errorFound = false;
  if (database.Get(0) != -1) {
    errorFound = true;
  }
  if (database.Get(65537) != -1) {
    errorFound = true;
  }
  if (database.Get(-1) != -1) {
    errorFound = true;
  }
  return !errorFound;
}

bool testGetNewAndOldKeys(Database &database) {
  bool error_found = false;
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    database.Put(i + 65537, i + 65537);
  }

  for (int i = 0; i < (PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES) * 2; i++) {
    if (database.Get(i + 1) != i + 1) {
      error_found = true;
      break;
    }
  }
  return !error_found;
}

bool testScan(Database &database) {
  ScanResponse scan_1 = database.Scan(1, 65536);
  if (scan_1.size != 65536) {
    return false;
  }
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    if (scan_1.result[i].value != i + 1) {
      return false;
    }
  }

  ScanResponse scan_2 = database.Scan(65537, 131072);
  if (scan_2.size != 65536) {
    return false;
  }
  for (int i = 0; i < 65536; i++) {
    if (scan_2.result[i].value != i + 65537) {
      return false;
    }
  }

  ScanResponse scan_3 = database.Scan(1, 131072);
  if (scan_3.size != 131072) {
    return false;
  }
  for (int i = 0; i < 131072; i++) {
    if (scan_3.result[i].value != i + 1) {
      return false;
    }
  }

  return true;
}

bool runBTreeDatabaseTests() {
  deleteAllFilesInDirectory("tests/ssts/btree_database_test");

  Database database(PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES, PAGE_SIZE);
  database.Open("tests/ssts/btree_database_test", BSST);

  int tests_passed = 0;
  int num_tests = 0;

  cout << "RUNNING BTREE_DATABASE TESTS\n\n";

  cout << "Running testGetAndPut\n";
  if (testGetAndPut(database)) {
    cout << "testGetAndPut passed.\n";
    tests_passed += 1;
  } else {
    cout << "testGetAndPut failed.\n";
  }
  num_tests += 1;

  cout << "Running testGetInvalidKeys\n";
  if (testGetInvalidKeys(database)) {
    cout << "testGetInvalidKeys passed.\n";
    tests_passed += 1;
  } else {
    cout << "testGetInvalidKeys failed.\n";
  }
  num_tests += 1;

  cout << "Running testGetNewAndOldKeys\n";
  if (testGetNewAndOldKeys(database)) {
    cout << "testGetNewAndOldKeys passed.\n";
    tests_passed += 1;
  } else {
    cout << "testGetNewAndOldKeys failed.\n";
  }
  num_tests += 1;

  cout << "Running testScan\n";
  if (testScan(database)) {
    cout << "testScan passed.\n";
    tests_passed += 1;
  } else {
    cout << "testScan failed.\n";
  }
  num_tests += 1;

  cout << "\nTotal of " << tests_passed << "/" << num_tests
       << " passed in BTREE_DATABASE TESTS\n";
  return tests_passed == num_tests;
}
