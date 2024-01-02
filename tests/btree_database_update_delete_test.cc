//
// Created by Yuchen Zeng on 2023-12-02.
//
#include "btree_database_test.hh"
#include "database_test.hh"

bool testGetAfterUpdateAndDelete(Database& database) {
  // Put 65536 into memtable and flush it into an SST
  // Test to see if getting each 65536 keys returns the correct value

  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    database.Put(i + 1, 10);
    if (database.Get(i + 1) != 10) {
      return false;
    }
  }
  /* Test again after flush */
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    if (database.Get(i + 1) != 10) {
      return false;
    }
  }
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    database.Delete(i + 1);
    if (database.Get(i + 1) != -1) {
      return false;
    }
  }
  /* Test again after flush */
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    if (database.Get(i + 1) != -1) {
      return false;
    }
  }
  return true;
}

bool testScanAfterUpdateAndDelete(Database& database) {
  ScanResponse scan_1 = database.Scan(1, 65536);
  /* All entries are deleted */
  if (scan_1.size != 0) {
    cerr << "scan_1.size wrong. Expected: " << 65280
         << " Actual: " << scan_1.size << endl;
    return false;
  }
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    database.Put(i + 1, i + 1);
  }
  for (int i = 0; i < PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES; i++) {
    if (i < PAGE_NUM_ENTRIES) {
      database.Put(i + 1, 0);
    } else {
      database.Put(i + 1, i + 2);
    }
  }
  ScanResponse scan_2 = database.Scan(1, 65536);
  if (scan_2.size != 65280) {
    cerr << "scan_2.size wrong. Expected: " << 65280
         << " Actual: " << scan_2.size << endl;
    return false;
  }
  vector<KeyValuePair> values = scan_2.result;
  for (int i = 0; i < 65280; i++) {
    int64_t value = i + PAGE_NUM_ENTRIES + 2;
    if (values[i].value != value) {
      cerr << "Value wrong. Expected: " << value << " Actual: " << values[i].key
           << " " << values[i].value << endl;
      return false;
    }
  }
  return true;
}

bool runBTreeDatabaseUpdateDeleteTests() {
  deleteAllFilesInDirectory("tests/ssts/btree_database_test");

  Database database(PAGE_NUM_ENTRIES * PAGE_NUM_ENTRIES, PAGE_SIZE);
  database.Open("tests/ssts/btree_database_test", BSST);

  int tests_passed = 0;
  int num_tests = 0;

  cout << "Running testGetAfterUpdateAndDelete\n";
  if (testGetAfterUpdateAndDelete(database)) {
    cout << "testGetAfterUpdateAndDelete passed.\n";
    tests_passed += 1;
  } else {
    cout << "testGetAfterUpdateAndDelete failed.\n";
  }
  num_tests += 1;

  cout << "Running testScanAfterUpdateAndDelete\n";
  if (testScanAfterUpdateAndDelete(database)) {
    cout << "testScanAfterUpdateAndDelete passed.\n";
    tests_passed += 1;
  } else {
    cout << "testScanAfterUpdateAndDelete failed.\n";
  }
  num_tests += 1;

  cout << "\nTotal of " << tests_passed << "/" << num_tests
       << " passed in BTREE DATABASE UPDATE/DELETE TESTS\n";
  return tests_passed == num_tests;
}
