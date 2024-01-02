#include <iostream>

#include "bloom_filter_test.hh"
#include "btree_database_test.hh"
#include "btree_database_update_delete_test.hh"
#include "btree_test.hh"
#include "bufferpoolLRU_test.hh"
#include "bufferpool_test.hh"
#include "database_test.hh"
#include "database_update_delete_test.hh"
#include "lsm_test.hh"
#include "memtable_test.hh"

int main() {
  bool allTestsPass = true;

  std::cout << "\nRUNNING MEMTABLE TESTS\n\n";
  allTestsPass &= runMemtableTests();

  std::cout << "RUNNING DATABASE TESTS\n\n";
  allTestsPass &= runDatabaseTests();

  std::cout << "RUNNING BUFFERPOOL LRU TESTS\n\n";
  allTestsPass &= runBufferpoolLRUTests();

  std::cout << "RUNNING BUFFERPOOL TESTS\n\n";
  allTestsPass &= runBufferpoolTests();

  std::cout << "RUNNING BTREE TESTS\n\n";
  allTestsPass &= runBTreeTests();

  std::cout << "RUNNING BTREE DATABASE TESTS\n\n";
  allTestsPass &= runBTreeDatabaseTests();

  std::cout << "RUNNING DATABASE UPDATE/DELETE TESTS\n\n";
  allTestsPass &= runDatabaseUpdateDeleteTests();

  std::cout << "RUNNING BTREE DATABASE UPDATE/DELETE TESTS\n\n";
  allTestsPass &= runBTreeDatabaseUpdateDeleteTests();

  std::cout << "RUNNING BLOOM FILTER TESTS\n\n";
  allTestsPass &= runBloomFilterTests();

  std::cout << "RUNNING LSM_TREE TESTS\n\n";
  allTestsPass &= runLSMTests();

  if (allTestsPass) {
    std::cout << "\nAll tests passed.\n";
    return 0;
  } else {
    std::cout << "\nSome tests failed.\n";
    return 1;
  }
}