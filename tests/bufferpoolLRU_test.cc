#include "../src/bufferpoolLRU.hh"

#include <iostream>

#include "bufferpoolLRU_test.hh"
using namespace std;

bool testPutRemoveHead(LRUQueue &queue) {
  string output = queue.remove_head();
  if ("" != output) {
    std::cerr << "Queue should be empty; LRU should return empty string."
              << std::endl;
    return false;
  }
  string buckets[4] = {"a", "b", "c", "d"};
  for (auto bucket : buckets) {
    shared_ptr<LRUNode> node = make_shared<LRUNode>(LRUNode(bucket));
    queue.put(node);
  }
  for (auto bucket : buckets) {
    output = queue.remove_head();
    if (bucket != output) {
      std::cerr << "bucket is wrong. Expectd: " << bucket
                << " Actual: " << output << std::endl;
      return false;
    }
  }
  output = queue.remove_head();
  if ("" != output) {
    std::cerr << "Queue should be empty; LRU should return empty string."
              << std::endl;
    return false;
  }
  return true;
}

bool testMoveToTail(LRUQueue &queue) {
  string buckets[4] = {"a", "b", "c", "d"};
  shared_ptr<LRUNode> node[4];
  for (int i = 0; i < 4; i++) {
    node[i] = make_shared<LRUNode>(buckets[i]);
    queue.put(node[i]);
  }
  queue.move_to_tail(node[0]);
  string output = queue.remove_head();
  if ("b" != output) {
    std::cerr << "bucket is wrong. Expectd: b"
              << " Actual: " << output << std::endl;
    return false;
  }
  queue.move_to_tail(node[2]);
  output = queue.remove_head();
  if ("d" != output) {
    std::cerr << "bucket is wrong. Expectd: d"
              << " Actual: " << output << std::endl;
    return false;
  }
  output = queue.remove_head();
  if ("a" != output) {
    std::cerr << "bucket is wrong. Expectd: a"
              << " Actual: " << output << std::endl;
    return false;
  }
  queue.move_to_tail(node[2]);
  output = queue.remove_head();
  if ("c" != output) {
    std::cerr << "bucket is wrong. Expectd: c"
              << " Actual: " << output << std::endl;
    return false;
  }
  return true;
}

bool testRemoveNode() {
  LRUQueue queue2;
  string buckets[4] = {"a", "b", "c", "d"};
  shared_ptr<LRUNode> node[4];
  for (int i = 0; i < 4; i++) {
    node[i] = make_shared<LRUNode>(buckets[i]);
    queue2.put(node[i]);
  }
  queue2.remove_node(node[1]);
  string output = queue2.remove_head();
  if ("a" != output) {
    std::cerr << "bucket is wrong. Expectd: a"
              << " Actual: " << output << std::endl;
    return false;
  }
  output = queue2.remove_head();
  if ("c" != output) {
    std::cerr << "bucket is wrong. Expectd: c"
              << " Actual: " << output << std::endl;
    return false;
  }
  return true;
}

bool runBufferpoolLRUTests() {
  LRUQueue queue;

  int test_pass_counter = 0;
  int total_tests = 0;

  cout << "Running testPutRemoveHead\n";
  if (testPutRemoveHead(queue)) {
    cout << "testPutRemoveHead passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testPutRemoveHead failed.\n";
  }
  total_tests += 1;

  cout << "Running testMoveToTail\n";
  if (testMoveToTail(queue)) {
    cout << "testMoveToTail passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testMoveToTail failed.\n";
  }
  total_tests += 1;

  cout << "\nTotal of " << test_pass_counter << "/" << total_tests
       << " passed in bufferpoolLRU.cc\n";
  return test_pass_counter == total_tests;
}