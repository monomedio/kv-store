#include "../src/bufferpool.hh"

#include <fstream>
#include <iostream>

#include "bufferpool_test.hh"
#include "constants.hh"
using namespace std;

bool testInsertAndSearch(Bufferpool &bufferpool,
                         vector<vector<KeyValuePair>> page_array,
                         const string (&pageIds)[5]) {
  for (int i = 0; i < 3; i++) {
    /* want to use # as divder here */
    vector<KeyValuePair> input(PAGE_NUM_ENTRIES);
    input = page_array[i];
    bufferpool.insert(pageIds[i], input);
  }
  /* Included test for collision resolution by linked buckets */
  for (int i = 0; i < 3; i++) {
    vector<KeyValuePair> *result = bufferpool.search(pageIds[i]);
    if (!result) {
      std::cerr << "Expect page but the page does not exist." << std::endl;
      return false;
    }
    vector<KeyValuePair> output = *result;
    for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
      if (output[j].key != i * PAGE_NUM_ENTRIES + j ||
          output[j].value != i * PAGE_NUM_ENTRIES + j) {
        std::cerr << "Bufferpool page value is wrong. Expectd: "
                  << to_string(i * PAGE_NUM_ENTRIES + j) << ", "
                  << to_string(i * PAGE_NUM_ENTRIES + j)
                  << " Actual: " << output[j].key << ", " << output[j].value
                  << std::endl;
        return false;
      }
    }
  }
  return true;
}

bool testEvict(Bufferpool &bufferpool, vector<vector<KeyValuePair>> page_array,
               const string (&pageIds)[5]) {
  for (int i = 3; i < 5; i++) {
    vector<KeyValuePair> input(PAGE_NUM_ENTRIES);
    input = page_array[i];
    bufferpool.insert(pageIds[i], input);
  }
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> *result = bufferpool.search(pageIds[i]);
    // bool result = bufferpool.search(pageIds[i]);
    if (i < 2 && result) {
      std::cerr << "Expect no page but the page exists: " << pageIds[i]
                << std::endl;
      return false;
    } else if (i >= 2) {
      if (!result) {
        std::cerr << "Expect page but the page does not exist." << std::endl;
        return false;
      }
      vector<KeyValuePair> output = *result;
      for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
        if (page_array[i][j].key != output[j].key ||
            page_array[i][j].value != output[j].value) {
          std::cerr << "Bufferpool page value is wrong. Expectd: "
                    << page_array[i][j].key << ", " << page_array[i][j].value
                    << " Actual: " << output[i].key << ", " << output[i].value
                    << std::endl;
          return false;
        }
      }
    }
  }
  return true;
}

bool testEvictWithMultiRead(Bufferpool &bufferpool,
                            vector<vector<KeyValuePair>> page_array,
                            const string (&pageIds)[5]) {
  // KeyValuePair *output;
  bufferpool.search(pageIds[2]);
  bufferpool.search(pageIds[3]);
  vector<KeyValuePair> input(PAGE_NUM_ENTRIES);
  input = page_array[0];
  bufferpool.insert(pageIds[0], input);
  bufferpool.search(pageIds[2]);
  bufferpool.search(pageIds[3]);
  vector<KeyValuePair> input2(PAGE_NUM_ENTRIES);
  input2 = page_array[1];
  bufferpool.insert(pageIds[1], input2);
  for (int i = 0; i < 5; i++) {
    // KeyValuePair *output;
    vector<KeyValuePair> *result = bufferpool.search(pageIds[i]);
    // bool result = bufferpool.search(pageIds[i]);
    if (i == 0 || i == 4) {
      if (result) {
        std::cerr << "Expect no page but the page exists: " << pageIds[i]
                  << std::endl;
        return false;
      }
    } else {
      if (!result) {
        std::cerr << "Expect page but the page does not exist." << std::endl;
        return false;
      }
      vector<KeyValuePair> output = *result;
      for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
        if (page_array[i][j].key != output[j].key ||
            page_array[i][j].value != output[j].value) {
          std::cerr << "Bufferpool page value is wrong. Expectd: "
                    << page_array[i][j].key << ", " << page_array[i][j].value
                    << " Actual: " << output[i].key << ", " << output[i].value
                    << std::endl;
          return false;
        }
      }
    }
  }
  return true;
}

bool testExpand(vector<vector<KeyValuePair>> page_array,
                const string (&pageIds)[5]) {
  Bufferpool bufferpool(3);
  bufferpool.resize(5);
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> input(PAGE_NUM_ENTRIES);
    input = page_array[i];
    bufferpool.insert(pageIds[i], input);
  }

  /* Included test for collision resolution by linked buckets */
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> *result = bufferpool.search(pageIds[i]);
    if (!result) {
      std::cerr << "Expect page but the page does not exist." << std::endl;
      return false;
    }
    vector<KeyValuePair> output = *result;
    for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
      if (page_array[i][j].key != output[j].key ||
          page_array[i][j].value != output[j].value) {
        std::cerr << "Bufferpool page value is wrong. Expectd: "
                  << page_array[i][j].key << ", " << page_array[i][j].value
                  << " Actual: " << output[i].key << ", " << output[i].value
                  << std::endl;
        return false;
      }
    }
  }
  return true;
}

bool testShrink(vector<vector<KeyValuePair>> page_array,
                const string (&pageIds)[5]) {
  Bufferpool bufferpool(5);
  bufferpool.resize(3);
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> input(PAGE_NUM_ENTRIES);
    input = page_array[i];
    bufferpool.insert(pageIds[i], input);
  }
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> *result = bufferpool.search(pageIds[i]);
    if (i < 2) {
      if (result) {
        std::cerr << "Expect no page but the page exists: " << pageIds[i]
                  << std::endl;
        return false;
      }
    } else {
      if (!result) {
        std::cerr << "Expect page but the page does not exist." << std::endl;
        return false;
      }
      vector<KeyValuePair> output = *result;
      for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
        if (page_array[i][j].key != output[j].key ||
            page_array[i][j].value != output[j].value) {
          std::cerr << "Bufferpool page value is wrong. Expectd: "
                    << page_array[i][j].key << ", " << page_array[i][j].value
                    << " Actual: " << output[i].key << ", " << output[i].value
                    << std::endl;
          return false;
        }
      }
    }
  }
  return true;
}

bool testShrink2(vector<vector<KeyValuePair>> page_array,
                 const string (&pageIds)[5]) {
  Bufferpool bufferpool(5);
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> input(PAGE_NUM_ENTRIES);
    input = page_array[i];
    bufferpool.insert(pageIds[i], input);
  }
  bufferpool.resize(3);
  for (int i = 0; i < 5; i++) {
    vector<KeyValuePair> *result = bufferpool.search(pageIds[i]);
    if (i < 2) {
      if (result) {
        std::cerr << "Expect no page but the page exists: " << pageIds[i]
                  << std::endl;
        return false;
      }
    } else {
      if (!result) {
        std::cerr << "Expect page but the page does not exist." << std::endl;
        return false;
      }
      vector<KeyValuePair> output = *result;
      for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
        if (page_array[i][j].key != output[j].key ||
            page_array[i][j].value != output[j].value) {
          std::cerr << "Bufferpool page value is wrong. Expectd: "
                    << page_array[i][j].key << ", " << page_array[i][j].value
                    << " Actual: " << output[i].key << ", " << output[i].value
                    << std::endl;
          return false;
        }
      }
    }
  }
  return true;
}

bool runBufferpoolTests() {
  Bufferpool bufferpool(3);
  string pageIds[5];
  vector<vector<KeyValuePair>> page_array(5);
  for (int i = 0; i < 5; i++) {
    size_t offset = i * PAGE_SIZE;
    pageIds[i] = "SST_0#" + to_string(offset);
    page_array[i].resize(PAGE_NUM_ENTRIES);
    for (int j = 0; j < PAGE_NUM_ENTRIES; j++) {
      page_array[i][j].key = i * PAGE_NUM_ENTRIES + j;
      page_array[i][j].value = i * PAGE_NUM_ENTRIES + j;
    }
  }
  int test_pass_counter = 0;
  int total_tests = 0;

  cout << "Running testInsertAndSearch\n";
  if (testInsertAndSearch(bufferpool, page_array, pageIds)) {
    cout << "testInsertAndSearch passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testInsertAndSearch failed.\n";
  }
  total_tests += 1;

  cout << "Running testEvict\n";
  if (testEvict(bufferpool, page_array, pageIds)) {
    cout << "testEvict passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testEvict failed.\n";
  }
  total_tests += 1;

  cout << "Running testEvictWithMultiRead\n";
  if (testEvictWithMultiRead(bufferpool, page_array, pageIds)) {
    cout << "testEvictWithMultiRead passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testEvictWithMultiRead failed.\n";
  }
  total_tests += 1;

  cout << "Running testExpand\n";
  if (testExpand(page_array, pageIds)) {
    cout << "testExpand passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testExpand failed.\n";
  }
  total_tests += 1;

  cout << "Running testShrink\n";
  if (testShrink(page_array, pageIds)) {
    cout << "testShrink passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testShrink failed.\n";
  }
  total_tests += 1;

  cout << "Running testShrink2\n";
  if (testShrink2(page_array, pageIds)) {
    cout << "testShrink2 passed.\n";
    test_pass_counter += 1;
  } else {
    cout << "testShrink2 failed.\n";
  }
  total_tests += 1;

  cout << "\nTotal of " << test_pass_counter << "/" << total_tests
       << " passed in bufferpool.cc\n";
  return test_pass_counter == total_tests;
}