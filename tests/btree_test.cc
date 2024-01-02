//
// Created by Sam Hui on 2023-11-20.
//

#include "../src/btree.hh"

#include <cmath>
#include <queue>

#include "../src/memtable.hh"
#include "btree_test.hh"

bool testRootLeaf(int num_entries) {
  Memtable memtable(num_entries + 1);

  for (int64_t i = 1; i < num_entries + 1; i++) {
    memtable.put(i, i);
  }

  std::vector<BTreePair*> kv_pairs;
  memtable.traverse(kv_pairs);

  queue<BTreeNode*> leaf_nodes;
  BTreeNode* curr_leaf = new BTreeNode();

  for (int64_t i = 0; i < num_entries; i++) {
    curr_leaf->insert(kv_pairs[i]);
    if (curr_leaf->get_size() >= 256) {
      leaf_nodes.push(curr_leaf);
      curr_leaf = new BTreeNode();
    }
  }

  if (curr_leaf->get_size() > 0) {
    leaf_nodes.push(curr_leaf);
  }

  BTreeNode* root_node = nullptr;

  if (leaf_nodes.size() == 1) {
    // There are 256 entries or fewer being flushed
    // The leaf itself can be the root node
    root_node = leaf_nodes.front();
  } else {
    // There are more than 256 entries being flushed
    // Thus we have at least two leaf nodes that should be referenced by a
    // parent
    bool root_ready = false;
    queue<BTreeNode*> internal_nodes;

    while (!root_ready) {
      int num_leaves = leaf_nodes.size();
      BTreeNode* curr_node = new BTreeNode();
      int num_internal = std::ceil(static_cast<double>(num_leaves) / 256);
      int base_distribution = num_leaves / num_internal;
      int num_extras = num_leaves % num_internal;

      while (num_leaves > 0) {
        BTreeNode* leaf = leaf_nodes.front();
        leaf_nodes.pop();
        curr_node->insert(leaf);
        num_leaves--;
        if (num_extras > 0 &&
            curr_node->get_size() == (base_distribution + 1)) {
          num_extras--;
          internal_nodes.push(curr_node);
          curr_node = new BTreeNode();
        } else if (num_extras == 0 &&
                   curr_node->get_size() == base_distribution) {
          internal_nodes.push(curr_node);
          curr_node = new BTreeNode();
        }
      }

      if (internal_nodes.size() <= 256) {
        root_ready = true;
      } else {
        leaf_nodes = internal_nodes;
        std::queue<BTreeNode*> empty;
        std::swap(empty, internal_nodes);
      }
    }

    if (internal_nodes.size() == 1) {
      // There is only one internal node left, make it the root
      root_node = internal_nodes.front();
    } else {
      // There are less than 256 internal nodes, thus we insert them all into
      // a parent node and return the parent node
      root_node = new BTreeNode();
      // Insert all nodes in internal_nodes to root_node
      while (!internal_nodes.empty()) {
        root_node->insert(internal_nodes.front());
        internal_nodes.pop();
      }
    }
  }
  bool result = (root_node->get_size() == 256) &&
                (!root_node->get_entries()[0]->isBTreeNode());
  EntryBase::clearBTree(root_node);
  root_node = nullptr;

  return result;
}

bool testHeightOne(int num_entries) {
  Memtable memtable(num_entries + 1);

  for (int64_t i = 1; i < num_entries + 1; i++) {
    memtable.put(i, i);
  }

  std::vector<BTreePair*> kv_pairs;
  memtable.traverse(kv_pairs);

  queue<BTreeNode*> leaf_nodes;
  BTreeNode* curr_leaf = new BTreeNode();

  for (int64_t i = 0; i < num_entries; i++) {
    curr_leaf->insert(kv_pairs[i]);
    if (curr_leaf->get_size() >= 256) {
      leaf_nodes.push(curr_leaf);
      curr_leaf = new BTreeNode();
    }
  }

  if (curr_leaf->get_size() > 0) {
    leaf_nodes.push(curr_leaf);
  }

  BTreeNode* root_node = nullptr;

  if (leaf_nodes.size() == 1) {
    // There are 256 entries or fewer being flushed
    // The leaf itself can be the root node
    root_node = leaf_nodes.front();
  } else {
    // There are more than 256 entries being flushed
    // Thus we have at least two leaf nodes that should be referenced by a
    // parent
    bool root_ready = false;
    queue<BTreeNode*> internal_nodes;

    while (!root_ready) {
      int num_leaves = leaf_nodes.size();
      BTreeNode* curr_node = new BTreeNode();
      int num_internal = std::ceil(static_cast<double>(num_leaves) / 256);
      int base_distribution = num_leaves / num_internal;
      int num_extras = num_leaves % num_internal;

      while (num_leaves > 0) {
        BTreeNode* leaf = leaf_nodes.front();
        leaf_nodes.pop();
        curr_node->insert(leaf);
        num_leaves--;
        if (num_extras > 0 &&
            curr_node->get_size() == (base_distribution + 1)) {
          num_extras--;
          internal_nodes.push(curr_node);
          curr_node = new BTreeNode();
        } else if (num_extras == 0 &&
                   curr_node->get_size() == base_distribution) {
          internal_nodes.push(curr_node);
          curr_node = new BTreeNode();
        }
      }

      if (internal_nodes.size() <= 256) {
        root_ready = true;
      } else {
        leaf_nodes = internal_nodes;
        std::queue<BTreeNode*> empty;
        std::swap(empty, internal_nodes);
      }
    }

    if (internal_nodes.size() == 1) {
      // There is only one internal node left, make it the root
      root_node = internal_nodes.front();
    } else {
      // There are less than 256 internal nodes, thus we insert them all into
      // a parent node and return the parent node
      root_node = new BTreeNode();
      // Insert all nodes in internal_nodes to root_node
      while (!internal_nodes.empty()) {
        root_node->insert(internal_nodes.front());
        internal_nodes.pop();
      }
    }
  }
  bool result =
      (root_node->get_size() == 256) && root_node->isBTreeNode() &&
      root_node->get_entries()[0]->isBTreeNode() &&
      root_node->get_entries()[127]->isBTreeNode() &&
      root_node->get_entries()[255]->isBTreeNode() &&
      static_cast<BTreeNode*>(root_node->get_entries()[0])->get_max_key() ==
          256 &&
      static_cast<BTreeNode*>(root_node->get_entries()[127])->get_max_key() ==
          32768 &&
      static_cast<BTreeNode*>(root_node->get_entries()[255])->get_max_key() ==
          65536;
  EntryBase::clearBTree(root_node);
  root_node = nullptr;

  return result;
}

bool testThreeInternalNodes(int num_entries) {
  Memtable memtable(num_entries + 1);

  for (int64_t i = 1; i < num_entries + 1; i++) {
    memtable.put(i, i);
  }

  std::vector<BTreePair*> kv_pairs;
  memtable.traverse(kv_pairs);

  queue<BTreeNode*> leaf_nodes;
  BTreeNode* curr_leaf = new BTreeNode();

  for (int64_t i = 0; i < num_entries; i++) {
    curr_leaf->insert(kv_pairs[i]);
    if (curr_leaf->get_size() >= 256) {
      leaf_nodes.push(curr_leaf);
      curr_leaf = new BTreeNode();
    }
  }

  if (curr_leaf->get_size() > 0) {
    leaf_nodes.push(curr_leaf);
  }

  BTreeNode* root_node = nullptr;

  if (leaf_nodes.size() == 1) {
    // There are 256 entries or fewer being flushed
    // The leaf itself can be the root node
    root_node = leaf_nodes.front();
  } else {
    // There are more than 256 entries being flushed
    // Thus we have at least two leaf nodes that should be referenced by a
    // parent
    bool root_ready = false;
    queue<BTreeNode*> internal_nodes;

    while (!root_ready) {
      int num_leaves = leaf_nodes.size();
      BTreeNode* curr_node = new BTreeNode();
      int num_internal = std::ceil(static_cast<double>(num_leaves) / 256);
      int base_distribution = num_leaves / num_internal;
      int num_extras = num_leaves % num_internal;

      while (num_leaves > 0) {
        BTreeNode* leaf = leaf_nodes.front();
        leaf_nodes.pop();
        curr_node->insert(leaf);
        num_leaves--;
        if (num_extras > 0 &&
            curr_node->get_size() == (base_distribution + 1)) {
          num_extras--;
          internal_nodes.push(curr_node);
          curr_node = new BTreeNode();
        } else if (num_extras == 0 &&
                   curr_node->get_size() == base_distribution) {
          internal_nodes.push(curr_node);
          curr_node = new BTreeNode();
        }
      }

      if (internal_nodes.size() <= 256) {
        root_ready = true;
      } else {
        leaf_nodes = internal_nodes;
        std::queue<BTreeNode*> empty;
        std::swap(empty, internal_nodes);
      }
    }

    if (internal_nodes.size() == 1) {
      // There is only one internal node left, make it the root
      root_node = internal_nodes.front();
    } else {
      // There are less than 256 internal nodes, thus we insert them all into
      // a parent node and return the parent node
      root_node = new BTreeNode();
      // Insert all nodes in internal_nodes to root_node
      while (!internal_nodes.empty()) {
        root_node->insert(internal_nodes.front());
        internal_nodes.pop();
      }
    }
  }

  bool result = (root_node->get_size() == 3) &&
                (root_node->get_entries()[0]->isBTreeNode()) &&
                (root_node->get_entries()[1]->isBTreeNode()) &&
                (root_node->get_entries()[2]->isBTreeNode()) &&
                (static_cast<BTreeNode*>(
                     static_cast<BTreeNode*>(root_node->get_entries()[0])
                         ->get_entries()[255])
                     ->get_max_key() == 65536) &&
                (static_cast<BTreeNode*>(
                     static_cast<BTreeNode*>(root_node->get_entries()[1])
                         ->get_entries()[255])
                     ->get_max_key() == 131072) &&
                (static_cast<BTreeNode*>(
                     static_cast<BTreeNode*>(root_node->get_entries()[2])
                         ->get_entries()[255])
                     ->get_max_key() == 196608) &&
                !(static_cast<BTreeNode*>(
                      static_cast<BTreeNode*>(root_node->get_entries()[0])
                          ->get_entries()[255])
                      ->get_entries()[0]
                      ->isBTreeNode()) &&
                !(static_cast<BTreeNode*>(
                      static_cast<BTreeNode*>(root_node->get_entries()[1])
                          ->get_entries()[255])
                      ->get_entries()[0]
                      ->isBTreeNode()) &&
                !(static_cast<BTreeNode*>(
                      static_cast<BTreeNode*>(root_node->get_entries()[2])
                          ->get_entries()[255])
                      ->get_entries()[0]
                      ->isBTreeNode());

  EntryBase::clearBTree(root_node);
  root_node = nullptr;

  return result;
}

bool runBTreeTests() {
  int tests_passed = 0;
  int num_tests = 0;

  cout << "Running testRootLeaf\n";
  if (testRootLeaf(256)) {
    cout << "testRootLeaf passed.\n";
    tests_passed += 1;
  } else {
    cout << "testRootLeaf failed\n";
  }
  num_tests += 1;

  cout << "Running testHeightOne\n";
  if (testHeightOne(65536)) {
    cout << "testHeightOne passed.\n";
    tests_passed += 1;
  } else {
    cout << "testHeightOne failed\n";
  }
  num_tests += 1;

  cout << "Running testThreeInternalNodes\n";
  if (testThreeInternalNodes(196608)) {
    cout << "testThreeInternalNodes passed.\n";
    tests_passed += 1;
  } else {
    cout << "testThreeInternalNodes failed\n";
  }
  num_tests += 1;

  cout << "\nTotal of " << tests_passed << "/" << num_tests
       << " passed in btree_test.cc\n";

  return tests_passed == num_tests;
}