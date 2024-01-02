#pragma once

#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "bloom-filter.hh"
#include "btree.hh"

using namespace std;

struct KeyValuePair {
  int64_t key;
  int64_t value;

  bool operator<(const KeyValuePair &other) const { return key < other.key; }
};

class Node {
 public:
  int64_t key;
  int64_t value;
  Node *left_subtree;
  Node *right_subtree;
  int height;

  Node(const int64_t &key, const int64_t &value);
};

class Memtable {
 private:
  Node *root_node;
  int size;
  int memtable_size;
  int sst_count;
  string database_name;
  string db_type;
  int64_t bits_per_entry;

  /**
   * Returns the height of the given node in an AVL tree.
   */
  int height(Node *node);

  /**
   * Calculates and returns the balance factor of the given node in an AVL tree.
   */
  int balanceFactor(Node *node);

  /**
   * Performs a left rotation on the given node (old_root) and returns the new
   * root of the subtree.
   */
  Node *leftRotation(Node *old_root);

  /**
   * Performs a right rotation on the given node (old_root) and returns the new
   * root of the subtree.
   */
  Node *rightRotation(Node *old_root);

  /**
   * Inserts a key-value pair into the AVL tree and returns the new root of the
   * tree.
   */
  Node *put(Node *node, const int64_t &key, const int64_t &value);

  /**
   * Retrieves the value associated with the given key in the AVL tree.
   */
  int64_t get(Node *node, const int64_t &key);

  /**
   * Returns a vector of key-value pairs in the specified key range [key1, key2]
   * in the AVL tree.
   */
  std::vector<KeyValuePair> scan(Node *node, const int64_t &key1,
                                 const int64_t &key2);

  /**
   * Writes the AVL tree to an SST file, using a buffer for optimization.
   */
  void writeToSST(Node *node, std::ofstream &outFile);

  /**
   * Writes the B-tree to a BSST file, including Bloom filter data and metadata.
   */
  static void writeToBSST(BTreeNode *b_tree, std::ofstream &outFile,
                          BloomFilter &bloom_filter);

  /**
   * Writes the AVL tree to an SST file using a buffer to manage data.
   */
  void writeToSSTBuffered(Node *node, std::vector<char> &buffer,
                          std::ofstream &outFile);

  /**
   * Pads the buffer with zeros to a specified page size.
   */
  void padBuffer(std::vector<char> &buffer);

  /**
   * Recursively deletes nodes in the AVL tree, clearing the Memtable.
   */
  void clearMemtable(Node *node);

  /**
   * Converts the AVL tree to an SST file and resets the Memtable.
   */
  void convertMemtableToSST();

  /**
   * Converts the AVL tree to a BSST file, applying a Bloom filter.
   */
  void convertMemtableToBSST();

  /**
   * Constructs a B-tree from key-value pairs, updating the Bloom filter.
   */
  BTreeNode *constructBTree(BloomFilter &bloom_filter,
                            std::vector<BTreePair *> kv_pairs);

  /**
   * Performs an in-order traversal of the AVL tree, collecting key-value pairs.
   */
  void traverse(Node *node, vector<BTreePair *> &kv_pairs);

 public:
  Memtable(int memtable_size, int64_t bits_per_entry = 10);

  /**
   * Inserts a key-value pair into the Memtable, potentially triggering
   * conversion to SST/BSST.
   */
  bool put(const int64_t &key, const int64_t &value);

  /**
   * Retrieves the value associated with a specified key from the AVL tree.
   */
  int64_t get(const int64_t &key);

  /**
   * Scans the AVL tree for key-value pairs within a given key range.
   */
  std::vector<KeyValuePair> scan(const int64_t &key1, const int64_t &key2);

  /**
   * Sets the name of the database.
   */
  void set_db_name(const std::string &db_name);

  /**
   * Sets the count of SST files.
   */
  void set_sst_count(const int &count);

  /**
   * Traverses the AVL tree to collect key-value pairs.
   */
  void traverse(std::vector<BTreePair *> &kv_pairs);

  /**
   * Returns the size of the Memtable.
   */
  int get_size() const;

  /**
   * Sets the type of the database.
   */
  void set_db_type(const std::string &database_type);

  /**
   * Closes the Memtable, performing necessary cleanup.
   */
  void close();

  /**
   * Returns the number of bits per entry used in the Memtable.
   */
  int get_bits_per_entry();

  /**
   * Creates a compacted BSST file from a set of B-tree pairs.
   */
  void CreateCompactedBSST(const std::string &filename,
                           std::vector<BTreePair *> &kv_pairs, int64_t size);
};
