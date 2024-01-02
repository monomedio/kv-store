#include "memtable.hh"

#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

#include "btree.hh"

// Sam's include directives
#include <cassert>
#include <queue>

#include "constants.hh"

using namespace std;

Node::Node(const int64_t &key, const int64_t &value)
    : key(key),
      value(value),
      left_subtree(nullptr),
      right_subtree(nullptr),
      height(0) {}

Memtable::Memtable(int memtable_size, int64_t bits_per_entry)
    : root_node(nullptr),
      size(0),
      memtable_size(memtable_size),
      sst_count(0),
      bits_per_entry(bits_per_entry) {}

int Memtable::height(Node *node) {
  if (node == nullptr) {
    return -1;
  } else {
    return node->height;
  }
}

int Memtable::balanceFactor(Node *node) {
  if (node == nullptr) {
    return 0;
  } else {
    return height(node->right_subtree) - height(node->left_subtree);
  }
}

Node *Memtable::leftRotation(Node *old_root) {
  Node *new_root = old_root->right_subtree;
  Node *old_root_right = new_root->left_subtree;
  new_root->left_subtree = old_root;
  old_root->right_subtree = old_root_right;

  old_root->height =
      max(height(old_root->left_subtree), height(old_root->right_subtree)) + 1;
  new_root->height =
      max(height(new_root->left_subtree), height(new_root->right_subtree)) + 1;

  return new_root;
}

Node *Memtable::rightRotation(Node *old_root) {
  Node *new_root = old_root->left_subtree;
  Node *old_root_left = new_root->right_subtree;
  new_root->right_subtree = old_root;
  old_root->left_subtree = old_root_left;

  old_root->height =
      max(height(old_root->left_subtree), height(old_root->right_subtree)) + 1;
  new_root->height =
      max(height(new_root->left_subtree), height(new_root->right_subtree)) + 1;

  return new_root;
}

Node *Memtable::put(Node *node, const int64_t &key, const int64_t &value) {
  if (node == nullptr) {
    size += 1;
    return new Node(key, value);
  }

  if (key < node->key) {
    node->left_subtree = put(node->left_subtree, key, value);
  } else if (key > node->key) {
    node->right_subtree = put(node->right_subtree, key, value);
  } else {
    node->value = value;
    return node;
  }

  node->height =
      1 + max(height(node->left_subtree), height(node->right_subtree));
  int balance = balanceFactor(node);

  // We need to rotate and account for all the single and double rotations:
  // Just Left rotation case
  if (balance > 1 && balanceFactor(node->right_subtree) > -1) {
    return leftRotation(node);
  }
  // Just Right rotation caase
  if (balance < -1 && balanceFactor(node->left_subtree) < 1) {
    return rightRotation(node);
  }
  // Right rotation on left_subtree first, then Left rotation on node
  if (balance > 1 && balanceFactor(node->right_subtree) <= -1) {
    node->right_subtree = rightRotation(node->right_subtree);
    return leftRotation(node);
  }
  // Left rotation on right_subtree first, then Right rotation on node
  if (balance < -1 && balanceFactor(node->left_subtree) >= 1) {
    node->left_subtree = leftRotation(node->left_subtree);
    return rightRotation(node);
  }

  return node;
}

int64_t Memtable::get(Node *node, const int64_t &key) {
  if (node == nullptr) {
    return -1;
  }

  if (key == node->key) {
    return node->value;
  } else if (key > node->key) {
    return get(node->right_subtree, key);
  } else {
    return get(node->left_subtree, key);
  }
}

void Memtable::traverse(Node *node, vector<BTreePair *> &kv_pairs) {
  if (node == nullptr) {
    return;
  }

  traverse(node->left_subtree, kv_pairs);
  BTreePair *key_value_pair = new BTreePair(node->key, node->value);
  kv_pairs.push_back(key_value_pair);
  traverse(node->right_subtree, kv_pairs);
}

vector<KeyValuePair> Memtable::scan(Node *node, const int64_t &key1,
                                    const int64_t &key2) {
  if (node == nullptr) {
    vector<KeyValuePair> emptyVector;
    return emptyVector;
  }

  KeyValuePair value;
  value.key = node->key;
  value.value = node->value;

  vector<KeyValuePair> values = {value};

  if (key1 <= node->key and key2 >= node->key) {
    vector<KeyValuePair> leftValues = scan(node->left_subtree, key1, key2);
    values.insert(values.begin(), leftValues.begin(), leftValues.end());
    vector<KeyValuePair> rightValues = scan(node->right_subtree, key1, key2);
    values.insert(values.end(), rightValues.begin(), rightValues.end());
    return values;
  } else if (key1 > node->key) {
    return scan(node->right_subtree, key1, key2);
  } else if (key2 < node->key) {
    return scan(node->left_subtree, key1, key2);
  } else {
    vector<KeyValuePair> emptyVector;
    return emptyVector;
  }
}

void Memtable::writeToSST(Node *node, std::ofstream &outFile) {
  std::vector<char> buffer;
  writeToSSTBuffered(node, buffer, outFile);

  // Write any remaining data in the buffer
  if (!buffer.empty()) {
    padBuffer(buffer);
    outFile.write(buffer.data(), buffer.size());
  }

  outFile.close();
}

// Function to write buffer to file and reset buffer index
void flushBSSTBuffer(std::ofstream &file, char *write_buffer,
                     int &buffer_index) {
  file.write(write_buffer, buffer_index);
  if (!file) {
    cerr << "Write error occurred.\n";
  }
  buffer_index = 0;
}

void Memtable::writeToBSST(BTreeNode *b_tree, std::ofstream &outFile,
                           BloomFilter &bloom_filter) {
  // root_node is the root of the BTree
  queue<BTreeNode *> flush_queue;

  flush_queue.push(b_tree);
  bool kv_seen = false;
  int64_t kv_offset = 0;
  int64_t total_bytes_written = 0;

  // Current assumption is that metadata takes up exactly one 4KB page at the
  // start Probably will also have to save the offset of the root
  int64_t internal_offset = 2;

  // Buffered write variables
  char write_buffer[PAGE_SIZE];
  int buffer_index = 0;

  // Pad first page with 256 key0 value0 entries to reserve metadata space
  for (int i = 0; i < PAGE_NUM_ENTRIES; i++) {
    int64_t padding = 0;

    // Buffered writes of padding (key 0 value 0)
    memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
    buffer_index += sizeof(padding);
    memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
    buffer_index += sizeof(padding);

    total_bytes_written += ENTRY_SIZE;

    // Check if buffer is full and flush it
    if (buffer_index >= PAGE_SIZE) {
      flushBSSTBuffer(outFile, write_buffer, buffer_index);
    }
  }

  while (!flush_queue.empty()) {
    BTreeNode *node = flush_queue.front();
    flush_queue.pop();
    int bytes_written = 0;

    for (auto child : node->get_entries()) {
      if (child->isBTreeNode()) {
        int64_t child_max_key = dynamic_cast<BTreeNode *>(child)->get_max_key();
        int64_t temp_internal_offset = internal_offset * PAGE_SIZE;

        // Write max_key to the buffer
        memcpy(write_buffer + buffer_index, &child_max_key,
               sizeof(child_max_key));
        buffer_index += sizeof(child_max_key);

        // Write the child (internal) node's page offset in the SST to buffer
        memcpy(write_buffer + buffer_index, &temp_internal_offset,
               sizeof(temp_internal_offset));
        buffer_index += sizeof(temp_internal_offset);

        // Check if buffer is full and flush it
        if (buffer_index >= PAGE_SIZE) {
          flushBSSTBuffer(outFile, write_buffer, buffer_index);
        }

        internal_offset += 1;
        flush_queue.push(dynamic_cast<BTreeNode *>(child));
      } else {
        // Save the byte offset of the SST entries to later add to the first
        // page where metadata is stored
        if (!kv_seen) {
          kv_offset = total_bytes_written;
          kv_seen = true;
        }

        int64_t kv_key = dynamic_cast<BTreePair *>(child)->get_key();
        int64_t kv_value = dynamic_cast<BTreePair *>(child)->get_value();

        // Write entry
        memcpy(write_buffer + buffer_index, &kv_key, sizeof(kv_key));
        buffer_index += sizeof(kv_key);

        memcpy(write_buffer + buffer_index, &kv_value, sizeof(kv_value));
        buffer_index += sizeof(kv_value);

        // Check if buffer is full and flush it
        if (buffer_index >= PAGE_SIZE) {
          flushBSSTBuffer(outFile, write_buffer, buffer_index);
        }
      }
      bytes_written += ENTRY_SIZE;
      total_bytes_written += ENTRY_SIZE;
    }

    // Pad page with zeroes
    while (bytes_written < PAGE_SIZE) {
      int64_t padding = 0;

      // Buffered writes of padding (key 0 value 0)
      memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
      buffer_index += sizeof(padding);
      memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
      buffer_index += sizeof(padding);

      bytes_written += ENTRY_SIZE;
      total_bytes_written += ENTRY_SIZE;

      if (buffer_index >= PAGE_SIZE) {
        flushBSSTBuffer(outFile, write_buffer, buffer_index);
      }
    }
  }

  assert(total_bytes_written % 4096 == 0);
  assert(buffer_index == 0);

  // total_bytes_written should now be the offset at which we write bloom filter
  int64_t filter_offset = total_bytes_written;
  vector<int64_t> filter = bloom_filter.get_filter();

  // Write filter to file
  for (int64_t element : filter) {
    // Buffered writes of filter element
    memcpy(write_buffer + buffer_index, &element, sizeof(element));
    buffer_index += sizeof(element);
    total_bytes_written += 8;

    if (buffer_index >= PAGE_SIZE) {
      flushBSSTBuffer(outFile, write_buffer, buffer_index);
    }
  }

  assert(buffer_index == 0 || (buffer_index > 0 && buffer_index < 4096));

  // Pad with zeroes then flush if the buffer is not empty
  if (buffer_index > 0 && buffer_index < 4096) {
    int64_t padding = 0;
    while (buffer_index < PAGE_SIZE) {
      memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
      buffer_index += sizeof(padding);
      total_bytes_written += 8;
    }
    flushBSSTBuffer(outFile, write_buffer, buffer_index);
  }

  assert(buffer_index == 0);
  assert(total_bytes_written % 4096 == 0);

  int64_t seeds_offset = total_bytes_written;
  vector<int64_t> seeds = bloom_filter.get_seeds();

  for (int64_t seed : seeds) {
    // Buffered writes of seed
    memcpy(write_buffer + buffer_index, &seed, sizeof(seed));
    buffer_index += sizeof(seed);
    total_bytes_written += 8;

    if (buffer_index >= PAGE_SIZE) {
      flushBSSTBuffer(outFile, write_buffer, buffer_index);
    }
  }

  // Pad with zeroes then flush if the buffer is not empty
  if (buffer_index > 0 && buffer_index < 4096) {
    int64_t padding = 0;
    while (buffer_index < PAGE_SIZE) {
      memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
      buffer_index += sizeof(padding);
      total_bytes_written += 8;
    }
    flushBSSTBuffer(outFile, write_buffer, buffer_index);
  }

  assert(buffer_index == 0);
  assert(total_bytes_written % 4096 == 0);

  int64_t file_size = total_bytes_written;
  int64_t bits_per_entry = bloom_filter.get_bits_per_entry();
  int64_t num_entries = bloom_filter.get_num_entries();
  int64_t filter_vector_size = bloom_filter.get_filter_size();
  int64_t num_seeds = bloom_filter.get_num_hash_functions();

  // Writing metadata to first page
  memcpy(write_buffer + buffer_index, &kv_offset, sizeof(kv_offset));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &filter_offset, sizeof(filter_offset));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &seeds_offset, sizeof(seeds_offset));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &bits_per_entry, sizeof(bits_per_entry));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &num_entries, sizeof(num_entries));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &filter_vector_size,
         sizeof(filter_vector_size));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &num_seeds, sizeof(num_seeds));
  buffer_index += 8;
  memcpy(write_buffer + buffer_index, &file_size, sizeof(file_size));
  buffer_index += 8;

  outFile.seekp(0);

  // Pad with zeroes then flush if the buffer is not empty
  if (buffer_index > 0 && buffer_index < 4096) {
    int64_t padding = 0;
    while (buffer_index < PAGE_SIZE) {
      memcpy(write_buffer + buffer_index, &padding, sizeof(padding));
      buffer_index += sizeof(padding);
    }
    flushBSSTBuffer(outFile, write_buffer, buffer_index);
  }

  assert(buffer_index == 0);
  assert(total_bytes_written % 4096 == 0);
}

void Memtable::writeToSSTBuffered(Node *node, std::vector<char> &buffer,
                                  std::ofstream &outFile) {
  if (node) {
    writeToSSTBuffered(node->left_subtree, buffer, outFile);

    int64_t data[2] = {node->key, node->value};
    size_t dataSize = sizeof(data);

    if (buffer.size() + dataSize > PAGE_SIZE) {
      padBuffer(buffer);
      outFile.write(buffer.data(), buffer.size());
      buffer.clear();
    }

    // Add data to buffer
    buffer.insert(buffer.end(), reinterpret_cast<char *>(&data[0]),
                  reinterpret_cast<char *>(&data[0]) + dataSize);

    if (buffer.size() >= PAGE_SIZE) {
      outFile.write(buffer.data(), buffer.size());
      buffer.clear();
    }

    writeToSSTBuffered(node->right_subtree, buffer, outFile);
  }
}

void Memtable::padBuffer(std::vector<char> &buffer) {
  // Pad with zeros
  buffer.resize(PAGE_SIZE, 0);
}

void Memtable::clearMemtable(Node *node) {
  if (node) {
    clearMemtable(node->left_subtree);
    clearMemtable(node->right_subtree);
    delete node;
  }
}

void Memtable::convertMemtableToSST() {
  if (size == 0) {
    return;
  }

  string filename =
      database_name + "/SST_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count()) +
      ".bin";
  ofstream outFile(filename);

  writeToSST(root_node, outFile);
  outFile.close();

  sst_count += 1;

  // Cear the current memtable
  clearMemtable(root_node);
  root_node = nullptr;
  size = 0;
}

BTreeNode *Memtable::constructBTree(BloomFilter &bloom_filter,
                                    vector<BTreePair *> kv_pairs = {}) {
  if (kv_pairs.empty()) {
    traverse(kv_pairs);
  }

  // CREATING LEAF NODES
  queue<BTreeNode *> leaf_nodes;
  auto *curr_leaf = new BTreeNode();

  for (auto kv_pair : kv_pairs) {
    curr_leaf->insert(kv_pair);
    bloom_filter.insert(kv_pair->get_key());
    if (curr_leaf->get_size() >= PAGE_NUM_ENTRIES) {
      leaf_nodes.push(curr_leaf);
      curr_leaf = new BTreeNode();
    }
  }

  if (curr_leaf->get_size() > 0) {
    leaf_nodes.push(curr_leaf);
  }

  auto *btree_root = new BTreeNode();

  if (leaf_nodes.size() == 1) {
    // There are 256 entries or fewer being flushed
    // The leaf itself can be the root node
    btree_root = leaf_nodes.front();
  } else {
    // There are more than 256 entries being flushed
    // Thus we have at least two leaf nodes that should be referenced by a
    // parent
    bool root_ready = false;
    queue<BTreeNode *> internal_nodes;

    while (!root_ready) {
      int num_leaves = leaf_nodes.size();
      auto *curr_node = new BTreeNode();
      int num_internal =
          ceil(static_cast<double>(num_leaves) / PAGE_NUM_ENTRIES);
      int base_distribution = num_leaves / num_internal;
      int num_extras = num_leaves % num_internal;

      while (num_leaves > 0) {
        BTreeNode *leaf = leaf_nodes.front();
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

      if (internal_nodes.size() <= PAGE_NUM_ENTRIES) {
        root_ready = true;
      } else {
        leaf_nodes = internal_nodes;
        std::queue<BTreeNode *> empty;
        std::swap(empty, internal_nodes);
      }
    }

    if (internal_nodes.size() == 1) {
      // There is only one internal node left, make it the root
      btree_root = internal_nodes.front();
    } else {
      // There are less than 256 internal nodes, thus we insert them all into
      // a parent node and return the parent node
      btree_root = new BTreeNode();
      // Insert all nodes in internal_nodes to root_node
      while (!internal_nodes.empty()) {
        btree_root->insert(internal_nodes.front());
        internal_nodes.pop();
      }
    }
  }

  return btree_root;
}

void Memtable::convertMemtableToBSST() {
  if (size == 0) {
    return;
  }

  string filename =
      database_name + "/BSST_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count()) +
      ".bin";
  ofstream outFile(filename, ios::binary);
  if (!outFile) {
    cerr << "Could not open file for writing.\n";
    throw runtime_error("Could not open file for writing.");
  }

  BloomFilter bloom_filter(get_size(), get_bits_per_entry());
  BTreeNode *b_tree = constructBTree(bloom_filter);
  writeToBSST(b_tree, outFile, bloom_filter);

  outFile.close();
  sst_count += 1;

  // Clear b_tree
  EntryBase::clearBTree(b_tree);
  b_tree = nullptr;

  // Clear the current memtable
  clearMemtable(root_node);
  root_node = nullptr;
  size = 0;
}

void Memtable::CreateCompactedBSST(const string &filename,
                                   vector<BTreePair *> &kv_pairs,
                                   int64_t merged_size) {
  ofstream outFile(filename, ios::binary);
  if (!outFile) {
    cerr << "Could not open file for writing.\n";
    throw runtime_error("Could not open file for writing.");
  }

  BloomFilter bloom_filter(merged_size, get_bits_per_entry());
  BTreeNode *b_tree = constructBTree(bloom_filter, kv_pairs);

  writeToBSST(b_tree, outFile, bloom_filter);

  outFile.close();

  // Clear b_tree
  EntryBase::clearBTree(b_tree);
  b_tree = nullptr;
}

bool Memtable::put(const int64_t &key, const int64_t &value) {
  // If memtable is flushed to SST return true, otherwise false
  bool flushedToSST = false;
  root_node = put(root_node, key, value);

  if (size >= memtable_size) {
    if (db_type == BSST || db_type == LSM_TREE) {
      convertMemtableToBSST();
    } else {
      convertMemtableToSST();
    }
    flushedToSST = true;
  }

  return flushedToSST;
}

void Memtable::close() {
  if (db_type == BSST || db_type == LSM_TREE) {
    convertMemtableToBSST();
  } else {
    convertMemtableToSST();
  }
}

int64_t Memtable::get(const int64_t &key) { return get(root_node, key); }

vector<KeyValuePair> Memtable::scan(const int64_t &key1, const int64_t &key2) {
  return scan(root_node, key1, key2);
}

void Memtable::set_db_name(const string &db_name) { database_name = db_name; }

void Memtable::set_sst_count(const int &count) { sst_count = count; }

void Memtable::traverse(std::vector<BTreePair *> &kv_pairs) {
  traverse(root_node, kv_pairs);
}

int Memtable::get_size() const { return size; }

void Memtable::set_db_type(const std::string &database_type) {
  db_type = database_type;
}

int Memtable::get_bits_per_entry() { return bits_per_entry; }
