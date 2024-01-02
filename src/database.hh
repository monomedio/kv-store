#ifndef DATABASE_HH_
#define DATABASE_HH_

#include <map>
#include <string>
#include <vector>

#include "bufferpool.hh"
#include "memtable.hh"

struct ScanResponse {
  vector<KeyValuePair> result;
  int size;
};

struct BSSTMetadata {
  int64_t entries_offset;
  int64_t filter_offset;
  int64_t seeds_offset;
  int64_t bits_per_entry;
  int64_t num_entries;
  int64_t filter_length;
  int64_t num_seeds;
  int64_t file_size;
};

class Database {
 private:
  Memtable memtable;
  Bufferpool bufferpool;
  std::vector<std::string> ssts;
  std::string database_dir;
  std::string db_type;
  bool bufferpool_enabled = true;
  std::map<int, std::vector<std::string>> lsm_tree;

  void find_page(const int &fd, vector<KeyValuePair> &buffer,
                 const int64_t &offset, const string &file_name);

  /**
   * Construct Bloom filter for the databse.
   */
  BloomFilter construct_bloom_filter(int fd, BSSTMetadata &metadata,
                                     vector<KeyValuePair> &buffer,
                                     const string &file_name);

  /**
   * Populate the filter vector.
   */
  void populate_filter_vector(int fd, vector<int64_t> &filter,
                              BSSTMetadata &metadata,
                              vector<KeyValuePair> &buffer,
                              const string &file_name);

  /**
   * Populate the seeds vector.
   */
  void populate_seeds_vector(int fd, vector<int64_t> &seeds,
                             BSSTMetadata &metadata,
                             vector<KeyValuePair> &buffer,
                             const string &file_name);

 public:
  Database(int memtable_size, size_t bufferpool_capacity,
           int64_t bits_per_entry = 10);

  /**
   * Opens the database and prepares it to run.
   */
  void Open(const std::string &db_name, const std::string &database_type);

  /**
   * Close the database.
   */
  void Close();

  /**
   * Get SSTs from database.
   */
  void get_ssts_from_db(const std::string &db_name);

  /**
   * Retrieves a value associated with targetKey in the database in
   * sorted SST file_name using binary search.
   */
  int64_t binary_search(int64_t targetKey, int file_size, int fd,
                        const string &file_name);

  /**
   * Retrieves all KV-pairs in a key range in key order (key1 < key2) in
   * sorted SST file_name using binary search.
   */
  static std::vector<KeyValuePair> binary_search_scan(int64_t key1,
                                                      int64_t key2,
                                                      int file_size, int fd,
                                                      const string &file_name);

  /**
   * Retrieves all KV-pairs in a key range in key order (key1 < key2) in
   * B-tree SST of file_name.
   */
  std::vector<KeyValuePair> b_tree_scan(int64_t key1, int64_t key2,
                                        int64_t file_size,
                                        int64_t entries_offset, int fd,
                                        const string &file_name);

  /**
   * Get metadata of B-tree SST of file_name.
   */
  BSSTMetadata get_btree_metadata(int fd, vector<KeyValuePair> &buffer,
                                  const string &file_name);

  /**
   * Get file size of file_name.
   */
  static long get_file_size(const std::string &file_name);

  /**
   * Delete the entry with given key in the database.
   */
  void Delete(const int64_t &key);

  /**
   * Retrieves a value associated with a given key in the database.
   */
  int64_t Get(const int64_t &key);

  /**
   * Update or add a new a KV-pairs in the database.
   */
  void Update(const int64_t &key, const int64_t &value);

  /**
   * Retrieves all KV-pairs in a key range in key order (key1 < key2).
   */
  ScanResponse Scan(const int64_t &key1, const int64_t &key2);

  /**
   * Stores a key associated with a value in the database.
   */
  void Put(const int64_t &key, const int64_t &value);

  /**
   * Search BTree SST of file_name.
   */
  int64_t searchBTree(const int64_t &key, int fd, int64_t offset,
                      int64_t entries_offset, vector<KeyValuePair> &buffer,
                      const string &file_name);

  /**
   * Get scan offset of the given SST of file_name.
   */
  int64_t getScanOffset(const int64_t &key, int fd, int64_t offset,
                        int64_t entries_offset, vector<KeyValuePair> &buffer,
                        const string &file_name);

  /**
   * If enabled is true, enable bufferpool. Otherwise, disable bufferpool.
   */
  void set_bufferpool_enabled(bool enabled);

  /**
   * Get SST type of databse.
   */
  string get_db_type();

  /**
   * Merge sort SSTs in sstsToMerge.
   */
  std::string merge_sort_SSTs(const std::vector<std::string> &sstsToMerge);

  /**
   * Check and perform compaction if LSM tree needs compaction.
   */
  void check_LSM_compaction();

  /**
   * Save LSM tree state of given SST from file_name.
   */
  void save_LSM_tree_state(const std::string &file_name);

  /**
   * Load LSM tree state of given SST from file_name.
   */
  void load_LSM_tree_state(const std::string &file_name);
};

#endif  // DATABASE_HH_