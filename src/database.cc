#include "database.hh"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "btree.hh"
#include "constants.hh"
#include "memtable.hh"

using namespace std;

Database::Database(int memtable_size, size_t bufferpool_capacity,
                   int64_t bits_per_entry)
    : memtable(memtable_size, bits_per_entry),
      bufferpool(bufferpool_capacity),
      database_dir("") {}

void Database::Open(const string &db_name, const string &database_type) {
  database_dir = db_name;
  memtable.set_db_name(db_name);
  db_type = database_type;
  memtable.set_db_type(database_type);

  // Check if directory exists.
  struct stat info;
  if (stat(database_dir.c_str(), &info) != 0) {
    // We need to create the directory
    mkdir(database_dir.c_str(), 0777);
  } else {
    if (db_type == LSM_TREE) {
      string path_to_lsm_data = database_dir + "/lsm_tree_state.txt";
      load_LSM_tree_state(path_to_lsm_data);

      // Remove the LSM tree state file after loading its contents
      if (remove(path_to_lsm_data.c_str()) != 0) {
        perror("Error deleting LSM tree state file");
      }
    }
    // Load SSTs into database
    get_ssts_from_db(db_name);
  }
}

void Database::get_ssts_from_db(const string &db_name) {
  const string directory = "./" + db_name;
  vector<string> sst_files;

  DIR *dir = opendir(directory.c_str());
  if (dir == nullptr) {
    cout << "Database/Directory does not exist.";
    return;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (entry->d_type == DT_REG &&
        string(entry->d_name).find(".bin") != string::npos) {
      sst_files.push_back(entry->d_name);
    }
  }

  closedir(dir);

  sort(sst_files.begin(), sst_files.end());
  memtable.set_sst_count((int)sst_files.size());
  ssts = sst_files;
}

int64_t Database::binary_search(int64_t target_key, int fileSize, int fd,
                                const string &file_name) {
  int left = 0;
  int right = fileSize - PAGE_SIZE;

  vector<KeyValuePair> pairs(PAGE_NUM_ENTRIES);
  while (left <= right) {
    int mid = left + (right - left) / 2;
    mid -= mid % PAGE_SIZE;  // Align mid to PAGE_SIZE.

    find_page(fd, pairs, mid, file_name);

    int64_t last_key_read = numeric_limits<int64_t>::min();
    int64_t return_value = -1;

    for (int i = 0; i < PAGE_NUM_ENTRIES; ++i) {
      if (pairs[i].key == 0) break;  // Assuming 0 is padding

      last_key_read = pairs[i].key;
      if (pairs[i].key > target_key) break;

      if (pairs[i].key == target_key) {
        return_value = pairs[i].value;
        break;
      }
    }

    if (return_value != -1) {
      return return_value;
    }

    if (target_key > last_key_read) {
      left = mid + PAGE_SIZE;
    } else {
      right = mid - 1;
    }
  }
  return -1;  // Key not found
}

long Database::get_file_size(const string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

int find_effective_size(vector<KeyValuePair> &buffer) {
  int effective_size = PAGE_NUM_ENTRIES;
  while (effective_size > 0 && buffer[effective_size - 1].key == 0) {
    --effective_size;
  }
  return effective_size;
}

int64_t Database::searchBTree(const int64_t &key, int fd, int64_t offset,
                              int64_t entries_offset,
                              vector<KeyValuePair> &buffer,
                              const string &file_name) {
  if (key == 0) {
    return -1;
  }

  while (true) {
    find_page(fd, buffer, offset, file_name);
    int buffer_effective_size = find_effective_size(buffer);

    if (offset >= entries_offset) {
      // Leaf node
      int left = 0;
      int right = buffer_effective_size - 1;

      while (left <= right) {
        int mid = left + (right - left) / 2;

        if (buffer[mid].key == key) {
          return buffer[mid].value;  // Key found
        } else if (buffer[mid].key < key) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }

      return -1;  // Key not found
    } else {
      // Internal node
      int left = 0;
      int right = buffer_effective_size - 1;
      int result_index = -1;

      while (left <= right) {
        int mid = left + (right - left) / 2;

        if (buffer[mid].key >= key) {
          result_index = mid;
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      }

      if (result_index != -1) {
        offset =
            buffer[result_index].value;  // Update offset for next iteration
      } else {
        return -1;  // Key not found
      }
    }
  }
}

BSSTMetadata Database::get_btree_metadata(int fd, vector<KeyValuePair> &buffer,
                                          const string &file_name) {
  // Read page at offset 0
  BSSTMetadata metadata{};
  find_page(fd, buffer, 0, file_name);
  metadata.entries_offset = buffer[0].key;
  metadata.filter_offset = buffer[0].value;
  metadata.seeds_offset = buffer[1].key;
  metadata.bits_per_entry = buffer[1].value;
  metadata.num_entries = buffer[2].key;
  metadata.filter_length = buffer[2].value;
  metadata.num_seeds = buffer[3].key;
  metadata.file_size = buffer[3].value;

  return metadata;
}

void Database::populate_filter_vector(int fd, vector<int64_t> &filter,
                                      BSSTMetadata &metadata,
                                      vector<KeyValuePair> &buffer,
                                      const string &file_name) {
  int64_t filter_offset = metadata.filter_offset;
  int64_t seeds_offset = metadata.seeds_offset;

  int filter_size = 0;

  while (filter_offset < seeds_offset) {
    find_page(fd, buffer, filter_offset, file_name);
    for (auto entry : buffer) {
      if (entry.key == 0) {
        if (filter_size >= metadata.filter_length) {
          break;
        }
      }
      filter.push_back(entry.key);
      filter_size += 1;

      if (entry.value == 0) {
        if (filter_size >= metadata.filter_length) {
          break;
        }
      }
      filter.push_back(entry.value);
      filter_size += 1;
    }
    filter_offset += PAGE_SIZE;
  }
  assert(filter_size == metadata.filter_length);
}

void Database::populate_seeds_vector(int fd, vector<int64_t> &seeds,
                                     BSSTMetadata &metadata,
                                     vector<KeyValuePair> &buffer,
                                     const string &file_name) {
  int64_t seeds_offset = metadata.seeds_offset;
  int64_t file_size = metadata.file_size;

  int seeds_size = 0;

  while (seeds_offset < file_size) {
    find_page(fd, buffer, seeds_offset, file_name);
    for (auto entry : buffer) {
      if (entry.key == 0) {
        if (seeds_size >= metadata.num_seeds) {
          break;
        }
      }
      seeds.push_back(entry.key);
      seeds_size += 1;

      if (entry.value == 0) {
        if (seeds_size >= metadata.num_seeds) {
          break;
        }
      }
      seeds.push_back(entry.value);
      seeds_size += 1;
    }
    seeds_offset += PAGE_SIZE;
  }
  assert(seeds_size == metadata.num_seeds);
}

BloomFilter Database::construct_bloom_filter(int fd, BSSTMetadata &metadata,
                                             vector<KeyValuePair> &buffer,
                                             const string &file_name) {
  vector<int64_t> filter;
  vector<int64_t> seeds;

  populate_filter_vector(fd, filter, metadata, buffer, file_name);
  populate_seeds_vector(fd, seeds, metadata, buffer, file_name);

  return {filter, metadata.num_entries, metadata.bits_per_entry, seeds};
}

int64_t Database::Get(const int64_t &key) {
  if (key < 1) {
    return -1;
  }
  // Try getting from memtable first.
  int64_t value = memtable.get(key);
  if (value != -1) {
    /* If value == 0, the entrie has been deleted(tombstone) */
    if (value == 0) return -1;
    return value;
  }

  // If not in memtable, search SSTs.
  for (size_t i = ssts.size(); i-- > 0;) {
    const auto &sst = ssts[i];

    string path_to_file = database_dir + "/" + sst;
    string file_name = sst.substr(0, sst.size() - 4);
    
    int fd = open(path_to_file.c_str(), O_RDONLY | O_DIRECT);
    if (fd == -1) {
      perror("Failed to open the file");
      exit(EXIT_FAILURE);
    }

    int64_t value;
    if (db_type == BSST || db_type == LSM_TREE) {
      vector<KeyValuePair> buffer(PAGE_NUM_ENTRIES);
      BSSTMetadata metadata = get_btree_metadata(fd, buffer, file_name);
      int64_t entries_offset = metadata.entries_offset;

      if (db_type == BSST) {
        value =
            searchBTree(key, fd, PAGE_SIZE, entries_offset, buffer, file_name);
      } else {
        BloomFilter filter =
            construct_bloom_filter(fd, metadata, buffer, file_name);
        if (filter.includes(key)) {
          // Metadata is only one page, so the root is after that at offset 4096
          value = searchBTree(key, fd, PAGE_SIZE, entries_offset, buffer,
                              file_name);
        } else {
          // Filter doesn't think key is in the SST
          value = -1;
        }
      }
    } else {
      int file_size = (int)get_file_size(path_to_file);
      value = binary_search(key, file_size, fd, file_name);
    }

    if (close(fd) == -1) {
      perror("Error closing file in Database::Get\n");
      exit(EXIT_FAILURE);
    }

    if (value != -1) {
      /* If value == 0, the entrie has been deleted(tombstone) */
      if (value == 0) return -1;
      return value;
    }
  }
  return -1;
}

void Database::Delete(const int64_t &key) {
  /* We do not call get to double-check if entries exist here */
  Put(key, 0);
}

void Database::Update(const int64_t &key, const int64_t &value) {
  Put(key, value);
}

struct CompareByKey {
  bool operator()(const KeyValuePair &a, const KeyValuePair &b) const {
    return a.key < b.key;
  }
};

int64_t Database::getScanOffset(const int64_t &key, int fd, int64_t offset,
                                int64_t entries_offset,
                                vector<KeyValuePair> &buffer,
                                const string &file_name) {
  if (key == 0) {
    return -1;
  }

  while (true) {
    if (offset >= entries_offset) {
      // If the offset we are at is beyond entries_offset, then offset is at a
      // leaf node where entries are. Return this offset to begin scanning at
      // this page
      return offset;
    } else {
      // read internal node, meaning its children are other BTreeNodes
      find_page(fd, buffer, offset, file_name);
      int buffer_effective_size = find_effective_size(buffer);

      // Internal node
      int left = 0;
      int right = buffer_effective_size - 1;
      int result_index = -1;

      while (left <= right) {
        int mid = left + (right - left) / 2;

        if (buffer[mid].key >= key) {
          result_index = mid;
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      }

      if (result_index != -1) {
        int64_t node_offset = buffer[result_index].value;
        // Update offset for the next iteration of the loop.
        offset = node_offset;
      } else {
        return -1;
      }
    }
  }
}

void Database::find_page(const int &fd, vector<KeyValuePair> &buffer,
                         const int64_t &offset, const string &file_name) {
  string pageId = file_name + "#" + to_string(offset);
  /* Search in bufferpool if it is enabled */
  if (bufferpool_enabled) {
    vector<KeyValuePair> *result = bufferpool.search(pageId);
    if (result) {
      buffer = *result;
      return;
    }
  }
  /* If bufferpool disabled or page is not in bufferpool */
  ssize_t bytes_read = pread(fd, buffer.data(), PAGE_SIZE, offset);
  if (bytes_read <= 0) {
    exit(EXIT_FAILURE);
  }
  if (bufferpool_enabled) {
    bufferpool.insert(pageId, buffer);
  }
}

vector<KeyValuePair> Database::b_tree_scan(int64_t key1, int64_t key2,
                                           int64_t fileSize,
                                           int64_t entries_offset, int fd,
                                           const string &file_name) {
  // Buffer to read in entry pages
  vector<KeyValuePair> read_buffer(PAGE_NUM_ENTRIES);
  // Return vector
  vector<KeyValuePair> entries_in_range;
  // Get page offset of the page where key1 may reside in
  int64_t scan_offset = getScanOffset(key1, fd, PAGE_SIZE, entries_offset,
                                      read_buffer, file_name);
  // -1 means either key1 is zero (disallowed due to it being reserved for
  // padding) or key1 is larger than any key in the SST
  if (scan_offset == -1) {
    vector<KeyValuePair> empty;
    return empty;
  }
  // Start scanning pages until end of file
  while (scan_offset < fileSize) {
    // Read in page at scan_offset
    ssize_t bytes_read = pread(fd, read_buffer.data(), PAGE_SIZE, scan_offset);
    if (bytes_read <= 0) {
      perror("pread failed");
      throw runtime_error("pread failed at file: " + file_name +
                          " offset: " + to_string(scan_offset));
    }
    for (int i = 0; i < PAGE_NUM_ENTRIES; ++i) {
      auto entry = read_buffer[i];
      if (entry.key >= key1 && entry.key <= key2) {
        // Entry is in range, push it to the return vector
        entries_in_range.push_back(entry);
      } else if (entry.key > key2) {
        // Entry is beyond key2, meaning all entries after it are also not in
        // range
        break;
      }
    }

    // Once all the entries in this page have been processed, increment
    // scan_offset to read the next page of entries.
    scan_offset += PAGE_SIZE;
  }
  return entries_in_range;
}

vector<KeyValuePair> Database::binary_search_scan(int64_t key1, int64_t key2,
                                                  int fileSize, int fd,
                                                  const string &file_name) {
  int left = 0;
  int right = fileSize - PAGE_SIZE;

  vector<KeyValuePair> pairs(
      PAGE_NUM_ENTRIES);  // Buffer for one page of key-value pairs
  vector<KeyValuePair> values;
  bool rangeFound = false;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    mid -= mid % PAGE_SIZE;  // Align mid to PAGE_SIZE.

    ssize_t bytesRead = pread(fd, pairs.data(), PAGE_SIZE, mid);
    if (bytesRead <= 0) {
      perror("pread failed");
      vector<KeyValuePair> empty;
      return empty;
    }

    int64_t last_key_read = numeric_limits<int64_t>::min();

    for (int i = 0; i < PAGE_NUM_ENTRIES; ++i) {
      if (pairs[i].key == 0) break;  // Assuming 0 is padding

      last_key_read = pairs[i].key;
      if (pairs[i].key > key2) break;  // The value is no longer in page we want

      if (pairs[i].key <= key2 && pairs[i].key >= key1) {
        rangeFound = true;
        values.push_back(pairs[i]);
      }
    }

    if (last_key_read < key2 && last_key_read > key1) {
      rangeFound = true;
      left += 1;
    } else if (key2 > last_key_read) {
      left = mid + PAGE_SIZE;
    } else if (last_key_read > key2 and rangeFound) {
      return values;
    } else {
      right = mid - 1;
    }
  }

  return values;
}

ScanResponse Database::Scan(const int64_t &key1, const int64_t &key2) {
  if (key1 >= key2) {
    vector<KeyValuePair> empty;
    ScanResponse emptyScan;
    emptyScan.result = empty;
    emptyScan.size = 0;
    return emptyScan;
  }

  set<KeyValuePair, CompareByKey> value_set;
  vector<KeyValuePair> memtableValues = memtable.scan(key1, key2);

  for (auto memtableValue : memtableValues) {
    value_set.insert(memtableValue);
  }

  for (size_t i = ssts.size(); i-- > 0;) {
    const auto &sst = ssts[i];
    string path_to_file = database_dir + "/" + sst;
    string file_name = sst.substr(0, sst.size() - 4);

    int fd = open(path_to_file.c_str(), O_RDONLY | O_DIRECT);
    if (fd == -1) {
      perror("Failed to open the file");
      vector<KeyValuePair> empty;
      ScanResponse emptyScan;
      emptyScan.result = empty;
      emptyScan.size = 0;
      return emptyScan;
    }

    vector<KeyValuePair> sst_values;

    if (db_type == SORTED_SST) {
      int file_size = (int)get_file_size(path_to_file);
      sst_values = binary_search_scan(key1, key2, file_size, fd, file_name);
    } else {
      vector<KeyValuePair> buffer(PAGE_NUM_ENTRIES);
      BSSTMetadata metadata = get_btree_metadata(fd, buffer, file_name);

      sst_values = b_tree_scan(key1, key2, metadata.filter_offset,
                               metadata.entries_offset, fd, file_name);
    }

    // Checking each KeyValuePair in sst_values to see if the key is already in
    // value_set If not, then insert it
    for (size_t i = 0; i < sst_values.size(); i++) {
      if (value_set.find(sst_values[i]) == value_set.end()) {
        value_set.insert(sst_values[i]);
      }
    }

    if (close(fd) == -1) {
      perror("Error closing file in Database::Get\n");
      exit(EXIT_FAILURE);
    }
  }

  vector<KeyValuePair> valuesInRange;
  for (const auto &pair : value_set) {
    if (pair.value != 0) {
      valuesInRange.push_back(pair);
    }
  }
  ScanResponse scanQuery;
  scanQuery.result = valuesInRange;
  scanQuery.size = (int)valuesInRange.size();
  return scanQuery;
}

string Database::merge_sort_SSTs(const vector<string> &sstsToMerge) {
  int fd_old = open((database_dir + "/" + sstsToMerge[0]).c_str(), O_RDONLY | O_DIRECT);
  int fd_new = open((database_dir + "/" + sstsToMerge[1]).c_str(), O_RDONLY | O_DIRECT);

  if (fd_old == -1 || fd_new == -1) {
    perror("Failed to open SST files");
    exit(EXIT_FAILURE);
  }
  vector<KeyValuePair> metadataBufferOld(PAGE_NUM_ENTRIES);
  vector<KeyValuePair> metadataBufferNew(PAGE_NUM_ENTRIES);

  BSSTMetadata olderSSTMetadata =
      get_btree_metadata(fd_old, metadataBufferOld, sstsToMerge[0]);
  BSSTMetadata newerSSTMetadata =
      get_btree_metadata(fd_new, metadataBufferNew, sstsToMerge[1]);

  string oldfile_name = sstsToMerge[0].substr(0, sstsToMerge[0].size() - 4);
  string newfile_name = sstsToMerge[1].substr(0, sstsToMerge[1].size() - 4);

  int64_t olderCurrentOffset = olderSSTMetadata.entries_offset;
  int64_t newerCurrentOffset = newerSSTMetadata.entries_offset;

  // Buffers to store entries
  vector<KeyValuePair> olderBuffer(PAGE_NUM_ENTRIES);
  vector<KeyValuePair> newerBuffer(PAGE_NUM_ENTRIES);
  vector<BTreePair *> mergedEntries;

  // Read the initial pages
  find_page(fd_old, olderBuffer, olderCurrentOffset, oldfile_name);
  find_page(fd_new, newerBuffer, newerCurrentOffset, newfile_name);

  int oldIndex = 0, newIndex = 0;

  while (olderCurrentOffset < olderSSTMetadata.filter_offset &&
         newerCurrentOffset < newerSSTMetadata.filter_offset) {
    // Perform the merge until one buffer is exhausted
    while (oldIndex < PAGE_NUM_ENTRIES && newIndex < PAGE_NUM_ENTRIES) {
      if (olderBuffer[oldIndex].key == 0) {
        oldIndex = PAGE_NUM_ENTRIES;
        break;
      }
      if (newerBuffer[newIndex].key == 0) {
        newIndex = PAGE_NUM_ENTRIES;
        break;
      }

      if (olderBuffer[oldIndex].key == newerBuffer[newIndex].key) {
        BTreePair *pair = new BTreePair(newerBuffer[newIndex].key,
                                        newerBuffer[newIndex].value);
        mergedEntries.push_back(pair);
        oldIndex += 1;
        newIndex += 1;
      } else if (olderBuffer[oldIndex].key < newerBuffer[newIndex].key) {
        BTreePair *pair = new BTreePair(olderBuffer[oldIndex].key,
                                        olderBuffer[oldIndex].value);
        mergedEntries.push_back(pair);
        oldIndex += 1;
      } else {  // newer key is less than older key
        BTreePair *pair = new BTreePair(newerBuffer[newIndex].key,
                                        newerBuffer[newIndex].value);
        mergedEntries.push_back(pair);
        newIndex += 1;
      }
    }

    // Read the next page if we've reached the end of one buffer
    if (oldIndex == PAGE_NUM_ENTRIES &&
        olderCurrentOffset < olderSSTMetadata.filter_offset) {
      // remove page from bufferpool if exist
      if (bufferpool_enabled) {
        bufferpool.remove(oldfile_name + "#" + to_string(olderCurrentOffset));
      }
      olderCurrentOffset += PAGE_SIZE;
      find_page(fd_old, olderBuffer, olderCurrentOffset, oldfile_name);
      if (olderCurrentOffset >= olderSSTMetadata.filter_offset) {
        break;
      };
      oldIndex = 0;
    }
    if (newIndex == PAGE_NUM_ENTRIES &&
        newerCurrentOffset < newerSSTMetadata.filter_offset) {
      // remove page from bufferpool if exist
      if (bufferpool_enabled) {
        bufferpool.remove(oldfile_name + "#" + to_string(olderCurrentOffset));
      }
      newerCurrentOffset += PAGE_SIZE;
      find_page(fd_new, newerBuffer, newerCurrentOffset, newfile_name);
      if (newerCurrentOffset >= newerSSTMetadata.filter_offset) {
        break;
      };
      newIndex = 0;
    }
  }

  // Fill up the mergedEntries with the rest of the other data.
  if (oldIndex < PAGE_NUM_ENTRIES) {
    while (oldIndex < PAGE_NUM_ENTRIES &&
           olderCurrentOffset < olderSSTMetadata.filter_offset) {
      if (olderBuffer[oldIndex].key != 0) {
        BTreePair *pair = new BTreePair(olderBuffer[oldIndex].key,
                                        olderBuffer[oldIndex].value);
        mergedEntries.push_back(pair);
        oldIndex += 1;
      } else {
        oldIndex = PAGE_NUM_ENTRIES;
      }

      if (oldIndex == PAGE_NUM_ENTRIES &&
          olderCurrentOffset < olderSSTMetadata.filter_offset) {
        // remove page from bufferpool if exist
        if (bufferpool_enabled) {
          bufferpool.remove(oldfile_name + "#" + to_string(olderCurrentOffset));
        }
        olderCurrentOffset += PAGE_SIZE;
        find_page(fd_old, olderBuffer, olderCurrentOffset, oldfile_name);
        if (olderCurrentOffset >= olderSSTMetadata.filter_offset) {
          break;
        };
        oldIndex = 0;
      }
    }
  }

  if (newIndex < PAGE_NUM_ENTRIES) {
    while (newIndex < PAGE_NUM_ENTRIES &&
           newerCurrentOffset < newerSSTMetadata.filter_offset) {
      if (newerBuffer[newIndex].key != 0) {
        BTreePair *pair = new BTreePair(newerBuffer[newIndex].key,
                                        newerBuffer[newIndex].value);
        mergedEntries.push_back(pair);
        newIndex += 1;
      } else {
        newIndex = PAGE_NUM_ENTRIES;
      }

      if (newIndex == PAGE_NUM_ENTRIES &&
          newerCurrentOffset < newerSSTMetadata.filter_offset) {
        // remove page from bufferpool if exist
        if (bufferpool_enabled) {
          bufferpool.remove(oldfile_name + "#" + to_string(olderCurrentOffset));
        }
        newerCurrentOffset += PAGE_SIZE;
        find_page(fd_new, newerBuffer, newerCurrentOffset, newfile_name);
        if (newerCurrentOffset >= newerSSTMetadata.filter_offset) {
          break;
        };
        newIndex = 0;
      }
    }
  }

  // Close all file descriptors
  close(fd_old);
  close(fd_new);

  string new_sst_file_name =
      "BSST_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count()) +
      ".bin";
  string new_sst_path = database_dir + "/" + new_sst_file_name;

  memtable.CreateCompactedBSST(new_sst_path, mergedEntries,
                               mergedEntries.size());

  return new_sst_file_name;
}

void Database::check_LSM_compaction() {
  string new_sst = ssts.back();
  lsm_tree[1].push_back(new_sst);

  vector<int> levelsToRemove;
  for (auto &level : lsm_tree) {
    if (level.second.size() >= 2) {
      // You have two or more SSTs at the same level, perform a merge sort
      string newSSTName = merge_sort_SSTs(level.second);
      // Delete old SST files
      remove((database_dir + "/" + level.second[0]).c_str());
      remove((database_dir + "/" + level.second[1]).c_str());

      level.second.clear();
      levelsToRemove.push_back(level.first);

      // Move the new SST to the next higher level
      lsm_tree[level.first + 1].push_back(newSSTName);
    }
  }

  // Remove marked levels from lsm_tree
  for (int levelKey : levelsToRemove) {
    lsm_tree.erase(levelKey);
  }
}

void Database::Put(const int64_t &key, const int64_t &value) {
  bool sstCreated = memtable.put(key, value);

  if (sstCreated) {
    get_ssts_from_db(database_dir);

    // If the db_type is an LSM_TREE, we need to check if we need to run the
    // compaction policy
    if (db_type == LSM_TREE) {
      check_LSM_compaction();
      get_ssts_from_db(database_dir);
    }
  }
}

void Database::set_bufferpool_enabled(bool enabled) {
  bufferpool_enabled = enabled;
}

string Database::get_db_type() { return db_type; }

void Database::Close() {
  int memtable_entries = memtable.get_size();
  if (memtable_entries != 0) {
    memtable.close();
    get_ssts_from_db(database_dir);

    if (db_type == LSM_TREE) {
      check_LSM_compaction();

      save_LSM_tree_state(database_dir + "/lsm_tree_state.txt");
    }
  } else {
    if (db_type == LSM_TREE) {
      save_LSM_tree_state(database_dir + "/lsm_tree_state.txt");
    }
  }
}

void Database::save_LSM_tree_state(const string &file_name) {
  std::ofstream file(file_name);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open file to save LSM tree state.");
  }

  for (const auto &level : lsm_tree) {
    file << level.first;  // Write level number
    for (const auto &sst : level.second) {
      file << "," << sst;  // Write SST file_names separated by commas
    }
    file << "\n";
  }

  file.close();
}

void Database::load_LSM_tree_state(const std::string &file_name) {
  std::ifstream file(file_name);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open file to load LSM tree state.");
  }

  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string token;
    int level;
    std::vector<std::string> ssts;

    // Get level number
    std::getline(iss, token, ',');
    level = std::stoi(token);

    // Get SST file_names
    while (std::getline(iss, token, ',')) {
      ssts.push_back(token);
    }

    lsm_tree[level] = ssts;
  }

  file.close();
}
