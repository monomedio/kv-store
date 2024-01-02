#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <random>

#include "../src/constants.hh"
#include "../src/database.hh"

using namespace std;

void deleteAllFilesInDirectory(const string &directoryPath) {
  DIR *dir = opendir(directoryPath.c_str());
  if (dir == nullptr) {
    cerr << "Database doesn't exist. Will create later" << directoryPath
         << endl;
    return;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (string(entry->d_name) == "." || string(entry->d_name) == "..") {
      continue;
    }

    string filePath = directoryPath + "/" + string(entry->d_name);

    struct stat path_stat;
    stat(filePath.c_str(), &path_stat);
    if (S_ISREG(path_stat.st_mode)) {
      if (unlink(filePath.c_str()) != 0) {
        cerr << "Failed to delete file: " << filePath << endl;
      }
    }
  }

  closedir(dir);

  // Attempt to remove the directory
  if (rmdir(directoryPath.c_str()) != 0) {
    cerr << "Failed to delete directory: " << directoryPath << endl;
  }
}

void experiment1() {
  deleteAllFilesInDirectory("experiments/ssts/database_experiments");

  Database database(262144, 0);
  database.set_bufferpool_enabled(false);
  database.Open("experiments/ssts/database_experiments", SORTED_SST);

  int MB = 1048576;
  vector<double> put_throughputs;
  vector<double> get_throughputs;
  vector<double> scan_throughputs;

  int db_entries = 0;

  for (int i = 0; i < 11; i++) {
    // Number of keys to insert
    int num_inserts;
    // Number of keys inserted after puts are complete
    int num_keys = (int)pow(2, i) * MB / 16;
    // Random generator setup
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> distrib(1, num_keys);

    if (i == 0) {
      num_inserts = 65536;
    } else {
      num_inserts = (int)(pow(2, i) * MB) / 32;
    }

    cout << "Increasing DB size to " << pow(2, i) << " MB\n";
    // START MEASURING PUTS
    auto start = chrono::high_resolution_clock::now();
    for (int j = db_entries; j < num_keys; j++) {
      int insert_key = j + 1;
      database.Put(insert_key, 1);
    }
    auto stop = chrono::high_resolution_clock::now();
    auto duration = chrono::duration<double>(stop - start).count();

    put_throughputs.push_back(num_inserts / duration);

    db_entries = num_keys;

    // START MEASURING GETS
    // Getting 0.01% of number of keys inserted because
    // memory might overload
    int num_gets = 0.0001 * num_keys;
    start = chrono::high_resolution_clock::now();
    for (int j = 0; j < num_gets; j++) {
      int get_key = distrib(gen);
      if (database.Get(get_key) == -1) {
        perror("Key should be in database");
        exit(EXIT_FAILURE);
      }
    }
    stop = chrono::high_resolution_clock::now();
    duration = chrono::duration<double>(stop - start).count();
    get_throughputs.push_back(num_gets / duration);

    // START MEASURING SCAN
    uniform_int_distribution<> distrib_scan(1, num_keys - 255);
    // Doing this so scans don't result in memory overload
    int num_scans = ceil(0.1 * num_gets);
    start = chrono::high_resolution_clock::now();
    for (int j = 0; j < num_scans; j++) {
      int key1 = distrib_scan(gen);
      int key2 = key1 + 255;
      database.Scan(key1, key2);
    }
    stop = chrono::high_resolution_clock::now();
    duration = chrono::duration<double>(stop - start).count();
    scan_throughputs.push_back(num_scans / duration);
  }

  cout << "Experiment 1 results:" << endl;

  cout << "PUT ops/s CSV:\n\n";
  cout << "Database Size (MB),Throughput (op/s)\n";
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << put_throughputs[i] << endl;
  }

  cout << "\n\nGET ops CSV:\n\n";
  cout << "Database Size (MB),Throughput (op/s)\n";
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << get_throughputs[i] << endl;
  }
  cout << "\n\nSCAN ops CSV:\n\n";
  cout << "Database Size (MB),Throughput (op/s)\n";
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << scan_throughputs[i] << endl;
  }
  deleteAllFilesInDirectory("experiments/ssts/database_experiments");
}

void experiment2HelperConcurrent() {
  // The size of the memtable will store 8MB = 8388608 bytes = 524288 entries
  // The size of the bufferpool will store 10 MB = 10485760 bytes = 2560 pages
  int MB = 1048576;
  int memtable_mb = 1;

  int memtable_size =
      memtable_mb * MB / 16;  // in terms of number of 16 byte entries
  int bufferpool_capacity;
  int bufferpool_enabled = true;
  if (bufferpool_enabled) {
    bufferpool_capacity = 2560;  // in terms of pages
  } else {
    bufferpool_capacity = 0;
  }

  Database sorted_database(memtable_size, bufferpool_capacity);
  sorted_database.set_bufferpool_enabled(bufferpool_enabled);
  sorted_database.Open("experiments/ssts/sorted", SORTED_SST);

  Database b_database(memtable_size, bufferpool_capacity);
  b_database.set_bufferpool_enabled(bufferpool_enabled);
  b_database.Open("experiments/ssts/bsst", BSST);
  vector<double> get_throughputs_sorted;
  vector<double> get_throughputs_b;

  for (int i = 0; i < 11; i++) {
    // Number of keys in db after puts are complete
    int num_keys = (int)pow(2, i) * MB / 16;

    // Number of inserts to make
    int num_inserts;
    if (i == 0) {
      num_inserts = 65536;
    } else {
      num_inserts = (int)(pow(2, i) * MB) / 32;
    }

    // Pre-generating keys to read
    const unsigned int seed = 123456789;
    mt19937 gen(seed);
    uniform_int_distribution<> distrib(1, num_keys);

    // Insert num_inserts keys
    cout << "Inserting " << num_inserts * 16 / MB << " MB" << endl;
    for (int j = 0; j < num_inserts; j++) {
      int insert_key = distrib(gen);
      sorted_database.Put(insert_key, insert_key);
      b_database.Put(insert_key, insert_key);
    }

    // Getting 0.001% of number of keys inserted because memory might overload
    int num_gets = ceil(0.0001 * num_keys);

    int get_keys[num_gets];
    for (int j = 0; j < num_gets; j++) {
      get_keys[j] = distrib(gen);
    }

    cout << "Reading database with size " << num_keys * 16 / MB << "MB" << endl;

    // START MEASURING GETS
    auto start = chrono::steady_clock::now();
    for (int j = 0; j < num_gets; j++) {
      sorted_database.Get(get_keys[j]);
    }
    auto stop = chrono::steady_clock::now();
    auto duration = chrono::duration<double>(stop - start).count();
    get_throughputs_sorted.push_back(num_gets / duration);

    start = chrono::steady_clock::now();
    for (int j = 0; j < num_gets; j++) {
      b_database.Get(get_keys[j]);
    }
    stop = chrono::steady_clock::now();
    duration = chrono::duration<double>(stop - start).count();
    get_throughputs_b.push_back(num_gets / duration);
  }

  cout << "Throughput:" << endl;

  cout << sorted_database.get_db_type() << " database size (MB),"
       << "Get ops/s" << endl;
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << get_throughputs_sorted[i] << endl;
  }

  cout << b_database.get_db_type() << " database size (MB),"
       << "Get ops/s" << endl;
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << get_throughputs_b[i] << endl;
  }

  cout << "Comparative performance:" << endl;
  cout << "database size(MB),relative throughput of B-tree to binary search "
          "(ops/s)"
       << endl;
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << ","
         << get_throughputs_b[i] / get_throughputs_sorted[i] << endl;
  }

  sorted_database.Close();
  b_database.Close();
}

void experiment2() {
  deleteAllFilesInDirectory("experiments/ssts/sorted");
  deleteAllFilesInDirectory("experiments/ssts/bsst");
  experiment2HelperConcurrent();
}

void experiment3() {
  deleteAllFilesInDirectory("experiments/ssts/lsm-tree");
  // The size of the memtable will store 8MB = 8388608 bytes = 524288 entries
  // The size of the bufferpool will store 10 MB = 10485760 bytes = 2560 pages
  int MB = 1048576;
  int memtable_mb = 1;

  int memtable_size =
      memtable_mb * MB / 16;  // in terms of number of 16 byte entries
  int bufferpool_capacity;
  int bufferpool_enabled = true;
  if (bufferpool_enabled) {
    bufferpool_capacity = 2560;  // in terms of pages
  } else {
    bufferpool_capacity = 0;
  }

  Database lsm_db(memtable_size, bufferpool_capacity, 5);
  lsm_db.set_bufferpool_enabled(bufferpool_enabled);
  lsm_db.Open("experiments/ssts/lsm-tree", LSM_TREE);

  vector<double> get_throughputs;
  vector<double> put_throughputs;
  vector<double> scan_throughputs;

  for (int i = 0; i < 11; i++) {
    // Number of keys in db after puts are complete
    int num_keys = (int)pow(2, i) * MB / 16;

    // Number of inserts to make
    int num_inserts;
    if (i == 0) {
      num_inserts = 65536;
    } else {
      num_inserts = (int)(pow(2, i) * MB) / 32;
    }

    // Pre-generating keys to read
    const unsigned int seed = 123456789;
    mt19937 gen(seed);
    uniform_int_distribution<> distrib(1, num_keys);

    // Insert num_inserts keys
    // Making value either 0 or 1 to simulate key deletion
    cout << "Inserting " << num_inserts * 16 / MB << " MB" << endl;
    auto start = chrono::steady_clock::now();
    for (int j = 0; j < num_inserts; j++) {
      int insert_key = distrib(gen);
      lsm_db.Put(insert_key, insert_key % 1);
    }
    auto stop = chrono::steady_clock::now();
    auto duration = chrono::duration<double>(stop - start).count();
    put_throughputs.push_back(num_inserts / duration);

    // Getting 0.001% of number of keys inserted because memory might overload
    int num_gets = ceil(0.0001 * num_keys);
    int get_keys[num_gets];
    for (int j = 0; j < num_gets; j++) {
      get_keys[j] = distrib(gen);
    }

    cout << "Reading database with size " << num_keys * 16 / MB << "MB" << endl;

    // START MEASURING GETS
    start = chrono::steady_clock::now();
    for (int j = 0; j < num_gets; j++) {
      lsm_db.Get(get_keys[j]);
    }
    stop = chrono::steady_clock::now();
    duration = chrono::duration<double>(stop - start).count();
    get_throughputs.push_back(num_gets / duration);

    // START MEASURING SCAN
    uniform_int_distribution<> distrib_scan(1, num_keys - 255);
    // Doing this so scans don't result in memory overload
    int num_scans = ceil(0.1 * num_gets);

    pair<int, int> scan_ranges[num_scans];
    for (int j = 0; j < num_scans; j++) {
      int key1 = distrib_scan(gen);
      int key2 = key1 + 255;
      scan_ranges[i] = make_pair(key1, key2);
    }

    start = chrono::steady_clock::now();
    for (int j = 0; j < num_scans; j++) {
      lsm_db.Scan(scan_ranges[i].first, scan_ranges[i].second);
    }

    stop = chrono::steady_clock::now();
    duration = chrono::duration<double>(stop - start).count();
    scan_throughputs.push_back(num_scans / duration);
  }

  cout << "GET Throughput:" << endl;
  cout << lsm_db.get_db_type() << " database size (MB),"
       << "Get ops/s" << endl;
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << get_throughputs[i] << endl;
  }

  cout << "PUT Throughput:" << endl;
  cout << lsm_db.get_db_type() << " database size (MB),"
       << "Put ops/s" << endl;
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << put_throughputs[i] << endl;
  }

  cout << "SCAN Throughput:" << endl;
  cout << lsm_db.get_db_type() << " database size (MB),"
       << "Scan ops/s" << endl;
  for (int i = 0; i < 11; i++) {
    cout << (int)pow(2, i) << "," << scan_throughputs[i] << endl;
  }

  lsm_db.Close();
}

int main() {
  cout << "EXPERIMENT 1" << endl;
  experiment1();
  cout << "EXPERIMENT 2" << endl;
  experiment2();
  cout << "EXPERIMENT 3" << endl;
  experiment3();
}