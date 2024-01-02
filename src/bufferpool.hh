#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <memory>
#include <string>
#include <vector>

#include "bufferpoolLRU.hh"
#include "constants.hh"
#include "memtable.hh"

using namespace std;
/**
 * @brief Each Bucket(Frame) contains page and the metadata
 */
struct Bucket {
  const string page_id;         // the ID of the page
  vector<KeyValuePair> data;    // the data in page, contains 256 entires
  unique_ptr<Bucket> next;      // next node in the bucket, if there is any
  shared_ptr<LRUNode> lruNode;  // lruNode of the page
  /**
   * @brief Construct a new Bucket
   *
   * @param page_id the ID of the page, consisting file name and page offset
   * @param data the data in page
   */
  Bucket(const string &page_id, const vector<KeyValuePair> &data)
      : page_id(page_id), data(data), next(nullptr), lruNode(nullptr) {}
};

class Bufferpool {
 private:
  vector<unique_ptr<Bucket>> table;  // Hash table
  LRUQueue queue;                    // LRU Queue
  size_t capacity;                   // Capacity in terms of number of pages
  size_t num_buckets;                // Number of Buckets
  size_t num_pages;                  // Current number of pages

  /**
   * Evict a page from buffer pool.
   */
  void evict();

  /**
   *  Return a number that is the smallest power of 2 greater than N.
   *  Used to calculate bucket size.
   */
  size_t nearest_power_of_2(size_t N);

  /**
   * Return the hash key of given page_id
   */
  uint32_t get_key(const std::string &page_id);

 public:
  Bufferpool(size_t size);
  ~Bufferpool();  // Destructor

  void insert(const std::string &page_id, vector<KeyValuePair> &page);
  /**
   * Return false when no such page with page_id exist in buffer pool, otherwise
   * return true and remove the page from table.
   */
  bool remove(const std::string &page_id);
  /**
   * Return true if found the page of a given page_id in buffer pool, false
   * otherwise. Point page to the found page.
   */
  vector<KeyValuePair> *search(const std::string &page_id);
  /**
   * Set new capacity of hash table. If more page are in the table than the new
   * capacity, evict them.
   */
  void resize(size_t new_capacity);
};

#endif  // BUFFERPOOL_H