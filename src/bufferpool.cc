#include "bufferpool.hh"

#include <cmath>
#include <iostream>
#include <memory>
#include <optional>

#include "MurmurHash3.hh"

using namespace std;

Bufferpool::Bufferpool(size_t size) : capacity(size), num_pages(0) {
  num_buckets = nearest_power_of_2(capacity);
  table.resize(num_buckets);
}

Bufferpool::~Bufferpool() {}

size_t Bufferpool::nearest_power_of_2(size_t N) {
  size_t a = log2(N);
  if (pow(2, a) == N) return N;
  return pow(2, a + 1);
}

void Bufferpool::evict() {
  string page_id = queue.remove_head();
  if (page_id != "") {
    // Remove corresponding frame in the table
    remove(page_id);
    return;
  }
  // Somethings is wrong
  std::cerr << "Fatal: try to remove lruNode that DNE." << std::endl;
  exit(EXIT_FAILURE);
}

void Bufferpool::resize(size_t new_capacity) {
  capacity = new_capacity;
  while (new_capacity < num_pages && num_pages != 0) {
    evict();
  }
  // Want to shrink buckets if there are large overheads
  size_t num_bucket_candidate = nearest_power_of_2(new_capacity);
  if (num_bucket_candidate < num_buckets) {
    num_buckets = num_bucket_candidate;
    // Rehash everything and put into new table
    vector<unique_ptr<Bucket>> new_table(num_buckets);
    for (auto &head : table) {
      while (head) {
        // Extract the node from the old table
        unique_ptr<Bucket> old_head = std::move(head);
        // Advance the head to the next node
        head = std::move(old_head->next);
        // Compute the new index for the old head
        uint32_t index = get_key(old_head->page_id);
        // Insert the node into the new table
        old_head->next = std::move(new_table[index]);
        new_table[index] = std::move(old_head);
      }
    }
    table = std::move(new_table);
  }
}

void Bufferpool::insert(const std::string &page_id,
                        vector<KeyValuePair> &value) {
  if (capacity == 0) {
    return;
  }
  // If bufferpool is full
  if (capacity == num_pages) {
    evict();
  }
  uint32_t index = get_key(page_id);
  unique_ptr<Bucket> newBucket(new Bucket(page_id, value));
  newBucket->next = std::move(table[index]);

  // Put into LRU queue
  shared_ptr<LRUNode> node = make_shared<LRUNode>(LRUNode(page_id));
  queue.put(node);
  newBucket->lruNode = node;

  table[index] = std::move(newBucket);
  num_pages++;
}

bool Bufferpool::remove(const std::string &page_id) {
  uint32_t index = get_key(page_id);
  Bucket *prev = nullptr, *head = table[index].get();
  while (head != nullptr) {
    if (head->page_id == page_id) {
      queue.remove_node(head->lruNode);
      if (prev)
        prev->next = std::move(head->next);
      else
        table[index] = std::move(head->next);
      // Memory are automatically released
      num_pages--;
      return true;
    }
    prev = head;
    head = (head->next).get();
  }
  return false;
}

vector<KeyValuePair> *Bufferpool::search(const std::string &page_id) {
  uint32_t index = get_key(page_id);
  Bucket *head = table[index].get();
  while (head != nullptr) {
    if (head->page_id == page_id) {
      // Move to tail of LRU queue for new access.
      queue.move_to_tail(head->lruNode);
      return &(head->data);
    }
    head = (head->next).get();
  }
  return nullptr;
}

uint32_t Bufferpool::get_key(const std::string &page_id) {
  uint32_t hash_otpt;
  MurmurHash3_x86_32(page_id.c_str(), (uint32_t)page_id.size(), SEED,
                     &hash_otpt);
  return hash_otpt % num_buckets;
}
