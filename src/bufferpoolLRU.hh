#ifndef LRU_HH_
#define LRU_HH_

#include <iostream>
#include <memory>

using namespace std;

struct LRUNode {
  const string page_id;            // ID of the page
  shared_ptr<LRUNode> prev, next;  // Prev node and next node
  LRUNode(const string &k) : page_id(k), prev(nullptr), next(nullptr) {}
};

class LRUQueue {
  /**
   * The bufferpool uses LRU as the eviction policy.
   *
   * Note that LRU Queue does not record capacity. Its capacity is the
   * same as the bufferpool and bufferpool should call evict when it is full.
   */
  shared_ptr<LRUNode> head, tail;

 public:
  LRUQueue() : head(nullptr), tail(nullptr) {}

  /**
   * Put a page_id of a page into the queue.
   * The page MUST NOT exist in the queue before.
   */
  void put(shared_ptr<LRUNode> node);

  /**
   * Print page_ids in order from head to tail.
   * For testing only.
   */
  void print_queue();

  /**
   * Move node to tail of the queue.
   * The node MUST exists in the queue.
   */
  void move_to_tail(shared_ptr<LRUNode> node);

  /**
   * Remove head of the queue (eviction) and return the page_id it points to.
   * The node MUST exists in the queue.
   */
  string remove_head();

  /**
   * Remove perticular node of the queue (eviction).
   * Used when the page is invalidated.
   * The node MUST exists in the queue.
   */
  void remove_node(shared_ptr<LRUNode> node);
};

#endif  // LRU_HH_