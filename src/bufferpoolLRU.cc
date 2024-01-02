#include "bufferpoolLRU.hh"

#include <iostream>

using namespace std;

void LRUQueue::move_to_tail(shared_ptr<LRUNode> node) {
  // node MUST in the queue and node MUST not be null
  if (tail == node) {
    // the node is already at the tail
    return;
  }
  // If the node to move is the head
  if (head == node) {
    head = node->next;
    head->prev = nullptr;
  } else {
    // Disconnect the node from its current position
    node->prev->next = node->next;
    if (node->next) {
      node->next->prev = node->prev;
    }
  }
  // Move the node to the tail
  tail->next = node;
  node->prev = tail;
  node->next = nullptr;
  tail = node;  // Update the tail pointer
}

void LRUQueue::remove_node(shared_ptr<LRUNode> node) {
  if (node->next) {
    node->next->prev = node->prev;
  } else {
    // Node is tail
    tail = node->prev;
  }
  if (node->prev) {
    node->prev->next = node->next;
  } else {
    // Node is head
    head = node->next;
  }
  // memory is handled automatically
  return;
}

string LRUQueue::remove_head() {
  if (!head) return "";

  if (head->next) head->next->prev = nullptr;

  string page_id = head->page_id;
  // memory is handled automatically
  head = head->next;
  if (!head) tail = nullptr;
  return page_id;
}

void LRUQueue::print_queue() {
  std::cerr << "[ ";
  LRUNode *curr = head.get();
  while (curr) {
    std::cerr << curr->page_id << " ";
    curr = curr->next.get();
  }
  std::cerr << "]" << std::endl;
}

void LRUQueue::put(shared_ptr<LRUNode> node) {
  // node MUST be DNE in the queue
  if (!tail) {
    head = tail = node;
  } else {
    tail->next = node;
    node->prev = tail;
    tail = node;
  }
}