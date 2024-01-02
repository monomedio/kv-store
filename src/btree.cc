#include "btree.hh"

#include <cmath>
#include <iostream>

BTreePair::BTreePair(int64_t key, int64_t value) : key(key), value(value) {}

int64_t BTreePair::get_key() { return key; }

int64_t BTreePair::get_value() { return value; }

BTreeNode::BTreeNode() : max_key(0), size(0) {}

std::vector<EntryBase*>& BTreeNode::get_entries() { return entries; }

int64_t BTreeNode::get_max_key() { return max_key; }

int BTreeNode::get_size() { return size; }

void BTreeNode::insert(EntryBase* entry) {
  if (entry->isBTreeNode()) {
    BTreeNode* bnode = static_cast<BTreeNode*>(entry);
    max_key = std::max(max_key, bnode->get_max_key());
  } else {
    BTreePair* kvp = static_cast<BTreePair*>(entry);
    max_key = std::max(max_key, kvp->get_key());
  }
  entries.push_back(entry);
  size++;
}

void EntryBase::clearBTree(EntryBase* node) {
  if (node->isBTreeNode()) {
    std::vector<EntryBase*> entries =
        (dynamic_cast<BTreeNode*>(node))->get_entries();
    for (auto entry : entries) {
      clearBTree(entry);
    }
  }
  delete node;
}
