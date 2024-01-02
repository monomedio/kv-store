#ifndef B_TREE_H
#define B_TREE_H

#include <algorithm>
#include <vector>

class EntryBase {
 public:
  virtual ~EntryBase() {}
  virtual bool isBTreeNode() const { return false; }
  static void clearBTree(EntryBase* node);
};

class BTreePair : public EntryBase {
 public:
  BTreePair(int64_t key, int64_t value);
  int64_t get_key();
  int64_t get_value();

 private:
  int64_t key;
  int64_t value;
};

class BTreeNode : public EntryBase {
 public:
  BTreeNode();
  std::vector<EntryBase*>& get_entries();
  int64_t get_max_key();
  int get_size();
  void insert(
      EntryBase* entry);  // Use a common base class pointer for insertion
  bool isBTreeNode() const override { return true; }

 private:
  int64_t max_key;
  std::vector<EntryBase*> entries;  // Store pointers to EntryBase (can be
                                    // KeyValuePair or BTreeNode)
  int size;
};

void verify_b_tree_correctness(BTreeNode* root);

#endif  // B_TREE_H
