//
// Created by Shawn Zhong on 2019-03-17.
//

#include "btree.h"
#include "filescan.h"

using namespace std;
using namespace badgerdb;

const string getIndexName(const string &relationName,
                          const int attrByteOffset) {
  ostringstream idxStr;
  idxStr << relationName << ',' << attrByteOffset;
  return idxStr.str();
}

int findInsertionIndex(int arr[], int key) {
  int index = 0;  // binary search
  return index;
}

NonLeafNodeInt *splitNonLeafNode(NonLeafNodeInt *node, int index) {
  //               copy last half of origNode to a newNode (malloc / ?)
  //               clean last half of origNode
  //               return newNode
  return nullptr;
}

LeafNodeInt *splitLeafNode(LeafNodeInt *node, int index) {
  //               copy last half (split by index) of origNode to a newNode
  //               (malloc / ?) clean last half of origNode return newNode
  return nullptr;
}

void insertToNonLeafNode(NonLeafNodeInt *node, int index, int key,
                         PageId pageID) {
  return;
}

void insertToLeafNode(LeafNodeInt *node, int index, int key, RecordId rid) {
  return;
}

bool isNonLeafNodeFull(NonLeafNodeInt *node) {
  // if last pageID of current node is occupied (not 0) // full, state in
  // assumption: pageID is zero => unoccupied
  return false;
}

bool isLeafNodeFull(LeafNodeInt *node) {
  // if last pageID of current node is occupied (not 0) // full, state in
  // assumption: pageID is zero => unoccupied
  return false;
}

BlobPage *createPageForNode(void *node) { return nullptr; }

bool isLeaf(BlobPage *page) {
  void *node = page->getNode();
  return *((int *)node) == -1;
}

BlobPage *getBlogPageByPid(PageId page_id) { return nullptr; }

const BlobPage *insertToLeafPage(BlobPage *origPage, const int key,
                                 const RecordId &rid, int *midVal) {
  LeafNodeInt *origNode = (struct LeafNodeInt *)origPage->getNode();
  int index = findInsertionIndex(origNode->keyArray, key);

  if (!isLeafNodeFull(origNode)) {
    insertToLeafNode(origNode, index, key, rid);
    origPage->setNode(origNode);
    return nullptr;
  }

  int capacity = INTARRAYLEAFSIZE;
  bool insertToLeft = index < capacity / 2;
  LeafNodeInt *newNode = splitLeafNode(origNode, capacity / 2 + insertToLeft);

  if (insertToLeft)
    insertToLeafNode(origNode, index, key, rid);
  else
    insertToLeafNode(newNode, index - capacity / 2, key, rid);

  BlobPage *newPage = createPageForNode(newNode);
  newNode->rightSibPageNo = origNode->rightSibPageNo;
  origNode->rightSibPageNo = newPage->page_number();

  origPage->setNode(origNode);
  newPage->setNode(newNode);

  *midVal = newNode->keyArray[0];
  return newPage;
}

const BlobPage *insertHelper(BlobPage *origPage, const int key,
                             const RecordId rid, int *midVal) {
  if (isLeaf(origPage))  // base case
    return insertToLeafPage(origPage, key, rid, midVal);

  NonLeafNodeInt *origNode = (NonLeafNodeInt *)origPage->getNode();

  int origChildPageId = findInsertionIndex(origNode->keyArray, key);
  BlobPage *origChildPage = getBlogPageByPid(origChildPageId);

  // insert key, rid to child and check whether child is splitted
  int newChildMidVal;
  const BlobPage *newChildPage =
      insertHelper(origChildPage, key, rid, &newChildMidVal);

  // not split in child
  if (newChildPage == nullptr) return nullptr;

  PageId newChildPageId = newChildPage->page_number();

  // split in child, need to add splitted child to currNode
  int index = findInsertionIndex(origNode->keyArray, key);
  if (!isNonLeafNodeFull(origNode)) {  // current node is not full
    insertToNonLeafNode(origNode, index, newChildMidVal, newChildPageId);
    origPage->setNode(origNode);
    return nullptr;
  }

  int capacity = INTARRAYNONLEAFSIZE;

  bool insertToLeft = index < capacity / 2;
  NonLeafNodeInt *newNode =
      splitNonLeafNode(origNode, capacity / 2 + insertToLeft);
  if (insertToLeft)
    insertToNonLeafNode(origNode, index, newChildMidVal, newChildPageId);
  else
    insertToNonLeafNode(newNode, index - capacity / 2, newChildMidVal,
                        newChildPageId);
  origPage->setNode(origNode);
  BlobPage *newPage = createPageForNode(newNode);
  *midVal = newNode->keyArray[0];
  return newPage;
}

int main(int argc, char *argv[]) {
  cout << getIndexName("123", 5) << endl;
  cout << findInsertionIndex(nullptr, 1) << endl;

  return 0;
}