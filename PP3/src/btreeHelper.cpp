//
// Created by Shawn Zhong on 2019-03-17.
//

#include "btree.h"
#include "filescan.h"

using namespace std;
using namespace badgerdb;

const int SENTINEL = numeric_limits<int>::min();

const string getIndexName(const string &relationName,
                          const int attrByteOffset) {
  ostringstream idxStr;
  idxStr << relationName << ',' << attrByteOffset;
  return idxStr.str();
}

int findInsertionIndex(int arr[], int len, int key) {
  for (int i = 0; i < len; i++)
    if (arr[i] >= key) return i;
  return len;
}

int findInsertionIndexLeaf(LeafNodeInt *node, int key) {
  int len = INTARRAYLEAFSIZE;
  RecordId emptyRid{};
  for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
    if (node->ridArray[i] == emptyRid) {
      len = i;
      break;
    }
  }
  return findInsertionIndex(node->keyArray, len, key);
}

int findInsertionIndexNonLeaf(NonLeafNodeInt *node, int key) {
  int len = INTARRAYNONLEAFSIZE + 1;
  for (int i = 1; i <= INTARRAYNONLEAFSIZE; i++) {
    if (node->pageNoArray[i] == 0) {
      len = i;
      break;
    }
  }
  return findInsertionIndex(node->keyArray, len - 1, key);
}

NonLeafNodeInt *splitNonLeafNode(NonLeafNodeInt *node, int index,
                                 bool moveKeyUp) {
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)calloc(1, sizeof(NonLeafNodeInt));
  size_t rightsize = INTARRAYNONLEAFSIZE - index;
  void *keyptr = &(node->keyArray[index]);
  // set keyArray
  if (moveKeyUp) {
    memcpy(&newNode->keyArray, keyptr, rightsize * sizeof(int));
    memset(keyptr, 0, rightsize * sizeof(int));
  } else {
    void *keyptr2 = &(node->keyArray[index + 1]);
    memcpy(&newNode->keyArray, keyptr2, (rightsize - 1) * sizeof(int));
    memset(keyptr, 0, rightsize * sizeof(int));
  }
  // set pgNoArray
  void *pgnptr = &(node->pageNoArray[index + 1]);
  memcpy(&newNode->pageNoArray, pgnptr, rightsize * sizeof(PageId));
  memset(pgnptr, 0, rightsize * sizeof(PageId));
  newNode->level = node->level;
  return newNode;
}

LeafNodeInt *splitLeafNode(LeafNodeInt *node, int index) {
  LeafNodeInt *newNode = (LeafNodeInt *)calloc(1, sizeof(LeafNodeInt));
  size_t leftsize = index;
  size_t rightsize = INTARRAYLEAFSIZE - index;
  void *keyptr = &(node->keyArray[index]);
  void *ridptr = &(node->ridArray[index]);
  memcpy(&newNode->keyArray, keyptr, rightsize * sizeof(int));
  memcpy(&newNode->ridArray, ridptr, rightsize * sizeof(RecordId));
  memset(keyptr, 0, rightsize * sizeof(int));
  memset(ridptr, 0, rightsize * sizeof(RecordId));
  newNode->level = node->level;
  // r s pg no is set outside;
  return newNode;
}

void insertToNonLeafNode(NonLeafNodeInt *node, int index, int key,
                         PageId pageID) {
  for (int i = INTARRAYNONLEAFSIZE - 1; i > index; i--) {
    node->keyArray[i] = node->keyArray[i - 1];
    node->pageNoArray[i + 1] = node->pageNoArray[i];
  }
  node->keyArray[index] = key;
  node->pageNoArray[index + 1] = pageID;
}

void insertToLeafNode(LeafNodeInt *node, int index, int key, RecordId rid) {
  for (int i = INTARRAYLEAFSIZE - 1; i > index; i--) {
    node->keyArray[i] = node->keyArray[i - 1];
    node->ridArray[i].page_number = node->ridArray[i - 1].page_number;
    node->ridArray[i].slot_number = node->ridArray[i - 1].slot_number;
  }
  node->keyArray[index] = key;
  node->ridArray[index].page_number = rid.page_number;
  node->ridArray[index].slot_number = rid.slot_number;
}

bool isNonLeafNodeFull(NonLeafNodeInt *node) {
  // assumption: pageID is zero => unoccupied
  return node->pageNoArray[INTARRAYNONLEAFSIZE] != 0;
}

bool isLeafNodeFull(LeafNodeInt *node) {
  // assumption: RecordID is zero => unoccupied
  RecordId emptyRid{};
  return node->ridArray[INTARRAYLEAFSIZE - 1] != emptyRid;
}

BlobPage *BTreeIndex::createPageForNode(void *node, PageId *returnPid) {
  PageId pid;
  Page *page;
  BTreeIndex::bufMgr->allocPage(BTreeIndex::file, pid, page);
  BlobPage *newPage = (BlobPage *)page;
  *returnPid = pid;
  newPage->setNode(node);
  return newPage;
}

bool isLeaf(BlobPage *page) {
  void *node = page->getNode();
  return *((int *)node) == -1;
}

BlobPage *BTreeIndex::getBlogPageByPid(PageId page_id) {
  Page *ret = nullptr;
  BTreeIndex::bufMgr->readPage(file, page_id, ret);
  return (BlobPage *)ret;
}

BlobPage *BTreeIndex::insertToLeafPage(BlobPage *origPage, const int key,
                                       const RecordId &rid, int *midVal) {
  LeafNodeInt *origNode = (struct LeafNodeInt *)origPage->getNode();
  int index = findInsertionIndexLeaf(origNode, key);

  if (!isLeafNodeFull(origNode)) {
    insertToLeafNode(origNode, index, key, rid);
    origPage->setNode(origNode);
    return nullptr;
  }

  int middleIndex = (INTARRAYLEAFSIZE - 1) / 2;
  bool insertToLeft = index < middleIndex;
  LeafNodeInt *newNode = splitLeafNode(origNode, middleIndex + insertToLeft);

  if (insertToLeft)
    insertToLeafNode(origNode, index, key, rid);
  else
    insertToLeafNode(newNode, index - middleIndex, key, rid);

  PageId pid;
  BlobPage *newPage = createPageForNode(newNode, &pid);
  newNode->rightSibPageNo = origNode->rightSibPageNo;
  origNode->rightSibPageNo = pid;  // TODO: page_number ?

  origPage->setNode(origNode);
  newPage->setNode(newNode);

  *midVal = newNode->keyArray[0];
  return newPage;
}

BlobPage *BTreeIndex::insertHelper(BlobPage *origPage, const int key,
                                   const RecordId rid, int *midVal) {
  if (isLeaf(origPage))  // base case
    return insertToLeafPage(origPage, key, rid, midVal);

  NonLeafNodeInt *origNode = (NonLeafNodeInt *)origPage->getNode();

  int origChildPageId = findInsertionIndexNonLeaf(origNode, key);
  BlobPage *origChildPage = getBlogPageByPid(origChildPageId);

  // insert key, rid to child and check whether child is splitted
  int newChildMidVal;
  const BlobPage *newChildPage =
      insertHelper(origChildPage, key, rid, &newChildMidVal);

  // not split in child
  if (newChildPage == nullptr) return nullptr;

  PageId newChildPageId = newChildPage->page_number();  // TODO: change this

  // split in child, need to add splitted child to currNode
  int index = findInsertionIndexNonLeaf(origNode, key);
  if (!isNonLeafNodeFull(origNode)) {  // current node is not full
    insertToNonLeafNode(origNode, index, key, newChildPageId);
    origPage->setNode(origNode);
    return nullptr;
  }

  int middleIndex = (INTARRAYNONLEAFSIZE - 1) / 2;

  bool insertToLeft = index < middleIndex;

  // split
  int splitIndex = middleIndex + insertToLeft;
  int insertIndex = insertToLeft ? index : index - middleIndex;

  bool moveKeyUp = !insertToLeft && insertIndex == 0;  // insert to right[0]

  // if we need to move key up, set midVal = key, else key at splited index
  *midVal = moveKeyUp ? key : origNode->keyArray[splitIndex];

  NonLeafNodeInt *newNode = splitNonLeafNode(origNode, splitIndex, moveKeyUp);

  if (!moveKeyUp) {  // need to insert
    NonLeafNodeInt *node = insertToLeft ? origNode : newNode;
    insertToNonLeafNode(node, insertIndex, newChildMidVal, newChildPageId);
  }

  origPage->setNode(origNode);        // save node to orig page
  return createPageForNode(newNode);  // return new page
}
