//
// Created by Shawn Zhong on 2019-03-17.
//

#include "btree.h"
#include "filescan.h"

using namespace std;
using namespace badgerdb;

/** Generic Helper*/

bool isLeaf(Page *page) {
  void *node = ((BlobPage *)page)->getNode();
  return *((int *)node) == -1;
}

const string getIndexName(const string &relationName,
                          const int attrByteOffset) {
  ostringstream idxStr;
  idxStr << relationName << ',' << attrByteOffset;
  return idxStr.str();
}

int findIndex(const int arr[], int len, int key, bool includeCurrentKey) {
  if (includeCurrentKey) {
    for (int i = 0; i < len; i++)
      if (arr[i] >= key) return i;
  } else {
    for (int i = 0; i < len; i++)
      if (arr[i] > key) return i;
  }
  return -1;
}

int getLeafLen(LeafNodeInt *node) {
  int len = INTARRAYLEAFSIZE;
  RecordId emptyRid{};
  for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
    if (node->ridArray[i] == emptyRid) {
      len = i;
      break;
    }
  }
  return len;
}

int getNonLeafLen(NonLeafNodeInt *node) {
  int len = INTARRAYNONLEAFSIZE + 1;
  for (int i = 1; i <= INTARRAYNONLEAFSIZE; i++) {
    if (node->pageNoArray[i] == 0) {
      len = i;
      break;
    }
  }
  return len;
}

/**
 * return the largest index if not found
 */
int findIndexNonLeaf(NonLeafNodeInt *node, int key) {
  int len = getNonLeafLen(node);
  int result = findIndex(node->keyArray, len - 1, key, true);
  return result == -1 ? len - 1 : result;
}

/**
 * return next insertion index if not found
 */
int findInsertionIndexLeaf(LeafNodeInt *node, int key) {
  int len = getLeafLen(node);
  int result = findIndex(node->keyArray, len, key, true);
  return result == -1 ? len : result;
}

/**
 * return -1 if not found
 */
int findScanIndexLeaf(LeafNodeInt *node, int key, bool includeCurrentKey) {
  int len = getLeafLen(node);
  return findIndex(node->keyArray, len, key, includeCurrentKey);
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

PageId BTreeIndex::createPageForNode(void *node) {
  PageId pid;
  Page *page;
  BTreeIndex::bufMgr->allocPage(BTreeIndex::file, pid, page);
  page->set_page_number(pid);
  ((BlobPage *)page)->setNode(node);
  bufMgr->unPinPage(file, pid, true);
  return pid;
}

BlobPage *BTreeIndex::getBlogPageByPid(PageId pid) {
  Page *ret = nullptr;
  bufMgr->readPage(file, pid, ret);
  return (BlobPage *)ret;
}

/*******************************  Insert  *******************************/

PageId BTreeIndex::insertToLeafPage(BlobPage *origPage, PageId origPageId,
                                    const int key, const RecordId &rid,
                                    int *midVal) {
  LeafNodeInt *origNode = (struct LeafNodeInt *)origPage->getNode();
  int index = findInsertionIndexLeaf(origNode, key);

  if (!isLeafNodeFull(origNode)) {
    insertToLeafNode(origNode, index, key, rid);
    origPage->setNode(origNode);
    bufMgr->unPinPage(file, origPageId, true);
    return 0;
  }

  int middleIndex = INTARRAYLEAFSIZE / 2;
  bool insertToLeft = index < middleIndex;
  LeafNodeInt *newNode = splitLeafNode(origNode, middleIndex + insertToLeft);

  if (insertToLeft)
    insertToLeafNode(origNode, index, key, rid);
  else
    insertToLeafNode(newNode, index - middleIndex, key, rid);

  PageId newPageId = createPageForNode(newNode);
  BlobPage *newPage = getBlogPageByPid(newPageId);

  newNode->rightSibPageNo = origNode->rightSibPageNo;
  origNode->rightSibPageNo = newPageId;

  origPage->setNode(origNode);
  newPage->setNode(newNode);
  *midVal = newNode->keyArray[0];

  bufMgr->unPinPage(file, origPageId, true);
  bufMgr->unPinPage(file, newPageId, true);

  return newPageId;
}

PageId BTreeIndex::insertHelper(PageId origPageId, const int key,
                                const RecordId rid, int *midVal) {
  BlobPage *origPage = getBlogPageByPid(origPageId);

  if (isLeaf(origPage))  // base case
    return insertToLeafPage(origPage, origPageId, key, rid, midVal);

  NonLeafNodeInt *origNode = (NonLeafNodeInt *)origPage->getNode();

  PageId origChildPageId =
      origNode->pageNoArray[findIndexNonLeaf(origNode, key)];

  // insert key, rid to child and check whether child is splitted
  int newChildMidVal;
  PageId newChildPageId =
      insertHelper(origChildPageId, key, rid, &newChildMidVal);

  // not split in child
  if (newChildPageId == 0) {
    bufMgr->unPinPage(file, origPageId, false);
    return 0;
  }

  // split in child, need to add splitted child to currNode
  int index = findIndexNonLeaf(origNode, newChildMidVal);
  if (!isNonLeafNodeFull(origNode)) {  // current node is not full
    insertToNonLeafNode(origNode, index, newChildMidVal, newChildPageId);
    origPage->setNode(origNode);
    bufMgr->unPinPage(file, origPageId, true);
    return 0;
  }

  int middleIndex = (INTARRAYNONLEAFSIZE - 1) / 2;

  bool insertToLeft = index < middleIndex;

  // split
  int splitIndex = middleIndex + insertToLeft;
  int insertIndex = insertToLeft ? index : index - middleIndex;

  bool moveKeyUp = !insertToLeft && insertIndex == 0;  // insert to right[0]

  // if we need to move key up, set midVal = key, else key at splited index
  *midVal = moveKeyUp ? newChildMidVal : origNode->keyArray[splitIndex];

  NonLeafNodeInt *newNode = splitNonLeafNode(origNode, splitIndex, moveKeyUp);

  if (!moveKeyUp) {  // need to insert
    NonLeafNodeInt *node = insertToLeft ? origNode : newNode;
    insertToNonLeafNode(node, insertIndex, newChildMidVal, newChildPageId);
  }

  origPage->setNode(origNode);  // save node to orig page
  bufMgr->unPinPage(file, origPageId, true);
  return createPageForNode(newNode);  // return new page
}

/*******************************  Scan  *******************************/

void BTreeIndex::initPageId() {
  currentPageData = getBlogPageByPid(currentPageNum);
  if (isLeaf(currentPageData)) return;

  NonLeafNodeInt *node =
      (NonLeafNodeInt *)((BlobPage *)currentPageData)->getNode();

  bufMgr->unPinPage(file, currentPageNum, false);
  currentPageNum = node->pageNoArray[findIndexNonLeaf(node, lowValInt)];
  initPageId();
}

void BTreeIndex::initEntryIndex() {
  LeafNodeInt *node = (LeafNodeInt *)((BlobPage *)currentPageData)->getNode();
  int entryIndex = findScanIndexLeaf(node, lowValInt, lowOp == GTE);
  if (-1 == entryIndex) {
    setNextEntry();
    return;
  }
  nextEntry = entryIndex;
}

void BTreeIndex::setNextEntry() {
  nextEntry++;
  LeafNodeInt *node = (LeafNodeInt *)((BlobPage *)currentPageData)->getNode();
  if (nextEntry >= INTARRAYLEAFSIZE ||
      node->ridArray[nextEntry].page_number == 0) {
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = node->rightSibPageNo;
    currentPageData = getBlogPageByPid(currentPageNum);
    nextEntry = 0;
  }
}