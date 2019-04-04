/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

#include "btreeHelper.cpp"

using namespace std;

namespace badgerdb {

/**
 * Constructor
 *
 * The constructor first checks if the specified index ﬁle exists.
 * And index ﬁle name is constructed by concatenating the relational name with
 * the offset of the attribute over which the index is built.
 *
 * If the index ﬁle exists, the ﬁle is opened. Else, a new index ﬁle is created.
 *
 * @param relationName The name of the relation on which to build the index.
 * @param outIndexName The name of the index file.
 * @param bufMgrIn The instance of the global buffer manager.
 * @param attrByteOffset The byte offset of the attribute in the tuple on which
 * to build the index.
 * @param attrType The data type of the attribute we are indexing.
 */
BTreeIndex::BTreeIndex(const string &relationName, string &outIndexName,
                       BufMgr *bufMgrIn, const int attrByteOffset,
                       const Datatype attrType)
    : attrByteOffset(attrByteOffset),
      bufMgr(bufMgrIn),
      attributeType(attrType) {
  outIndexName = getIndexName(relationName, attrByteOffset);

  relationName.copy(indexMetaInfo.relationName, 20, 0);
  indexMetaInfo.attrByteOffset = attrByteOffset;
  indexMetaInfo.attrType = attrType;

  file = new BlobFile(outIndexName, true);

  Page *newPage;
  bufMgr->allocPage(file, indexMetaInfo.rootPageNo, newPage);
  BlobPage *root = (BlobPage *)newPage;

  LeafNodeInt node{};
  root->setNode(&node);

  FileScan fscan(relationName, bufMgr);
  try {
    RecordId scanRid;
    while (1) {
      fscan.scanNext(scanRid);
      std::string recordStr = fscan.getRecord();
      const char *record = recordStr.c_str();
      int key = *((int *)(record + attrByteOffset));
      insertEntry(&key, scanRid);
    }
  } catch (EndOfFileException e) {
    std::cout << "Read all records" << std::endl;
  }
}

/**
 * Destructor
 *
 * Perform any cleanup that may be necessary, including
 *      clearing up any state variables,
 *      unpinning any B+ Tree pages that are pinned, and
 *      flushing the index file (by calling bufMgr->flushFile()).
 *
 * Note that this method does not delete the index file! But, deletion of the
 * file object is required, which will call the destructor of File class causing
 * the index file to be closed.
 */
BTreeIndex::~BTreeIndex() {
  bufMgr->flushFile(file);
  scanExecuting = false;
  delete file;
  //  free(file);
}

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in.
 *The insertion may cause splitting of leaf node. This splitting will require
 *addition of new leaf page number entry into the parent non-leaf, which may
 *in-turn get split. This may continue all the way upto the root causing the
 *root to get split. If root gets split, metapage needs to be changed
 *accordingly. Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char
 *string
 * @param rid			Record ID of a record whose entry is getting
 *inserted into the index.
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  int splittedPageMidval;
  PageId newPageId = insertHelper(indexMetaInfo.rootPageNo, *(int *)key, rid,
                                  &splittedPageMidval);

  if (newPageId == 0) return;

  PageId pid;
  Page *newPage;
  bufMgr->allocPage(file, pid, newPage);

  NonLeafNodeInt *newRoot = (NonLeafNodeInt *)calloc(1, sizeof(NonLeafNodeInt));
  newRoot->level = 0;
  newRoot->keyArray[0] = splittedPageMidval;
  newRoot->pageNoArray[0] = indexMetaInfo.rootPageNo;
  newRoot->pageNoArray[1] = newPageId;
  ((BlobPage *)newPage)->setNode(newRoot);

  indexMetaInfo.rootPageNo = pid;
}

/**
 *
 * This method is used to begin a filtered scan” of the index.
 *
 * For example, if the method is called using arguments (1,GT,100,LTE), then
 * the scan should seek all entries greater than 1 and less than or equal to
 * 100.
 *
 * @param lowValParm The low value to be tested.
 * @param lowOpParm The operation to be used in testing the low range.
 * @param highValParm The high value to be tested.
 * @param highOpParm The operation to be used in testing the high range.
 */
const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm) {
  if (lowOpParm != GT && lowOpParm != GTE) throw BadOpcodesException();
  if (highOpParm != LT && highOpParm != LTE) throw BadOpcodesException();

  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);
  if (lowValInt > highValInt) throw BadScanrangeException();

  lowOp = lowOpParm;
  highOp = highOpParm;

  scanExecuting = true;

  currentPageNum = getLeafPageIdByKey(indexMetaInfo.rootPageNo, lowValInt);
  bufMgr->readPage(file, currentPageNum, currentPageData);
  nextEntry = getEntryIndexByKey(currentPageNum, lowValInt);
  if (lowOp == GT) getNextEntry(currentPageNum, nextEntry);
}

/**
 * This method fetches the record id of the next tuple that matches the scan
 * criteria. If the scan has reached the end, then it should throw the
 * following exception: IndexScanCompletedException.
 *
 * For instance, if there are two data entries that need to be returned in a
 * scan, then the third call to scanNext must throw
 * IndexScanCompletedException. A leaf page that has been read into the buffer
 * pool for the purpose of scanning, should not be unpinned from buffer pool
 * unless all records from it are read or the scan has reached its end. Use
 * the right sibling page number value from the current leaf to move on to the
 * next leaf which holds successive key values for the scan.
 *
 * @param outRid An output value
 *               This is the record id of the next entry that matches the scan
 * filter set in startScan.
 */
const void BTreeIndex::scanNext(RecordId &outRid) {
  if (!scanExecuting) throw ScanNotInitializedException();
}

/**
 * This method terminates the current scan and unpins all the pages that have
 * been pinned for the purpose of the scan.
 *
 * It throws ScanNotInitializedException when called before a successful
 * startScan call.
 */
const void BTreeIndex::endScan() {
  if (!scanExecuting) throw ScanNotInitializedException();
  scanExecuting = false;
}

}  // namespace badgerdb
