/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

#include "btreeHelper.cpp"

using namespace std;


namespace badgerdb {
    /**
     * Constructor
     * @param relationName The name of the relation on which to build the index.
     * @param outIndexName The name of the index ﬁle.
     * @param bufMgrIn The instance of the global buffer manager.
     * @param attrByteOffset The byte offset of the attribute in the tuple on which to build the index.
     * @param attrType The data type of the attribute we are indexing.
     */
    BTreeIndex::BTreeIndex(const string &relationName, string &outIndexName, BufMgr *bufMgrIn, const int attrByteOffset,
                           const Datatype attrType) : attrByteOffset(attrByteOffset), bufMgr(bufMgrIn),
                                                      attributeType(attrType) {

        outIndexName = getIndexName(relationName, attrByteOffset);
    }


    /**
     * Destructor
     *
     * Perform any cleanup that may be necessary, including
     *      clearing up any state variables,
     *      unpinning any B+ Tree pages that are pinned, and
     *      ﬂushing the index ﬁle (by calling bufMgr->flushFile()).
     *
     * Note that this method does not delete the index ﬁle! But, deletion of the ﬁle object is required,
     * which will call the destructor of File class causing the index ﬁle to be closed.
     */
    BTreeIndex::~BTreeIndex() {
        delete file;
    }

    /**
     *
     * This method inserts a new entry into the index using the pair <key, rid>.
     *
     * @param key A pointer to the value (integer) we want to insert.
     * @param rid The corresponding record id of the tuple in the base relation.
     */
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {

    }

    /**
     *
     * This method is used to begin a filtered scan” of the index.
     *
     * For example, if the method is called using arguments (1,GT,100,LTE), then
     * the scan should seek all entries greater than 1 and less than or equal to 100.
     *
     * @param lowValParm The low value to be tested.
     * @param lowOpParm The operation to be used in testing the low range.
     * @param highValParm The high value to be tested.
     * @param highOpParm The operation to be used in testing the high range.
     */
    const void BTreeIndex::startScan(const void *lowValParm, const Operator lowOpParm, const void *highValParm,
                                     const Operator highOpParm) {

        if (lowOpParm != GT && lowOpParm != GTE) throw BadOpcodesException();
        if (highOpParm != LT && highOpParm != LTE) throw BadOpcodesException();
        if (lowValParm > highValParm) throw BadScanrangeException();

        lowValInt = *((int *) lowValParm);
        highValInt = *((int *) highValParm);
        lowOp = lowOpParm;
        highOp = highOpParm;


        scanExecuting = true;

    }

    /**
     * This method fetches the record id of the next tuple that matches the scan criteria.
     * If the scan has reached the end, then it should throw the following exception: IndexScanCompletedException.
     *
     * For instance, if there are two data entries that need to be returned in a scan, then the third call to scanNext
     * must throw IndexScanCompletedException. A leaf page that has been read into the buffer pool for the purpose of
     * scanning, should not be unpinned from buffer pool unless all records from it are read or the scan has reached
     * its end. Use the right sibling page number value from the current leaf to move on to the next leaf which holds
     * successive key values for the scan.
     *
     * @param outRid An output value
     *               This is the record id of the next entry that matches the scan filter set in startScan.
     */
    const void BTreeIndex::scanNext(RecordId &outRid) {
        if (!scanExecuting) throw ScanNotInitializedException();

    }

    /**
     * This method terminates the current scan and unpins all the pages that have been pinned for the purpose of the scan.
     *
     * It throws ScanNotInitializedException when called before a successful startScan call.
     */
    const void BTreeIndex::endScan() {
        if (!scanExecuting) throw ScanNotInitializedException();

    }

}
