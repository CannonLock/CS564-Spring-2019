//
// Created by Shawn Zhong on 2019-03-17.
//

#include "btree.h"
#include "filescan.h"

using namespace std;

const string getIndexName(const string &relationName, const int attrByteOffset) {
  ostringstream idxStr;
  idxStr << relationName << ',' << attrByteOffset;
  return idxStr.str();
}