#include "btreeHelper.cpp"

// Insert 12:
// [      |1|     |10|      |20|          |30|      |35} ]
//   /       /          |           \            |
//  []      []   [10, 11, 13, 15]   []          []

// [    |1|     |10|   |12|   |20|          |30|      ]
//                   |     |
//        [10, 11, _, _]->[12, 13, 15, _]

// Expected:
//              [    |12|     ]
//               /          |
// [  |1|  |10|   ]        [    |20|      |30|      ]
//              |             |
//            [10, 11] -> [12, 13, 15]

void printLeaf(LeafNodeInt *node) {
  cout << "Node level: " << node->level << " | Right Pg #： "
       << node->rightSibPageNo << endl
       << "Key Array: ";
  for (int i = 0; i < 7; i++) {
    // if (node->keyArray[i] != 0)
    cout << node->keyArray[i] << " | ";
  }
  cout << endl << "Rid Array: " << endl;
  for (int i = 0; i < 7; i++) {
    RecordId *record = &node->ridArray[i];
    // if (record->page_number != 0)
    cout << record->page_number << "|" << record->slot_number << " | ";
  }
  cout << endl;
}

void printNonLeaf(NonLeafNodeInt *a) {
  cout << a->level << endl << " |-" << a->pageNoArray[0] << "-| ";
  for (int i = 0; i < 7; i++)
    // if (a->keyArray[i] != 0)
    cout << a->keyArray[i] << " |-" << a->pageNoArray[i + 1] << "-| ";
  cout << endl;
}

int main(int argc, char *argv[]) {
  // cout << getIndexName("adasd", 5) << endl;
  // cout << findInsertionIndex(nullptr, 1) << &"123" << endl << "123" <<
  // endl;

  // NonLeafNodeInt tmp1 = {};
  // LeafNodeInt tmp2 = {};
  // tmp1.pageNoArray[0] = 1;
  // for (int i = 0; i <= 100; i++) {
  //   tmp1.keyArray[i] = 2 * (i + 1);
  //   tmp1.pageNoArray[i + 1] = 4 * (i + 1);
  //   tmp2.keyArray[i] = 2 * (i + 1);
  //   tmp2.ridArray[i].page_number = 3 * (i + 1);
  //   tmp2.ridArray[i].slot_number = 4 * (i + 1);
  // }

  // RecordId tmp = {2, 3};
  // insertToNonLeafNode(&tmp1, findInsertionIndexNonLeaf(&tmp1, 33), 33,
  //   233);
  // printNonLeaf(&tmp1);
  // cout << findInsertionIndexLeaf(&tmp2, 400) << " | "
  //  << findInsertionIndexNonLeaf(&tmp1, 400) << endl;

  //   NonLeafNodeInt t1{};
  //   t1.level = 4;
  //   for (int i = 0; i < 7; i++) {
  //     t1.keyArray[i] = (i + 1) * 2;
  //     t1.pageNoArray[i] = (i + 1);
  //   }
  //   t1.pageNoArray[7] = 8;
  //   printNonLeaf(&t1);
  //   NonLeafNodeInt *t2 = splitNonLeafNode(&t1, 3, true);
  //   printNonLeaf(&t1);
  //   printNonLeaf(t2);

  //   cout << "  " << endl;

  //   t1.level = 4;
  //   for (int i = 0; i < 7; i++) {
  //     t1.keyArray[i] = (i + 1) * 2;
  //     t1.pageNoArray[i] = (i + 1);
  //   }
  //   t1.pageNoArray[7] = 8;
  //   printNonLeaf(&t1);
  //   t2 = splitNonLeafNode(&t1, 3, false);
  //   printNonLeaf(&t1);
  //   printNonLeaf(t2);

  //   LeafNodeInt t1{};
  //   for (int i = 0; i < 7; i++) {
  //     t1.keyArray[i] = (i + 1) * 2;
  //     t1.ridArray[i].page_number = 5;
  //     t1.ridArray[i].slot_number = i + 1;
  //   }
  //   printLeaf(&t1);
  //   LeafNodeInt *t2 = splitLeafNode(&t1, 3);
  //   printLeaf(&t1);
  //   printLeaf(t2);
  //   return 0;
}