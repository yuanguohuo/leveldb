// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
//Yuanguo:
//
// 这里说的都是internal key，要经过DBIter的过滤才暴露给user；所以结合DBIter一起看。见db/db_iter.cc中DBIter::FindNextUserEntry()函数前的注释。
//
// MergingIterator归并多个子Iterator，对外就像一个Iterator一样。
//   1. comparator是InternelComparator，见DBImpl::NewIterator() -> DBImpl::NewInternalIterator() -> NewMergingIterator()
//         - 它定义的顺序是: 按user comparator的升序；按seqno的降序；
//         - 对于key(形如user_key:seqno), k2:4  k2:6  k2:3  k3:1  k3:2  k1:5，最终顺序是:
//                 k1:5  k2:6  k2:4  k2:3  k3:2  k3:1
//   2. children_是待合并的子Iterator; 是在DBImpl::NewIterator() -> DBImpl::NewInternalIterator()中创建的:
//         Memtable
//         Immutable-Memtable
//         Level0-Table0
//         Level0-Table1
//         ...
//         Level0-TableN
//         Level1    //见Version::AddIterators()->Version::NewConcatenatingIterator()创建的多层嵌套的iterator，对外表现为迭代一层内的所有kv;
//         Level2
//         ...
//         LevelM
//
//   3. 例子: 例如合并6个Iterator: I1, I2, ..., I6 (Level2以后和Level1一样，所以没有展示)；
//
//         - user创建DBIter的时候，sequence number 是 91；所以DBIter::sequence_ = 91;
//         - user目的是list k3以后的key；所以Seek(k3); 在DBIter::Seek()中，变成internal key "k3:91"
//         - 所以，对于本类来说，即Seek(k3:91);
//
//         Memtable(I1)          Immutable-Memtable(I2)
//              k3:95                  k2:83
//              k3:92                  k3:86
//              k3:90                  k4:85
//              k4:91                  k4:80
//              k5:96                  k5:82
//              k5:93                  k6:81
//              k7:94                  k8:84
//
//         Level0-Table(I3)      Level0-Table(I4)      Level0-Table(I5)
//             k2:72                 k4:65                 k1:56
//             k5:76                 k4:63                 k2:52
//             k5:70                 k4:60                 k3:54
//             k6:74                 k5:64                 k8:51
//             k7:73                 k5:61                 k9:55
//             k8:75                 k8:62                 k9:53
//             k9:71                 k9:66                 k9:50
//
//         Level1(I6)
//             k2:20                 k5:42                 k8:48
//             k3:32                 k6:41                 k8:27
//             k4:47                 k6:23                 k8:25
//             k4:33                 k7:39                 k9:43
//             k4:21                 k7:37                 k9:30
//
//    -----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
//    列号       |  A  |  B  |  C  |  D  |  E  |  F  |  G  |  H  |  I  |  J  |  K  |  L  |  M  |  N  |  O  |  P  |  Q  |  R  |  S  |  T  |  U  |  V  |
//    -----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
//    操作       |Seek |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |Next |
//    -----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
//    I1         |k3:90|k4:91|k4:91|k4:91|k4:91|k5:96|k5:96|k5:96|k5:96|k5:96|k5:96|k5:96|k5:96|k5:96|k5:93|k7:94|k7:94|k7:94|k7:94|k7:94|k7:94|k7:94|
//    I2         |k3:86|k3:86|k4:85|k4:85|k4:85|k4:85|k4:80|k5:82|k5:82|k5:82|k5:82|k5:82|k5:82|k5:82|k5:82|k5:82|k6:81|k6:81|k6:81|k6:81|k6:81|k6:81|
//    I3         |k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:76|k5:70|k6:74|k6:74|k6:74|k6:74|
//    I4         |k4:65|k4:65|k4:65|k4:65|k4:65|k4:65|k4:65|k4:65|k4:63|k4:60|k5:64|k5:64|k5:64|k5:64|k5:64|k5:64|k5:64|k5:64|k5:64|k5:61|k8:62|k8:62|
//    I5         |k3:54|k3:54|k3:54|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|k8:51|
//    I6         |k3:32|k3:32|k3:32|k3:32|k4:47|k4:47|k4:47|k4:47|k4:47|k4:47|k4:47|k4:33|k4:21|k5:42|k5:42|k5:42|k5:42|k5:42|k5:42|k5:42|k5:42|k6:41|
//    -----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
//    current_   |I1   |I2   |I5   |I6   |I1   |I2   |I2   |I4   |I4   |I4   |I6   |I6   |I6   |I1   |I1   |I2   |I3   |I3   |I4   |I4   |I6   | I2  |
//    -----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
//    结果       |k3:90|k3:86|k3:54|k3:32|k4:91|k4:85|k4:80|k4:65|k4:63|k4:60|k4:47|k4:33|k4:21|k5:96|k5:93|k5:82|k5:76|k5:70|k5:64|k5:61|k5:42|k6:81|
//    -----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
//
//    说明：
//        - Seek：Seek(k3:91)之后，I1指向k3:90，I2指向k3:86，I3指向k5:76，I4指向k4:65，I5指向k3:54，I6指向k3:32;
//          此时最小的是k3:90；所以，Seek() --> FindSmallest()选择的是I1，即此时MergingIterator的current_ = I1;
//          函数key()和value()返回的是I1指向的k3:90;
//          也就是说，Seek()返回之后，MergingIterator的状态就是A列；
//        - 第一次Next(): current_->Next()就是I1->Next()，使得I1向前移动一个entry，指向k4:91；
//          此时最小的是k3:86；所以，Next() --> FindSmallest()选择的是I2，即此时MergingIterator的current_ = I2;
//          函数key()和value()返回的是I2指向的k3:86;
//          也就是说，第一次Next()返回之后，MergingIterator的状态就是B列；
//        - 第二次Next(): current_->Next()就是I2->Next()，使得I2向前移动一个entry，指向k4:85；
//          此时最小的是k3:54；所以，Next() --> FindSmallest()选择的是I5，即此时MergingIterator的current_ = I5;
//          函数key()和value()返回的是I5指向的k3:54;
//          也就是说，第二次Next()返回之后，MergingIterator的状态就是C列；
//        - 依次类推；
//
//    重要：
//        1. 这里说的都是internal key，不是对user可见的key；
//        2. DBIter消费这些internal key，过滤之后呈现给user。继续上面的例子: MergingIterator吐出的key序列是：
//                    k3:90|k3:86|k3:54|k3:32|k4:91|k4:85|k4:80|k4:65|k4:63 ... k5:96|k5:93 ... k7:94 ...
//           显然，DBIter不能直接吐给user：
//               a. 过滤多版本，比如k3就有k3:90, k3:86, k3:54, k3:32。假如k3:90是kTypeValue，则只应吐出k3:90，后面的都要跳过去。见DBIter::FindNextUserEntry()前的注释。
//               b. (其实是a的一个特例) 假如k3:90不是kTypeValue而是kTypeDeletion，则k3全都要跳过去。见DBIter::Seek() -> DBIter::FindNextUserEntry()。
//                       - 设置 skipping = true; *skip = k3;
//                       - 跳过：skipping && user_key <= k3，即k3打头的所有；
//                  可见，DBIter::Seek()的时候都可能要跳过很多internal key，导致耗时。
//               c. snapshot与可见性：user创建的iterator其实是LevelDB的一个snapshot，即seqno=91那一刻的snapshot。之后，Memtable还有写入，所以会
//                  出现seqno>91的。但seqno>91的key应该不可见，MergingIterator吐出的
//                      k5:96|k5:93 ... k7:94 ...
//                  都应该被DBIter过滤掉。见DBIter::FindNextUserEntry()中的：
//                      if (ParseKey(&ikey) && ikey.sequence <= sequence_) { ... }
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  //Yuanguo: 构造之后，不seek (Seek, SeekToFirst, SeekToLast)，是invalid的状态。
  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
