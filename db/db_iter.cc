// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBIter(const DBIter&) = delete;
  DBIter& operator=(const DBIter&) = delete;

  ~DBIter() override { delete iter_; }
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  size_t RandomCompactionPeriod() {
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_;
  size_t bytes_until_read_sampling_;
};

// Yuanguo: bytes_until_read_sampling_ = 1.5MB initially,
//    whenever `bytes_read` amount of data is read, bytes_until_read_sampling_ -= bytes_read;
//    if bytes_until_read_sampling_ falls low enough, 
//        bytes_until_read_sampling_ is recharged, e.g. bytes_until_read_sampling_ += 1.2MB
//        RecordReadSample is called;
inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();

  size_t bytes_read = k.size() + iter_->value().size();
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

//Yuanguo: 见DBIter::FindNextUserEntry()前面的注释；
void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  FindNextUserEntry(true, &saved_key_);
}

//Yuanguo:
//
//  重要!!! 先看table/merger.cc中class MergingIterator前的注释。如那里所述，本函数用来过滤internal key，把不可见的隐藏掉:
//      a. 过滤多版本: 一个key有多个版本，则只暴露最新的(当然是snapshot的seqno之前的最新的)；
//      b. (其实是a的一个特例) 假如一个key的第一个版本是kTypeDeletion，则跳过该key的所有版本；
//      c. snapshot与可见性；snapshot其实就是一个seqno；只能暴露 <= seqno 的kv；
//
//  假如
//    - iter_ 输出的序列是：k3:90|k3:86|k3:42|k4:91|k4:85|k4:80|k4:65|k4:63 ... k5:96|k5:93 ... k7:94 ...;
//    - sequence_ = 91;
//
//  模拟从k3开始向前迭代(direction_ = kForward):
//
//  假如k3:90的type是kTypeValue:
//      1. DBIter::Seek()
//          - 调用iter_->Seek(k3:91)之后，iter_指向k3:90;
//          - 调用FindNextUserEntry()，传入skipping=false，因为user还没有消费current kv，不能跳过。
//                - k3:90满足ikey.sequence <= sequence_，匹配case kTypeValue，立即返回。
//                - 此时DBIter的current是k3:90对应的kv；
//      2. user调用DBIter::key()/value()来消费k3:90对应的kv;
//      3. user调用DBIter::Next()
//          - SaveKey(...)，即saved_key_ = k3;
//          - iter_->Next(); iter_指向k3:86；
//          - 调用FindNextUserEntry()，传入skipping=true; *skip=saved_key_=k3；表示跳过k3的所有版本，因为k3已经被user消费了。
//                - do-while循环跳过 k3:86, k3:42，停在k4:91上，返回。
//      4. user调用DBIter::key()/value()来消费k4:91对应的kv;
//      ......
//      5. k5:96, k5:93, k7:94时，因不满足ikey.sequence <= sequence_被跳过(snapshot与可见性).
//
//  假如k3:90的type是kTypeDeletion:
//      1. DBIter::Seek()
//          - 调用iter_->Seek(k3:91)之后，iter_指向k3:90;
//          - 调用FindNextUserEntry()，传入skipping=false，因为user还没有消费current kv，不能跳过。
//                - k3:90满足ikey.sequence <= sequence_，匹配case kTypeDeletion，故设置skipping=true; *skip=k3;
//                - do-while循环跳过k3的所有版本：k3:90, k3:86, k3:42;
//                - 遇见k4:91，假如k4:91的type又是kTypeDeletion，那么do-while循环要继续运行，跳过k4的所有版本。
//                ...
//
//                重要!!! 所以，对于压缩不及时的LevelDB库，Seek操作有可能需要跳过很多internal key，最终停在一个user可见的key上。例如，
//                用LevelDB记录待处理的gc操作：
//                     obj1          (type=kTypeValue)
//                     obj2          (type=kTypeValue)
//                     ...
//                     obj1000000000 (type=kTypeValue)
//
//                gc运行之后，就把这些删除，压缩之前，LevelDB就是这样的：
//                     obj1          (type=kTypeDeletion)
//                     obj1          (type=kTypeValue)
//                     obj2          (type=kTypeDeletion)
//                     obj2          (type=kTypeValue)
//                     ...
//                     obj1000000000 (type=kTypeDeletion)
//                     obj1000000000 (type=kTypeValue)
//                此时调用DBIter::Seek()，就会在FindNextUserEntry()中循环跳过所有key。
//
//  Yuanguo: 验证：
//      在 table/merger.cc 中加一些代码：在Seek()和Next()函数的结尾处，打印key()的内容，打印格式是:
//           {user_key}:{seqno}{kTypeValue|kTypeDeletion}
//
//      测试程序:
//           #include <iostream>
//           #include <string>
//           #include <leveldb/db.h>
//           
//           #include <glog/logging.h>
//           
//           int main(int argc, char** argv)
//           {
//             google::InitGoogleLogging("leveldbtest");
//           
//             if (argc != 3) {
//               std::cout << "Error: invalid arguments" << std::endl;
//               std::cout << "Usage: " << argv[0] << " {seek-and-list: 0|1} {value}" << std::endl;
//               return 1;
//             }
//           
//             bool seek_list = argv[1][0] == '1';
//           
//             leveldb::DB* db;
//             leveldb::Options opts{};
//             opts.create_if_missing = true;
//             leveldb::Status s;
//           
//             s = leveldb::DB::Open(opts, "myleveldb",&db);
//             if (!s.ok()) {
//               std::cout << "failed to create leveldb. error: " << s.ToString() << std::endl;
//               return 1;
//             }
//           
//             leveldb::Iterator* itr = nullptr;
//             if (seek_list) {
//               LOG(ERROR) << "-------------------- new iterator --------------------";
//               itr = db->NewIterator(leveldb::ReadOptions());
//             }
//           
//             leveldb::WriteOptions wopts;
//             for (int i=0; i<8; ++i) {
//               std::string key = "test-key-" + std::to_string(i);
//               std::string val(argv[2]);
//               s = db->Put(wopts, leveldb::Slice(key), leveldb::Slice(val));
//               if (!s.ok()) {
//                 std::cout << "failed to put. error: " << s.ToString() << std::endl;
//                 return 1;
//               }
//             }
//           
//             if (seek_list) {
//               LOG(ERROR) << "-------------------- seek --------------------";
//               itr->Seek("test-key-3");
//           
//               LOG(ERROR) << "-------------------- next --------------------";
//               for(; itr->Valid(); itr->Next()) {
//                 std::cout << itr->key().ToString() << " => " << itr->value().ToString() << std::endl;
//               }
//               delete itr;
//             }
//           
//             delete db;
//             return 0;
//           }
//
//      运行:
//           a.out
//           $ ./a.out 0 AA
//           $ ./a.out 0 BB
//           $ ./a.out 1 CC
//           E0422 15:23:52.373059 16447 main.cpp:32] -------------------- new iterator --------------------
//           E0422 15:23:52.373416 16447 main.cpp:48] -------------------- seek --------------------
//           Seek valid at test-key-3:12:kTypeValue
//           E0422 15:23:52.373461 16447 main.cpp:51] -------------------- next --------------------
//           test-key-3 => BB
//           Next valid at test-key-3:4:kTypeValue
//           Next valid at test-key-4:21:kTypeValue
//           Next valid at test-key-4:13:kTypeValue
//           test-key-4 => BB
//           Next valid at test-key-4:5:kTypeValue
//           Next valid at test-key-5:22:kTypeValue
//           Next valid at test-key-5:14:kTypeValue
//           test-key-5 => BB
//           Next valid at test-key-5:6:kTypeValue
//           Next valid at test-key-6:23:kTypeValue
//           Next valid at test-key-6:15:kTypeValue
//           test-key-6 => BB
//           Next valid at test-key-6:7:kTypeValue
//           Next valid at test-key-7:24:kTypeValue
//           Next valid at test-key-7:16:kTypeValue
//           test-key-7 => BB
//           Next valid at test-key-7:8:kTypeValue
//           Next invalid
//
//      说明:
//
//           MemTable (I1):
//           test-key-0:17:kTypeValue CC
//           test-key-1:18:kTypeValue CC
//           test-key-2:19:kTypeValue CC
//           test-key-3:20:kTypeValue CC
//           test-key-4:21:kTypeValue CC
//           test-key-5:22:kTypeValue CC
//           test-key-6:23:kTypeValue CC
//           test-key-7:24:kTypeValue CC
//
//           Level0-Table2 (I2)             Level0-Table1 (I3)
//           test-key-0:9 :kTypeValue BB    test-key-0:1:kTypeValue AA
//           test-key-1:10:kTypeValue BB    test-key-1:2:kTypeValue AA
//           test-key-2:11:kTypeValue BB    test-key-2:3:kTypeValue AA
//           test-key-3:12:kTypeValue BB    test-key-3:4:kTypeValue AA
//           test-key-4:13:kTypeValue BB    test-key-4:5:kTypeValue AA
//           test-key-5:14:kTypeValue BB    test-key-5:6:kTypeValue AA
//           test-key-6:15:kTypeValue BB    test-key-6:7:kTypeValue AA
//           test-key-7:16:kTypeValue BB    test-key-7:8:kTypeValue AA
//
//           - NewIterator的时候，系统的sequence number是16。
//           - 之后，又继续Put (值为"CC")；
//           - itr->Seek("test-key-3")
//                       --> iter_::Seek("test-key-3:16") (即MergingIterator::Seek())
//                                I1指向 test-key-4:21:kTypeValue  注意：test-key-3:20:kTypeValue 排在 "test-key-3:16" 之前，所以被直接调过；
//                                I2指向 test-key-3:12:kTypeValue
//                                I3指向 test-key-3:4:kTypeValue
//           - 所以，iter_ (MergingIterator) 输出的序列是:
//                                test-key-3:12:kTypeValue BB    --> 当前版本，输出
//                                test-key-3:4:kTypeValue  AA    --> 过时版本
//                                test-key-4:21:kTypeValue CC    --> 在snapshot (iterator) 之后的版本
//                                test-key-4:13:kTypeValue BB
//                                test-key-4:5:kTypeValue  AA
//                                test-key-5:22:kTypeValue CC
//                                test-key-5:14:kTypeValue BB
//                                test-key-5:6:kTypeValue  AA
//                                test-key-6:23:kTypeValue CC
//                                test-key-6:15:kTypeValue BB
//                                test-key-6:7:kTypeValue  AA
//                                test-key-7:24:kTypeValue CC
//                                test-key-7:16:kTypeValue BB
//                                test-key-7:8:kTypeValue  AA
//            - 经过DBIter的过滤，输出:
//                                test-key-3 => BB
//                                test-key-4 => BB
//                                test-key-5 => BB
//                                test-key-6 => BB
//                                test-key-7 => BB
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          //Yuanguo: break的是switch，do-while循环继续；
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
          0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

//Yuanguo: 见DBIter::FindNextUserEntry()前面的注释；
void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(&saved_key_,
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
