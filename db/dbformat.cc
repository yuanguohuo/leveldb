// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

//Yuanguo: 注意比较的时候，不是直接按字节比较rep_，而是:
//    - 使用user的comparator去比较user_key部分，ascending;
//    - 若相等，按比较 "seq<<8|type" 部分，descending;
//          - 按seq的降序:              akey.seq > bkey.seq，返回-1;  akey.seq < bkey.seq，返回+1；
//          - 若相等，再按type的降序:   akey.type > bkey.type，返回-1;  akey.type < bkey.type，返回+1；
//  为什么按降序？是这样的：
//    - user调用 DBImpl::NewIterator()获取一个Iterator，其实是获取了一个DB的Snapshot; 
//    - Snapshot就是一个seq，例如998;
//    - 之后，DB有新的修改: seq = 999， 1000， 1001, ...；
//    - 但user不应该看到这些新的修改；他应该只能看到998以前的修改：998, 997, 996, ...
//    - 例如对于同一个user_key = foo; 有两个修改: foo:998, foo:1000，按降序
//            foo:1000在前，foo:998在后
//    - 所以user的Iterator Seek(foo:998)的时候，就直接跳过了foo:1000
//
//  type也是同一个道理：
//       foo:998:kTypeValue(1)在前，foo:998:kTypeDeletion(0)在后
//  使用 Seek(foo:998:kValueTypeForSeek(1))的时候，能够看到 foo:998:kTypeValue(1) 和 foo:998:kTypeDeletion(0)
//  问题：什么情况下，两个修改的seq相同？
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

// Yuanguo: return a shorter string `x` such that `*start <= x < limit`; thus
//   it is a valid implementation to return `*start` (keep it unchanged);
//   the implementation depends on user_comparator_->FindShortestSeparator;
void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.

    // Yuanguo:  
    //          +-------------------------------+--+--------------+
    //      low |     tmp                       |01|  MAX-SEQ     | high
    //          +-------------------------------+--+--------------+
    //                        user_key         type(1B)  seq(7B) 
    //
    // Yuanguo: because internal keys are sorted by 
    //              user_key ascending order;
    //              type|seq descending ordr;
    // this is the earliest/smallest internal key for user_key=tmp
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

//Yuanguo:
//           4字节                                                       1B       7B
//   +--------------------+--------------------------------------------+----+-----------------+
//   | "user_key的长度+8" |               user_key的数据               |type| seq(低字节在前) |
//   +--------------------+--------------------------------------------+----+-----------------+
//   ^    即后面总长度    ^                                                                   ^
//   |                    |                                                                   |
// start_              kstart_                                                               end_
//
//  注意：PackSequenceAndType() 生成的是： seq << 8 | type
//        EncodeFixed64() 是低位在前，所以最终type在最前；
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
