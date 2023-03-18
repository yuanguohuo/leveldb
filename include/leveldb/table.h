// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class LEVELDB_EXPORT Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Table** table);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  friend class TableCache;
  struct Rep;

  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  explicit Table(Rep* rep) : rep_(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  //Yuanguo:
  //  - Table::InternalGet()找"第一个>=k且和k有着相同user-key的kv-pair"，若找到，则对其调用handle_result；若找不到满足的，
  //    就什么也不做。
  //  - 但由于false-positive的作用，也可能找到user-key>k的kv-pair (只满足第一个条件，不满足第二个条件)，并对其调用handle_result;
  //  - 所以，handle_result必须能够甄别并处理false-positive;
  //
  //  以k=k08:99为例。本意是找: 第一个k08:x(x<99)。但由于以下两个原因，它做不到:
  //        1. filter(我们只假设使用BloomFilterPolicy)具有false-positive的属性。也就是说，本来希望找第一个k08:x(x<99)，但有一
  //           定的概率找到"k09:*", k10:*, ...
  //        2. 没有使用filter：只要找到k08:x(x<99), k09:*, k10:*, ..., 就都算找到。
  //  可以认为"没有使用filter"是使用了一种false-positive率为100%的filter；所以，统一认为是false-positive导致的。
  //
  //  总之，Table::InternalGet对外表现的行为是：在table中找"第一个>=k的kv-pair"，不一定和k有相同的user-key，然后对其调用handle_result。
  //  为了保证正确性，handle_result必须能够甄别并处理false-positive。
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v));

  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);

  Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
