// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  //Yuanguo:                level, file-number
  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;
  //Yuanguo: 一个log/wal文件对应的memtable已经持久化成table file之后，这个log/wal文件就没有用了；
  //  log_number_记录的是最小的有用的log/wal；也就是说，log_number_-1以及之前的，都无用了；
  //  所以，这里记录并持久化log_number_，相当于 truncate-log 操作。
  //
  //  重要：truncate-log要和"对持久化的table file的引用"作为一个原子被持久化；
  //     - 持久化table file之后，立即崩溃也无所谓：这时log_number_没有更新，也没有任何Version/VersionEdit引用持久化的table file；
  //       所以，table file是一个无用的文件。重启之后，重新replay log，生成memtabe，再持久化成新的table file；
  //     - 而一旦 log_number_ 和 "对持久化的table file的引用" 被原子地持久化，崩溃再重启时，就不replay truncate的log/wal，其对应的内容
  //       已经在持久化的table file中。
  //     - 本类(VersionEdit)就是原子地持久化log_number_ 和 "对持久化的table file的引用"
  //              - log_number_在这里;
  //              - "对持久化的table file的引用"在new_files_中；
  uint64_t log_number_;
  //Yuanguo: 新版的LevelDB不再使用prev_log_number_，见DBImpl::Recover()中的注释。
  uint64_t prev_log_number_;
  //Yuanguo:
  //  next_file_number_是文件号分配器。
  //    - manifest文件、log/wal文件、table file的文件号，都是通过递增这个计数器来分配的。
  //    - 这个计数器，运行时存在于VersionSet::next_file_number_中。它总是在产生新Version的时候递增。
  //      产生新Version，也必然要产生并持久化VersionEdit, 自然，把它存到VersionEdit中并持久化；
  //    - crash/stop之后重启时，VersionSet::Recover()函数再把它加载到VersionSet::next_file_number_中。
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
