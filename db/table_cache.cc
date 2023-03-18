// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

//Yuanguo:
//
//   +----------------+
//   |   TableCache   |   +------------------------------> +---------------------------+    +--> +---------------------------------------------------------------+
//   |                |   |                                |              Table        |    |    |                         BlockCache                            |
//   | +------------+ |   |                                +---------------------------+    |    +---------------------------------------------------------------+
//   | |TableAndFile| |   |                                | rep_->options.block_cache |    |    | +---------------+     +---------------+     +---------------+ |
//   | +------------+ |   |  +--> +----------------+ <-----|-rep_->file                |    |    | | Block         | ... | Block         | ... | Block         | |
//   | | table  ----|-|---+  |    |RandomAccessFile|       | rep_->cache_id            |    |    | +---------------+     +---------------+     +---------------+ |
//   | | file   ----|-|------+    +----------------+       | rep_->filter              |    |    | |data_          |     |data_          |     |data_          | |
//   | +------------+ |                                    | rep_->index_block --------|----+    | |size_          |     |size_          |     |size_          | |
//   |                |                                    +---------------------------+    ^    | |restart_offset_|     |restart_offset_|     |restart_offset_| |
//   |                |                                                                     ^    | +---------------+     +---------------+     +---------------+ |
//   |     ...        |                                                                     |    |                                                               |
//   |                |                                                                     |    | +---------------+     +---------------+     +---------------+ |
//   |                |                                                                     |    | | Block         | ... | Block         | ... | Block         | |
//   |                |   +------------------------------> +---------------------------+    |    | +---------------+     +---------------+     +---------------+ |
//   |                |   |                                |              Table        |    |    | |data_          |     |data_          |     |data_          | |
//   | +------------+ |   |                                +---------------------------+    |    | |size_          |     |size_          |     |size_          | |
//   | |TableAndFile| |   |                                | rep_->options.block_cache |    |    | |restart_offset_|     |restart_offset_|     |restart_offset_| |
//   | +------------+ |   |  +--> +----------------+ <-----|-rep_->file                |    |    | +---------------+     +---------------+     +---------------+ |
//   | | table  ----|-|---+  |    |RandomAccessFile|       | rep_->cache_id            |    |    |                                                               |
//   | | file   ----|-|------+    +----------------+       | rep_->filter              |    |    |                            ......                             |
//   | +------------+ |                                    | rep_->index_block --------|----+    +---------------------------------------------------------------+
//   |                |                                    +---------------------------+
//   |     ...        |
//   |                |
//   +----------------+
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  //Yuanguo: key是"encoded-file_number";
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      //Yuanguo:
      //   key          : "encoded-file_number";
      //   value        : tf;
      //   返回的handle : LRUHandle{key="encoded-file_number"; value=tf;};
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  //Yuanguo:
  //  - 上面通过调用 FindTable --> cache_->Insert(见LRUCache::Insert及其中注释)创建了一个Handle* handle;
  //  - 这个handle用完之后需要通过cache_->Release(handle)来释放；究竟handle是否被cache缓存了、有没有别的user通过Lookup持有了handle，都不用关心；
  //  - 所以，这里注册一个回调函数，在result(Iterator)析构的时候触发Release。Release之后，可能也还在cache中缓存，也可能不在。
  //对比: TableCache::Get()，用完立即Release(handle);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
