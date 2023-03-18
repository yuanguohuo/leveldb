// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  // Yuanguo:
  //   假如有key-x:997, key-x:995，能把key-x:995删除(覆盖)吗？不一定。
  //       - 假如有人持有snapshot 996，那么他看到的key-x的"最新"版本就是key-x:995，所以不能删除key-x:995；
  //       - 假如所有人持有的最小snapshot是997及以上，那么key-x:995, key-y:996, ... 等都可以删除。
  //   smallest_snapshot的作用就在于此。
  //       - DBImpl::DoCompactionWork()中初始化它；
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

//Yuanguo:
//  1. this->internal_comparator_: 假如user创建db的时候，没有指定raw_options.comparator，那么raw_options.comparator将是
//     BytewiseComparator(按字节比较)，见Options::Options()。
//     注意，不是把BytewiseComparator直接赋值给this->internal_comparator_；this->internal_comparator_是利用BytewiseComparator
//     构造的InternalKeyComparator对象：
//         user-key部分使用BytewiseComparator比较(ascending);
//         seq部分按降序比较是InternalKeyComparator实现的(descending);
//     在SanitizeOptions中，这个comparator被赋值给 result.comparator，即赋值给this->options_.comparator;
//  2. filter_policy: 比较绕。
//     假如user创建db的时候，没有指定raw_options.filter_policy，即raw_options.filter_policy = nullptr;
//         - 首先，this->internal_filter_policy_是使用nullptr构造的InternalFilterPolicy对象(不是直接赋值).
//           显然，这个对象不能使用，因为它没有判断是否为nullptr，直接使用user_policy_指针，肯定会crash；
//         - 好在，this->internal_filter_policy_从来没有被直接使用。是这样的：
//               - 在SanitizeOptions函数中，判断src.filter_policy(就是raw_options.filter_policy)，因为是nullptr，所以
//                 result.filter_policy (即this->options_.filter_policy) = nullptr;
//               - 以后都用this->options_.filter_policy(使用它的时候都判断是否为nullptr);
//     相反，假如user创建db的时候，指定raw_options.filter_policy = foo_user_policy (不为nullptr);
//         - this->internal_filter_policy_是使用foo_user_policy构造的InternalFilterPolicy对象；这是一个可用的对象。
//         - 在SanitizeOptions函数中，判断src.filter_policy(就是raw_options.filter_policy)，因为不是nullptr，所以
//              result.filter_policy (即this->options_.filter_policy) = ipolicy = &this->internal_filter_policy_；
//     所以，以后要使用filter_policy，一定要使用this->options_.filter_policy，不要直接使用this->internal_filter_policy_；
//  3. block_cache: 也很绕。
//     假如user创建db的时候，没有指定raw_options.block_cache，即raw_options.block_cache = nullptr;
//         - 在SanitizeOptions函数中，result.block_cache (即this->options_.block_cache) = 新建的8MB的cache；
//           注意src.block_cache(即raw_options.block_cache)还是nullptr;
//         - 所以，this->owns_cache_ = true; 表示DBImpl own这个cache，析构的时候要销毁。
//     相反，假如user创建db的时候，指定raw_options.block_cache = foo_user_cache (不为nullptr);
//         - 在SanitizeOptions函数中，result = src，即this->options_.block_cache = raw_options.block_cache = foo_user_cache;
//         - 所以，this->owns_cache_ = false; 表示DBImpl引用外部的cache，析构的时候要不能销毁。这用于leveldb嵌入别的程序的场景，
//           比如leveldb被作为一个库嵌入FooProject；FooProject创建了一个cache，传入leveldb。
//     无论哪种情况，block_cache都是全局唯一的，要通过this->options_.block_cache使用它。
//  4. info_log：和block_cache一样。
//     假如user创建db的时候，没有指定raw_options.info_log，即raw_options.info_log = nullptr;
//         - this->options_.info_log = 新建{dbname}/LOG;
//         - this->owns_info_log_ = true;
//     相反，假如user创建db的时候，指定raw_options.info_log = foo_log (不为nullptr);
//         - this->options_.info_log = foo_log;
//         - this->owns_info_log_ = false; 这用于leveldb嵌入别的程序的场景。
//  5. this->table_cache_：和block_cache一样，table_cache_也是全局唯一的。但DBImpl一定own它。
//     this->table_cache_缓存table，table的bocks又缓存在block_cache里。
//         - table_cache_构造的时候，拷贝了this->options_；
//         - TableCache::FindTable()调用Table::Open()构造table; table也拷贝了this->options_ (在table.rep_->options);
//         - Table::BlockReader()从磁盘读Block，并放进table.rep_->options.block_cache里，它指向全局唯一的block_cache;
//
//  总之：要通过this->options_使用
//         - comparator
//         - filter_policy
//         - block_cache
//         - info_log
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

//Yuanguo: 创建新LevelDB数据库，也就是：
//  - 创建文件dbname_/MANIFEST-0000001，其内容是：
//         VersionEdit new_db{ log_number_ = 0;  next_file_number_ = 2; last_sequence_ = 0; ... }
//  - 创建文件dbname_/CURRENT，其内容是字符串：
//         "MANIFEST-0000001\n"
Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    //Yuanguo: 等价于
    //  echo -n "MANIFEST-0000001\n" > tmp
    //  mv tmp dbname/CURRENT
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  //Yuanguo: dbname_/CURRENT存在与否 <=> LevelDB数据存在与否；
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      //Yuanguo: LevelDB数据库不存在，但允许创建，就创建它。所谓创建，就是做如下操作：
      //  - 创建文件dbname_/MANIFEST-0000001，其内容是：
      //         VersionEdit new_db{ log_number_ = 0;  next_file_number_ = 2; last_sequence_ = 0; ... }
      //  - 创建文件dbname_/CURRENT，其内容是字符串：
      //         "MANIFEST-0000001\n"
      //就是创建CURRENT、MANIFEST，并写入一个创世的VersionEdit；这样一来，对于后续流程而言，第一次创建DB和重启DB就一致了。
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  //Yuanguo: 有两种可能走到这里：
  //      1. 数据库之前不存在，前面NewDB()刚刚创建的;
  //      2. 数据库之前就存在，本次是重启；
  //对于两种情况，到这里就一致了:
  //      - 都有dbname_/CURRENT文件;
  //      - dbname_/CURRENT文件的内容是"MANIFEST-000000X\n"，指向manifest文件"dbname_/MANIFEST-000000X";
  //versions_->Recover()的主要任务是：从 dbname_/CURRENT --> dbname_/MANIFEST-000000X 恢复重启之前(或新建的)
  //      a. VersionSet::current_, 即重启之前的Version；
  //      b. VersionSet::中的一些计数器；
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  //Yuanguo: list dbname_目录下的所有文件；
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  //Yuanguo: 把所有version的所有level引用的table file的number放进expected；
  //   这些table file都应该存在，下面是一个检查:
  //      - 遍历dbname_录下的所有文件，把它们的number从expected一一删去；
  //      - 若最终expected不空，则里面剩下的就是不存在的文件；
  //      - 这种情况下，LevelDB corrupted;
  std::set<uint64_t> expected;
  //Yuanguo: 把所有version的所有level引用的table file的number放进expected；
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  //Yuanguo: 遍历dbname_录下的所有文件，把其中的log文件放进logs;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      //Yuanguo: 更严格一点：type==kTableFile成立时，才erase(number);
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  //Yuanguo: 上面做检查的同时，也把log/wal文件过滤出来了。下面把log/wal中的数据replay到memtable，进而
  //  写到file table;

  //Yuanguo: 这里以及RecoverLogFile()里，把log/wal中的数据写到table file中，并在
  //  edit中记录了对table file的引用。但为什么没有设置edit.log_number_(truncate log)呢？
  //  因为以后在DB::Open()中设置。
  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

//Yuanguo: 这个函数中
//  save_manifest被本函数置为true <====充分必要====> 本函数replay log/wal产生了新table file并记录到了edit中；
//  所以理论上，不需要save_manifest参数，调用者只看edit就知道了。
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    //Yuanguo: 创建一个Table File，写入实际的内容，并sync；这个过程中，不持有mutex_；
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      //Yuanguo: 之前，我以为一定flush在level-0上。看来不是这样的：假如没有overlap，尽量放在kMaxMemCompactLevel=2层。为什么？
      // 看kMaxMemCompactLevel的注释。
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    //Yuanguo: imm_是immutable memtable；在它变成immutable的时候，就新建了一个(mutable memtable)和log/wal file，
    //  logfile_number_ 正是那个新建的log/wal file的number；
    //  这里持久化logfile_number_表示：logfile_number_之前的的(不包括)都是无用的了，因为imm_中的的数据已经被WriteLevel0Table写到sst文件中了。
    //  这相当于truncate-log操作。
    //  注意，这两个东西必须被记录到edit中，原子地持久化：
    //          - 对sst文件的引用 (WriteLevel0Table函数把它记录到edit中了)；
    //          - truncate-log，即更新logfile_number_(这里做的事)；
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    //Yuanguo: 原子地持久化，并更新内存；
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

//Yuanguo:
//  user请求的compact range [begin, end]涉及多个"compaction"。每次调用TEST_CompactRange(level, ...)是进行一个
//  从第level层到level+1层的"compaction";
//  一个从第level层到level+1层的"compaction"由一个class Compaction的object表示:
//          输入: 第level层的M个table files;
//                第level+1层的N个table files;
//          输出：第level+1层的X个table files; "顶替"原来N个table files的位置。
//
//  注意1："顶替"是通过Version的更替发生的：
//          - 压缩前current_:
//                        0层       :  ......
//                        1层       :  ......
//                        ...       :  ......
//                        level层   ： [f1, f2, f3, f4, f5]
//                        level+1层 ： [F1, F2, F3, F4, F5, F6]
//                        level+2层 :  ......
//                        ...       :  ......
//          - 压缩：
//                        level [f2, f3, f4]  +  level+1 [F2, F3, F4, F5]  ==>  level+1 [Fx, ..., Fy]
//                        产生VersionEdit edit_c:
//                                  在第level层delete   : f2, f3, f4
//                                  在第level+1层delete : F2, F3, F4, F5
//                                  在第level+1层add    : Fx, ..., Fy
//
//          - 压缩后current_( = 压缩前current_ + edit_c):
//                        0层       :  ......
//                        1层       :  ......
//                        ...       :  ......
//                        level层   ： [f1, f5]
//                        level+1层 ： [F1, Fx, ..., Fy, F6]
//                        level+2层 :  ......
//                        ...       :  ......
//          - [Fx, ..., Fy]为什么能"顶替"[F2, F3, F4, F5]? 见DBImpl::BackgroundCompaction()前的例子。
//
//  注意2：在leveldb中，只有一个压缩线程(flush memtable也是一种压缩，也有本线程完成)，所以只有它来切换Version; (RocksDB?)
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  //Yuanguo: 把当前memtable被刷到table file，是这样完成的：
  //   1. 强制切换memtable(即当前memtable变成imm_)；
  //   2. 等待imm_==nullptr成立，即imm_被刷下去；
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  //Yuanguo: max_level_with_files是和[begin, end]有overlap的最大的层。
  //  例如max_level_with_files = 5，下面的循环只进行level = 0~4。
  //  当进行level=4时，就是把Level-4压缩到Level-5。
  //  显然，没有必要循环level=5 (Level-5到Level-6的压缩)。
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      //Yuanguo:
      //  1. 假如background_compaction_scheduled_=false，没有别的compaction在进行中。
      //        - MaybeScheduleCompaction()就会在一个独立的线程中(env_->Schedule)中执行 BGWork() --> BackgroundCall()，即执行我们的manual compaction；
      //        - MaybeScheduleCompaction()返回；这里的while循环继续，进入下面的background_work_finished_signal_.Wait()，等待我们的manual compaction完成。
      //        - Wait被唤醒：
      //            - 若manual.done为true，我们的manual compaction完成了，while循环结束；
      //            - 若manual.done为false，我们的manual compaction只是部分完成，其中的begin已经被修改到了已压缩结束的地方(见DBImpl::BackgroundCompaction函数末尾处)，
      //              所以继续调用MaybeScheduleCompaction()，即重新调度我们的manual compaction;
      //  2. 假如background_compaction_scheduled_=true，表示还有别人的compaction在进行中。
      //        - MaybeScheduleCompaction()立即返回；
      //        - 这里的while循环继续，进入下面的background_work_finished_signal_.Wait()，等待别人的compaction(和我们的manual compaction)完成。
      //              - 别人的compaction也是通过 BGWork() --> BackgroundCall()进去的；
      //              - 注意BackgroundCall()相当于一个循环或递归：它执行完一个compaction，会再调度一个compaction(即调用MaybeScheduleCompaction)；
      //              - 若再调度的compaction是我们的manual compaction，我们的manual compaction就得以执行了。
      //              - 当然，在我们的manual compaction之前，也有可能调度其他的compaction，或者memtable compaction，这都无所谓。这里的wait也可能被唤醒，
      //                但只要我们的manual compaction没有完成，就会重新wait。
      //        - Wait被唤醒：
      //            - 若manual.done为true，我们的manual compaction完成了，while循环结束；
      //            - 若manual.done为false，我们的manual compaction可能部分完成了，也可能还没开始，重新尝试调度它。
      //
      //Yuanguo: 更深刻的理解：
      //  先看MaybeScheduleCompaction函数前面的注释。
      //
      //  - 上面设置manual_compaction_ = &manual; 就是把manual放进"队列"。
      //  - 这里调用MaybeScheduleCompaction()唯一目的是：让压缩线程开始工作。假如它已经在忙着，那么什么都不做: manual已经在"队列"中了，而压缩线程也在忙着，能干什么呢? 
      //    只能立即返回，然后就会经过while循环进入下面的wait;
      //  - 压缩线程每完成一个工作，就会唤醒所有人(DBImpl::BackgroundCall中的SignalAll)，我们也会被唤醒。被唤醒之后，有3中情况：
      //       - 情况1: 我们的manual还在"队列"中，即manual_compaction_=&manual成立。这种情况下，又会直接再次wait，因为上面有判断: if (manual_compaction_ == nullptr) {...}
      //       - 情况2：压缩线程完成了manual中的部分工作，然后修改了manual，并把manual_compaction_设置为nullptr(见BackgroundCompaction的最后)。这相当于完成了我们的部分工作，
      //                就把manual移出"队列"了。这种情况下，因为manual.done=false，且if (manual_compaction_ == nullptr)不成立，就会再次走到这里: 
      //                设置manual_compaction_ = &manual，把manual放进"队列"，并MaybeScheduleCompaction();
      //       - 情况3：压缩线程彻底完成了manual的工作。while退出。
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

//Yuanguo: 把当前memtable被刷到table file，是这样完成的：
//   1. Write(WriteOptions(), nullptr); 强制切换memtable(即当前memtable变成imm_)；
//   2. 等待imm_==nullptr成立，即imm_被刷下去；
Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

//Yuanguo: 其实所有的复杂性，都来自于MaybeScheduleCompaction函数：它不是以队列的方式实现的，而是以这样一种方式:
//    - 下面有N=1个压缩线程，执行DBImpl::BGWork函数。测试中发现N=1，并且仅用一个background_compaction_scheduled_变量来表示压缩线程是忙着还是闲着，
//      所以N应该只能为1(RocksDB中应该有优化)。
//    - 上面有M个请求线程可能随机调用MaybeScheduleCompaction()。谁想请求压缩，就在自己的线程中调用MaybeScheduleCompaction()。外部用户
//      通过db->CompactRange (DBImpl::CompactRange)请求manual compaction只是一个特例。
//    - 这M个请求线程，有些是请求manual compaction，有些是让versions_去PickCompaction();
//    - 请求manual compaction的线程会设置manual_compaction_=&manual，并且压缩线程忙的时候，会等待。逻辑见TEST_CompactRange()函数。
//    - 其他请求线程(很多地方)，只有觉得可能要压缩或者可能有memtable要flush，就调用MaybeScheduleCompaction，压缩线程忙的时候(假设忙于CompactionX)，
//      也不等待：因为压缩线程忙完CompactionX(或CompactionX的部分)之后，也会调用一下MaybeScheduleCompaction，这时，有压缩工作要做或者有memtable
//      要flush，就有机会做了。
//    - 还有一个细节：请求线程T (我们假设为请求manual compation的外部用户线程)，调用MaybeScheduleCompaction，假如background_compaction_scheduled_=false(压缩线程闲着)
//      就触发压缩线程开始工作。虽然是请求线程T触发的压缩线程，但压缩线程并不一定执行manual compaction；见DBImpl::BackgroundCompaction：
//              - 它可能去flush一个memtable (CompactMemTable)然后返回。
//              - 这没关系：BackgroundCall()会唤醒请求线程T，请求线程T发现自己的done为false，重新调用MaybeScheduleCompaction().
//
//Yuanguo: 更深刻的理解：
//    - 设置manual_compaction_=&manual; 设置imm_=memtable; 让versions_->NeedsCompaction()=true，都是把压缩工作放进"队列"；
//    - 相反，设置manual_compaction_=nullptr; 设置imm_=nullptr; versions_->PickCompaction()都是从"队列"中取出压缩工作；
//    - 压缩线程处理完一个工作之后，再次调用MaybeScheduleCompaction()，相当于逐个消费"队列"中的压缩工作；
//
//    - 你要有压缩工作要做，可能是manual_compaction_, imm_, 也可能是versions_->NeedsCompaction()=true，就调用MaybeScheduleCompaction。
//      MaybeScheduleCompaction的唯一目的是: 让压缩线程开始工作。
//        - 假如压缩线程忙着，MaybeScheduleCompaction()什么都不干：因为压缩线程已经在忙着。当它忙完当前压缩工作，会自己调用MaybeScheduleCompaction(BackgroundCall中的调用), 取出
//          你的压缩工作去做。还是那句话，你设置了manual_compaction_、imm_或者让versions_->NeedsCompaction()=true，就已经把工作放进"队列"了。压缩线程会逐个完成。
//        - 假如压缩线程闲着，MaybeScheduleCompaction让它立即开始工作：但它不一定先做你的工作。还是前面的抽象：你把你的压缩工作放到"队列"中，然后然压缩线程开始工作，但同时可能
//          有别人并发的把他的工作放到你前面去了，所以工作线程不一定先干你的。但它能够通过自己反复调用MaybeScheduleCompaction(BackgroundCall中的调用)，排到你的工作。
//
//    - 设置了manual_compaction_，imm_，或者让versions_->NeedsCompaction()=true，就是把压缩工作放到队列中了，你可以等待它完成，也可以不等（压缩线程反复调用
//      MaybeScheduleCompaction()逐个完成）。
//    - 但有一个细节，让manual compaction还是等待为好:
//        - 对于压缩线程而言，它完成了manual compaction的一部分，然后修改一下manual.begin，它就认为把整个工作完成了：把manual_compaction_设置为nullptr，相当于从"队列"中移除
//          (见BackgroundCompaction的最后)。
//        - 压缩线程反复调用MaybeScheduleCompaction(ackgroundCall中的调用)也不会调度到它了，因为manual_compaction_=nullptr (从"队列"中移除了)；
//        - 所以，请求线程要想全部完成，只能等待被唤醒，发现没有彻底完成，再设置manual_compaction_=&manual(见TEST_CompactRange)，即放进"队列"。
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    //Yuanguo: 在独立的线程中执行: BGWork() --> BackgroundCall()
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

//Yuanguo: 线程体
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

//Yuanguo: BackgroundCall()相当于一个循环或递归：
//  它执行完一个compaction，会调用MaybeScheduleCompaction(), 再调度一个compaction;
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  // Yuanguo: 这里调用MaybeScheduleCompaction时，background_compaction_scheduled_一定为false（因为mutex_被锁住，更新不了）。
  //    所以，MaybeScheduleCompaction()不会在if (background_compaction_scheduled_)那里返回，所以，只要还有compaction要做，
  //    就能调度一个。
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

//Yuanguo: BackgroundCompaction在一个独立的线程中执行。以一个manual compaction为例：
//  - 压缩前current_:
//       0层       :  ......
//       1层       :  ......
//       ...       :  ......
//       level层   ：
//                         f1                f2                 f3                 f4             f5
//                    [a:555, d:777]    [d:716,  g:553]    [h:812, k:654]    [k:650, m:800]   [n:828, p:700]
//       level+1层 ：
//                         F1               F2              F3               F4              F5               F6
//                    [a:414, b:433]   [b:431, d:480]   [d:471, h:492]   [h:487, m:499]   [m:490, n:455]  [n:411,  q:400]
//       level+2层 :  ......
//       ...       :  ......
//
//  - 假设user请求的压缩区间是[e:*, j:*]。根据versions_->CompactRange()中的逻辑，最终选择的参与压缩的文件是:
//       level层    : [f2, f3, f4]
//       level+1层  ：[F2, F3, F4, F5]
//
//       注意：
//         - 对于level层，不能留着f4不压缩，因为k:650是一个过时版本，留在level层，Get会优先返回它(过时版本)。
//         - 对于level+1层，则没有这个问题：假设压缩之后产生的level+1层的文件是[Fx, ..., Fy]，
//                   - level层的[f2, f3, f4]中没有b:*和n:* (若有，则level+1层的F1和F6就必须参与压缩了，见VersionSet::SetupOtherInputs，其中记录了一个实验);
//                   - 所以，[Fx, ..., Fy]包含的区间是[b:431, ..., n:455];
//                   - 所以，[Fx, ..., Fy]"顶替"[F2, F3, F4, F5]没有破坏顺序;
//         - 总之：参与压缩的第level层右边界必须是"清晰"的；level+1层左右边界都可以是"模糊"的。关于"清晰"和"模糊"的定义，见
//           Version::GetOverlappingInputs()前的注释。
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    //Yuanguo: 返回到BackgroundCall()，立即调度下一个compaction，那时imm_很可能为nullptr;
    // 所以，如果有其它要压缩，可以继续。
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
   //Yuanguo: 尽管函数名字叫做CompactRange，它其实一点压缩的工作也没做，只是
   // 在level层和level+1层各选择一些table file参与压缩而已。
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      //
      // Yuanguo: 这个注释说：我们可能只压缩了请求的range的一部分。DBImpl::TEST_CompactRange()中，
      //    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
      //    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
      // 不是从start:kMaxSequenceNumber(start的最新版本)到end:0(end的最老版本)吗？为什么还可能只压缩了一部分呢？
      // 答案是这样的：在VersionSet::CompactRange()中，对于level > 0，做了一个MaxFileSizeForLevel的check，可能截断。
      //
      // Yuanguo: 另一个问题：改了之后如何生效呢？
      //    - 首先，m指向的是DBImpl::TEST_CompactRange()中的manual;
      //    - 这里改了它的begin；
      //    - 本函数结束，返回BackgroundCall();
      //    - BackgroundCall()调用MaybeScheduleCompaction()，但没有compaction要做了，因为manual_compaction_=nullptr （我们假定没有别人请求compaction，也没有memtable要flush）;
      //    - BackgroundCall()唤醒DBImpl::TEST_CompactRange (用户请求compaction的线程)；
      //    - DBImpl::TEST_CompactRange检测manual done为false，继续循环；且manual_compaction_=nullptr，所以设置manual_compaction_=&manual并调用MaybeScheduleCompaction();
      //    - 所以，修改过的manual (compaction)得以再次执行。
      //    - 如此反复，直到最后一轮: 本函数上面的versions_->CompactRange(m->level, m->begin, m->end)返回nullptr，m->done被设置为true；
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

// Yuanguo: to compact,
//   1. create an instance of `class Compaction`, say `c` which contains 2 overlapping levels:
//            inputs_[0]:  files in Level-N
//            inputs_[1]:  files in Level-N+1
//   2. create an instance of `CompactionState`, say `compact = new CompactionState(c);`
//   3. call this function DoCompactionWork, passing in `compact`, which roughly,
//         a. create `input`, which is a merging iterator on all files in `c.inputs_[0]` and `c.inputs_[1]`
//         b. iterate over `input`, for each key, drop old versions and save latest version in a table builder;
//         c. if builder->FileSize() large enough, close this builder (generating an output file) and create a new one;
//         d. repeat b-c, until finished or should stop early (see func ShouldStopBefore);
//         e. now we get a list of output files, then call InstallCompactionResults;
//                i.   make a VersionEdit instance, say `edit` 
//                ii.  add `c.inputs_[0]` and `c.inputs_[1]` in `edit.deleted_files_`
//                iii. add output files in `edit.new_files_` on level `N+1`; So, commpation is from [N, N+1] ==> N+1
//                iv.  versions_->LogAndApply(edit);
//         注意：step e 使整个压缩工作被持久化。在此之前，
//                  - 产生了一些table file；也sync+close了，所以它们本身是完整的。
//                  - 这些table file被保存在compact->outputs中。
//               假如此时发生crash，那本次压缩工作对外完全不可见，相当于没发生。内部产生的这些table file(在磁盘上)成为垃圾而已。
//               假如没有crash，InstallCompactionResults函数通过VersionEdit，原子地apply到VersionSet::current_上(先持久化manifest再apply内存):
//                  - current_的第N层和第N+1层分别删除c.inputs_[0]和c.inputs_[1]文件；
//                  - current_的第N+1层添加compact->outputs中的文件；
//               这就形成了新的current_。
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    //Yuanguo: input (merging iterator)输出的key是internal key。
    //   下面的逻辑根据
    //         1. 同一个user_key的覆盖关系；
    //         2. 同时考虑smallest_snapshot；
    //   来确定那些internal key可以被丢弃，那些必须被保留。
    //   被保留的key(还是internal key)被原封不动地写入output文件。
    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      //Yuanguo: 保留多版本，因为user持有snapshot；
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        //Yuanguo: 比如smallest_snapshot=997，key-x有5个版本:
        //             key-x:999
        //             key-x:998
        //             key-x:997
        //             key-x:996
        //             key-x:995
        //         显然，我们应该保留: key-x:999, key-x:998, key-x:997, 丢弃key-x:996, key-x:995;
        //         那么，为什么这个判断是 <= 呢？997满足=，drop不就被置为true，进而key-x:997也被丢弃了吗？
        //         执行一下：
        //             第1次key=key-x:999, last_sequence_for_key=kMaxSequenceNumber > 997, drop=false (即保留key-x:999), 然后设置last_sequence_for_key=999;
        //             第2次key=key-x:998, last_sequence_for_key=999,               > 997, drop=false (即保留key-x:998), 然后设置last_sequence_for_key=998;
        //             第3次key=key-x:997, last_sequence_for_key=998,               > 997, drop=false (即保留key-x:997), 然后设置last_sequence_for_key=997;
        //             第4次key=key-x:996, last_sequence_for_key=997,               = 997, drop=true  (即丢弃key-x:996), 然后设置last_sequence_for_key=996;
        //             第5次key=key-x:995, last_sequence_for_key=996,               < 997, drop=true  (即丢弃key-x:995), 然后设置last_sequence_for_key=995;
        //         可见，key-x:997是被保留的，没有被丢弃。原因是：last_sequence_for_key变量可以看做是"前一个处理过的key的版本号":
        //             前一个<=smallest_snapshot，现在的这个一定<smallest_snapshot;
        //             所以应该被丢弃，drop=true;
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        //Yuanguo: 本次压缩的第一个key，或者上一个table file满了，被close之后的第一个key：
        //  新建一个builder以及对应的table file (outfile)，并为table file预分配number;
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      //Yuanguo: 注意，可能产生
      //                 fN             fN+1
      //           [..., key-x:998] [key-x:997, ...]
      //  而没有试图把同一个user key的所有版本放入同一个table file。
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        //Yuanguo: compact->outfile满了，所以，sync+close它；
        //   销毁compact->builder，并置为nullptr，
        //   以便下一个key到来时，创建新的builder和table file(outfile);
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  //Yuanguo: 返回给user的Iterator (user可能持有很久) 使用Version current_；
  //  所以，若内部发生版压缩(没看见哪里有这样的压缩，理论上是可以的):
  //       - current_ + editX + editY + ... = v;
  //       - original_current_ = current_;
  //       - current_ = v;
  //       - original_current_ -> Unref();
  //  正是因为这个Ref，original_current_(user的Iterator还在使用)不会被销毁；
  //  自然地，而对应的Unref在CleanupIteratorState()中(user是否Iterator的时
  //  候调用)。
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  //Yuanguo:
  //  4个user在各自的线程中调用DBImpl::Write()，这里形成:
  //        writers_: w, w1, w2, w3
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    //Yuanguo: w1, w2, w3对应的线程都在此等待；
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  //Yuanguo: w对应的的线程继续;
  // 注意：
  //     - 不可能多个用户线程并发走到这里；
  //     - 所以后面释放mutex_的时间里(包括MakeRoomForWrite中)，不可能有用户线程并发处理写任务，顶多
  //       有用户线程把Writer放进writers_队列(上面的writers_.push_back(&w))然后等待。

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    //Yuanguo: 注意，这里BuildBatchGroup函数尽可能多的把w1, w2, w3也带上，除非:
    //                 - w不sync，但被带的要sync；
    //                 - write_batch已经够大了；
    //         假设，这里带上w1, w2;
    //         返回时，输出参数last_writer = &w2;
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    //Yuanguo: 这里预留对应数量的sequence number;
    //         WriteBatchInternal::InsertInto()的时候，顺序地分给各个Write Op;
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    // Yuanguo: 这里注释说"&w"负责保护不发生并发log和并发写mem_；
    //    如何保护呢？应该是上面的判断：
    //           &w != writers_.front()
    //    满足的话，就wait，所以只有front能往下走。
    {
      mutex_.Unlock();
      //Yuanguo: 写入log/wal; 若options.sync=false，何时sync? 还是说，根本不sync，user承担丢数据/corruption的后果？
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        //Yuanguo: 已经写入log/wal；现写入memtable;
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  //Yuanguo:
  //  第1次：ready == &w 且 ready != last_writer(&w2) ; do nothing;
  //  第2次：read = &w1 (ready != &w成立)，唤醒w1对应的线程(它返回时，w1.done为true，直接返回);
  //  第3次：read = &w2 (ready != &w成立)，唤醒w2对应的线程(它返回时，w2.done为true，直接返回)；ready == last_writer成立，break；
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    //Yuanguo: w, w1, w2都完成了，且从writes_中pop出来了； w3变成front，通知对应的线程。
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
// Yuanguo: 只有一处调用，即DBImpl::Write()。
// DBImpl::Write()中，
//     - 若updates == nullptr，则调用MakeRoomForWrite(force=true); 表示并没有WriteBatch要写，而是要强制
//       创建新的memtable和新的log/wal文件，这又可能导致compaction;
//            - while循环中，经过0到多次的background_work_finished_signal_.Wait() (两处)，最终进入最后分支(创建新的memtable和新的log/wal文件，调度compaction);
//            - 最后分支中，force被设置为false；
//            - 下一轮循环，进入 else if (!force && ...) {} ，认为空间足够，break退出。
//     - 若updates != nullptr，则调用MakeRoomForWrite(force=false); 表示要确保memtable的空间足够；可能经过
//            SleepForMicroseconds(1000);
//            background_work_finished_signal_.Wait();
//            和/或最后分支(创建新的memtable和新的log/wal文件)
//       最终在空间足够时break退出。
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      // Yuanguo: memtable满了，并且没有immutable memtable;
      //     - 重新打开一个log (wal) 文件；
      //     - 把memtable变为immutable;
      //     - 重新创建一个空的memtable;
      // 注意：log/wal文件和table文件是一一对应的。memtable切换时，log(wal)文件也一起切换；这样做的好处是：
      // 当memtable被flush到磁盘(产生table file)，truncate log/wal非常方便: 
      //     - 通过edit记录并持久化log_number_，表示这个log/wal文件号之前的log都被truncate了;
      //     - 并且，通过同一个edit记录并持久化manifest对table file的引用，所以是原子的;
      //     - 即数据从log/wal ---> table file的转换是原子的;
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      //Yuanguo: 把memtable变为immutable;
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      //Yuanguo: force置为false，下一轮循环，能够发现空间足够，并break；
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

//Yuanguo:
//
// ===================================================================== 创建LevelDB =====================================================================
//
//  1. 构造DBImpl，VersionSet(versions_)，这时versions_中的Version链表:
//             dummy_versions_   <------>  empty_version(current_)
//  2. 构造edit1 (DB::Open中的VersionEdit edit;)
//  3. impl->Recover():
//       A. 调用DBImpl::NewDB()，创建:
//              - 文件dbname_/MANIFEST-0000001，其内容是:
//                        VersionEdit new_db{log_number_ = 0;  next_file_number_ = 2; last_sequence_ = 0; ...};
//                我们把它记作edit0。
//              - 文件dbname_/CURRENT，其内容是字符串："MANIFEST-0000001\n";
//              - 这样就创建了一个初始状态，是创世之前的工作。有了它，后面的VersionSet::Recover()就像是重启一样。所以这里记号是edit0，DB::Open中的是edit1。
//          注: next_file_number_是文件号分配器。
//              - manifest文件、log/wal文件、table file的文件号，都是通过递增这个计数器来分配的。
//              - 1已经分配给了dbname_/MANIFEST-0000001，所以这里直接持久化为2(下一个文件号是2);
//              - 这个计数器，运行时存在于VersionSet::next_file_number_中。它总是在产生新Version的时候递增。产生新Version，也必然要产生并持久化VersionEdit,
//                自然，把它存到VersionEdit中并持久化；
//              - crash/stop之后重启时，VersionSet::Recover()函数再把它加载到VersionSet::next_file_number_中。
//       B. 调用VersionSet::Recover():
//              - 此时current_是empty_version；
//              - 读MANIFEST-0000001，并apply到current_(empty_version)，得到 Version* v = empty_version + edit0，我们记为v1;
//              - 调用AppendVersion(v1)，此时, versions_中的Version链表变成:
//                       dummy_versions_   <------>  empty_version  <-----------> current_(v1)
//              - 此时，已经从dbname_/MANIFEST-0000001恢复(就像重启一样)了计数器:
//                       VersionSet::manifest_file_number_ = 2；
//                       VersionSet::next_file_number_ = 3 (本来是2，分配给VersionSet::manifest_file_number_之后，变为3);
//                       VersionSet::last_sequence_ = 0;
//                       VersionSet::log_number_ = 0;
//                如前所述，next_file_number_是文件号分配器。
//                       1分配给了dbname_/MANIFEST-0000001;
//                       2分配给了VersionSet::manifest_file_number_；下一个manifest文件号；
//                       所以，目前值为3；
//              - Options::reuse_logs默认为false，save_manifest被置为true； descriptor_log_为nullptr;
//       C. 没有log/wal要replay;
//
//  4. if (s.ok() && impl->mem_ == nullptr) 成立: 创建一个空的memtable以及对应的log/wal文件，注意：
//              - new_log_number=3; 是从VersionSet::next_file_number_分配的。分配之后，VersionSet::next_file_number_ = 4;
//              - 在空的edit1 (第2步构造的，现在还是空的)中记录:
//                       edit1.log_number_ = new_log_number = 3。
//                       持久化的VersionEdit::log_number_，不是用来记录或分配下一个log/wal文件号的，
//                       它表示3以前(不包括3)的log/wal被truncate了(当然，对于新创建的LevelDB，此时还没有log/wal文件);
//              - 重要：log/wal文件和table文件是一一对应的。现在创建了一个memtable和000003.log。当memtable切换时，log/wal文件也一起切换，见
//                       DBImpl::Write() -->
//                       DBImpl::MakeRoomForWrite()
//                这样做的好处是：当memtable被flush到磁盘(产生table file)，truncate log/wal非常方便:
//                       就是通过edit记录并持久化log_number_，表示这个log/wal文件号之前的log都被truncate了;
//                       并且，通过同一个edit记录并持久化manifest对table file的引用，所以是原子的。即数据从log/wal ---> table file的转换是原子的。
//
//  5. 因为save_manifest=true，调用LogAndApply()，descriptor_log_为nullptr。
//       A. edit1.next_file_number_ = 4; edit1.last_sequence_ = 0;
//       B. 创建dbname_/MANIFEST-0000002，内容是:
//             Version: v1
//             VersionEdit: edit1{log_number_=3; next_file_number_=4; last_sequence_=0};
//          记 v2 = v1 + edit1;
//       C. 更新dbname_/CURRENT："MANIFEST-0000002\n";
//       D. AppendVersion(v2):
//                       dummy_versions_   <------>  empty_version  <-----------> v1  <--------> current_(v2)
//
//       注：第5步其实不是必须的，并且可以通过配置Options::reuse_logs=true，进而导致 save_manifest=false 来跳过这一步。
//       这一步其实没有必要的工作：就是从dbname_/MANIFEST-0000001切换到dbname_/MANIFEST-0000002 (导致文件号分配器next_file_number_更新了)。
//
//  6. RemoveObsoleteFiles:
//       删掉dbname_/MANIFEST-0000001;
//
//  7. 至此，LevelDB创建完成，descriptor_log_是打开的dbname_/MANIFEST-0000002的句柄。新创建的LevelDB目录：
//              myleveldb/
//              ├── 000003.log
//              ├── CURRENT
//              ├── LOCK
//              ├── LOG
//              └── MANIFEST-000002
//
//  8. 开始写入数据，当第一个memtable满时，DBImpl::Write() --> DBImpl::MakeRoomForWrite()，切换memtable以及对应的log/wal(如前所说，它们一一对应)
//         - 分配log/wal文件号4并创建000004.log；分配之后，VersionSet::next_file_number_ = 5;
//         - 创新的空的memtable；
//         - 之前的memtable (和000003.log对应的)变为immtable，记录在DBImpl::imm_中。
//         - DBImpl::MaybeScheduleCompaction() -> ... -> DBImpl::CompactMemTable()：
//                 A. 创建一个VersionEdit edit2 (DBImpl::CompactMemTable()函数中的VersionEdit edit;)；
//                 B. 调用WriteLevel0Table(): 
//                         1. 分配table file文件号5；分配之后，VersionSet::next_file_number_ = 6);
//                         2. 把immtable memtable (DBImpl::imm_)写到table file，名为000005.ldb中，并sync；
//                         3. 在edit2中记录对000005.ldb的引用;
//                 C. edit2.log_number_ = 4，表示000004.log之前的log都被truncate了；
//                 D. LogAndApply: 
//                         1. edit2.next_file_number_ = 6 (持久化文件号分配器，以便crash重启之后恢复VersionSet::next_file_number_)
//                         2. 记: v3 = current_(v2) + edit2。
//                         3. 把edit2 append到MANIFEST-0000002 (descriptor_log_ != nullptr，它就是打开的MANIFEST-0000002的句柄);
//                            我们的目的是让MANIFEST-0000002保存v3。
//                            注意：当前MANIFEST-0000002中的内容是(见前面5.B步);
//                                      Version: v1
//                                      VersionEdit: edit1;
//                            即，当前MANIFEST-0000002中的内容是 v1 + edit1 = v2;
//                            所以，我们不能append v3，而只需append edit2。append之后，MANIFEST-0000002中的内容是：
//                                      Version: v1
//                                      VersionEdit: edit1;
//                                      VersionEdit: edit2;
//                            这和v3是等价的:  v3 = v1 + edit1 + edit2;
//                         4. 更新内存中的VersionSet的version链表:
//                                dummy_versions_   <------>  empty_version  <-----------> v1  <--------> v2 <-------> current_(v3)
//                 E. 删除无用的000003.log；此时目录:
//                        myleveldb/
//                        ├── 000004.log
//                        ├── 000005.ldb
//                        ├── CURRENT
//                        ├── LOCK
//                        ├── LOG
//                        └── MANIFEST-000002
//
//     重点：
//          - 不变式：dbname/CURRENT指向的manifest文件 (我们记为current_manifest) 中记录的内容叠加起来等于VersionSet::current_(即当前版本);
//          - 新版本产生时，总是先更新current_manifest，再更新VersionSet::current_；反之不行，更新完VersionSet::current_之后就可见，
//            若更新current_manifest失败，则相当于把为持久化的状态暴露出去了。
//          - current_manifest保存着Version变迁的Snapshot和LOG；
//          - current_manifest也会切换(例如重启时)；以 manifestX --> manifestY 的切换为例:
//                - 先把manifestX的build成一个Version，相当于压缩成一个Snapshot并把Snapshot写到manifestY，见函数
//                      LogAndApply --> WriteSnapshot
//                - manifestY打开着，再慢慢追加VersionEdit(相当于LOG);
//          - 显然：版本Version的变迁和manifest文件是否切换没有关系：一个VersionEdit对应一个版本。
//
// 假如此时发生了重启。
//
// ===================================================================== 重启LevelDB =====================================================================
//
//  R1. 构造DBImpl，VersionSet(versions_)，这时versions_中的Version链表:
//             dummy_versions_   <------>  empty_version(current_)
//  R2. 构造edit3 (DB::Open中的VersionEdit edit;)
//  R3. impl->Recover():
//       A. 调用VersionSet::Recover():
//              - 此时current_是empty_version；
//              - 读MANIFEST-0000002的内容(见上面第8.D.3步)：
//                                      Version: v1
//                                      VersionEdit: edit1;
//                                      VersionEdit: edit2;
//                并apply到current_(empty_version)，得到 Version* v = empty_version + v1 + edit1 + edit2，它是重启之前的v3;
//              - 调用AppendVersion(v3)，此时, versions_中的Version链表变成:
//                       dummy_versions_   <------>  empty_version  <-----------> current_(v3)
//              - 此时，已经从dbname_/MANIFEST-0000002恢复了计数器:
//                       VersionSet::manifest_file_number_ = 6；
//                       VersionSet::next_file_number_ = 7 (本来是6，分配给VersionSet::manifest_file_number_之后，变为7);
//                       VersionSet::log_number_ = 4;
//              - Options::reuse_logs默认为false，save_manifest被置为true； descriptor_log_为nullptr;
//
//       B. 调用RecoverLogFile，replay 000004.log:
//              - 创建memtable;
//              - 读000004.log，恢复到memtable中；
//              - 假定Options::reuse_logs=false(默认值)，不设置DBImpl::mem_，即impl->mem_ = nullptr不变。
//              - 调用WriteLevel0Table把memtable写到磁盘上生成table file 000007.ldb。分配的的文件号是7，分配之后，VersionSet::next_file_number_ = 8;
//              - 把对000007.ldb的引用记录到edit3中。
//
//  R4. if (s.ok() && impl->mem_ == nullptr) 成立(见R3.B步): 创建一个空的memtable以及对应的log/wal文件，注意：
//              - new_log_number=8; 分配之后，VersionSet::next_file_number_ = 9;
//              - 在edit3中记录:
//                       edit3.log_number_ = new_log_number = 8。
//                表示8以前(不包括8)的log/wal，即000004.log，被truncate了;
//
//                再提示一遍：
//                    - R3.B中，把对000007.ldb的引用记录到edit3中了，它就是000004.log的内容;
//                    - 现在truncate掉000004.log;
//                    - 这2个操作，通过同一个edit3原子地持久化。也就是说，数据从000004.log转移到000007.ldb是原子的。
//
//  R5. 因为save_manifest=true，调用LogAndApply()，descriptor_log_为nullptr。
//       A. edit3.next_file_number_ = 9;
//       B. 创建dbname_/MANIFEST-0000006 (为什么文件号是6？见第R3.A步，Recover的时候，就预留了manifest_file_number_ = 6).
//       C. 写入dbname_/MANIFEST-0000006内容:
//             Version: v3   (WriteSnapshot函数写入的);
//             VersionEdit: edit3 {next_file_number_ = 9; log_number_ = 8; ref 000007.ldb; ...}
//          记 v4 = v3 + edit3;
//       D. 更新dbname_/CURRENT："MANIFEST-0000006\n";
//       E. AppendVersion(v4):
//                       dummy_versions_   <------>  empty_version  <-----------> v3 <----------->  current_(v4)
//
//          这里的v3是重启之前的状态的Snapshot。重启之前，它在dbname_/MANIFEST-0000002文件中的存在形式是(3条):
//                       Version: v1
//                       VersionEdit: edit1;
//                       VersionEdit: edit2;
//          现在在dbname_/MANIFEST-0000006中的存在形式是(1条):
//                        Version: v3
//
//  R6. RemoveObsoleteFiles:
//       删掉dbname_/MANIFEST-0000002;
//
//  R7. 至此，LevelDB创建完成，descriptor_log_是打开的dbname_/MANIFEST-0000006的句柄。此时LevelDB目录：
//             myleveldb/
//             ├── 000005.ldb
//             ├── 000007.ldb
//             ├── 000008.log
//             ├── CURRENT
//             ├── LOCK
//             ├── LOG
//             ├── LOG.old
//             └── MANIFEST-000006
Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  //Yuanguo:
  //    - Recover的过程中要replay log到mem-table;
  //    - mem-table满了之后，就flush到table file (产生新table file);
  //    - edit就是记录新的table file；
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  //Yuanguo: save_manifest在两个地方被设置为true:
  //   A. replay log导致有新的table file产生；见 DBImpl::Recover() --> DBImpl::RecoverLogFile();
  //   B. dbname/CURRENT指向的manifest文件比较大；见DBImpl::Recover() --> VersionSet::Recover()调用ReuseManifest判断；
  //不过Options::reuse_logs默认为false，所以忽略B，所以只剩A:
  //      save_manifest=true <====充分必要====> replay log/wal产生了新table file并记录到了edit中
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    //Yuanguo: 重启前的log/wal都已经replay了，truncate它们(即impl->logfile_number_之前的log都被truncate了)。
    edit.SetLogNumber(impl->logfile_number_);
    //Yuanguo: 持久化edit，产生新Version current = {original-current} + edit;
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
