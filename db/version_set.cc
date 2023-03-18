// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

// Yuanguo: the same logic as seeking a block in a table: the key of the block index is
//   the largest key of that block, just like `f->largest` here;
//   the returned block is the first block whose largest key >= "target"; so "target"
//   may only exist in this block:
//       previous blocks  : largest key < "target";
//       following blocks : first key > "target";
//   here, `FindFile` returns the first file whose largest key >= "target";
//
// Yuanguo:  return N
//          fM.largest < key
//          fN.largest >= key
//    +------ fM ----------+
//    | smallest           |
//    | ...                |
//    | ...                |
//    | largest            |
//    +--------------------+
//    +------ fN ----------+
//    | smallest           |
//    | ...                |
//    | ...                |
//    | largest            |
//    +--------------------+
//    +------ fO ----------+
//    | smallest           |
//    | ...                |
//    | ...                |
//    | largest            |
//    +--------------------+
//
//    所以: key只可能在fN范围内:
//        - key一定不在fM范围内，因为fM.largest < key
//        - key一定不在fO范围内，因为fO.smallest > key
//
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

//Yuanguo:
//   在AfterFile()中，nullptr代表最小key；所以user_key改名为smallest_user_key更合适；
//   在BeforeFile()中，nullptr代表最大key；所以user_key改名为largest_user_key更合适；
//见SomeFileOverlapsRange()对它们的调用;
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    // Yuanguo: for level-0; Level-0的各个文件是可以相交的，所以只能遍历；
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  // Yuanguo: for levels other than level-0; 其他层的文件不相交，可以binary search;
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    // Yuanguo: the first file whose largest key >= small_key; the files
    //   before it are irrelevant, because their largest key < small_key;
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // Yuanguo: files[index] is the first file whose largest key >= small_key;
  //   if largest_user_key is befor it (largest_user_key < its smallest key), no overlap;
  //   else, overlap;
  // Yuanguo: 已知 smallest_user_key <= files[index].largest;
  //  case-1:                                   files[index].smallest         files[index].largest
  //                                                    |                             |
  //                                                    V                             V
  //                                                    +-----------------------------+
  //                                                    |                             |
  //                                                    +-----------------------------+
  //                                                          ^                  ^
  //                                                          |                  |
  //                                                   smallest_user_key   largest_user_key
  //
  //
  //  case-2:                                   files[index].smallest         files[index].largest
  //                                                    |                             |
  //                                                    V                             V
  //                                                    +-----------------------------+
  //                                                    |                             |
  //                                                    +-----------------------------+
  //                                              ^                              ^
  //                                              |                              |
  //                                        smallest_user_key              largest_user_key
  //
  //
  //  case-3:                                   files[index].smallest         files[index].largest
  //                                                    |                             |
  //                                                    V                             V
  //                                                    +-----------------------------+
  //                                                    |                             |
  //                                                    +-----------------------------+
  //                            ^                   ^
  //                            |                   |
  //                      smallest_user_key   largest_user_key
  //
  // 所以，是否overlap，取决于largest_user_key是否before file；
  //        largest_user_key before file:      case3，没有overlap；
  //        largest_user_key not before file:  case-1和case-2，有overlap；
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
//Yuanguo: version V的第L层，有N个file：
//      fP   +------------------+
//           |                  |
//           |     largest      |
//           +------------------+
//      fQ   +------------------+
//           |                  |
//           |     largest      |
//           +------------------+
//      ...
//      fX   +------------------+
//           |                  |
//           |     largest      |
//           +------------------+
//
// 迭代：
//       fP.larget =====>  {P, size-of-fP}
//       fQ.larget =====>  {Q, size-of-fQ}
//          ...
//       fX.larget =====>  {X, size-of-fX}
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  // Yuanguo: key is the largest key of current file;
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  // Yuanguo: value is "number .. file_size" of current file;
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

// Yuanguo: get the iterator of the "Table object" corresponding to the file;
//   it's the return value of "Table::NewIterator()" in table/table.cc;
//   file_value是 {file_number, file_size}，
//   也就是上面Version::LevelFileNumIterator迭代产生的结果；
static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

// Yuanguo: return a TwoLevelIterator, it is the iterator of "files in a given level";
//   upper level: LevelFileNumIterator, which yields file (largest_key => number..file_size) one by one;
//   lower level: table iterator created by "GetFileIterator()", which yields kv pairs in the table/file;
// Notice, lower level (the table iterator) is also a TwoLevelIterator, see "Table::NewIterator()" in
// table/table.cc;
//   即，嵌套结构:
//       NewTwoLevelIterator
//             index_iter_/upper (LevelFileNumIterator): 迭代产生largest_key_in_file#F ==> Slice{file#F, file_size}
//             data_iter_/lower (NewTwoLevelIterator): 使用upper迭代输出的Slice{file#F, file_size}构建的，Table::NewIterator()返回的，也是一个NewTwoLevelIterator，见GetFileIterator();
//                        index_iter_/upper: 迭代产生一个个的block handle; block handle用来创建block内的Iterator;
//                        data_iter_/lower: 使用upper迭代输出的block handle构建的，block内的Iterator，迭代产生key-val-pairs;
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

// Yuanguo: [
//           Iterator-level0-file0, Iterator-level0-file1, ..., Iterator-level0-fileN,
//           Iterator-level1,
//           Iterator-level2,
//           ...,
//          ]
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  // Yuanguo: how are they merged? I don't see any merge here.
  // Yuanguo: 这些iterator都被放进vector iters，最终被用来构造
  //          一个NewMergingIterator。见:
  //            DBImpl::NewInternalIterator() -->
  //            NewMergingIterator()
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace

// Yuanguo: *arg->value = v if ikey matches arg->user_key;
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

// Yuanguo: ForEachOverlapping的逻辑：
//   1. for each file `f` in level-0: if `f` overlaps with user_key, call `func` on it;
//   2. for level 1, 2, ..., find the find `f` overlapping with internal_key, call `func` on it;
// the process stops once `func` returns false;
//
//ForEachOverlapping有两个使用场景：
//
// Version::Get,
//    user_key/internal_key: the key to get; they are used to find potential files (overlapping files);
//    func: a Match function;
//            if func(potential_file) return false, meaning target key is FOUND, ForEachOverlapping will return;
//            else, func(potential_file) return true, meaning NOT FOUND, ForEachOverlapping will keep trying;
// it goes like this:
//   1. find the potential files (multiple) in level-0, sort them by `NewestFirst` (the larger number, the newest),
//      and for each of them, call func(potential_file), until FOUND (func returns false) in some file;
//   2. if not found in level-0, keep trying in level 1, 2, ..., for each level, find the potential file (only one),
//      and call func(potential_file), until FOUND in some level;
//
// Version::RecordReadSample
//    user_key/internal_key: the key to sample; they are used to find overlapping files;
//    func: a Match function;
//            count the overlapping files (matches++);
//            if matches >= 2, return false (stop counting), meaning >=2 files overlapping with the key, compaction
//            may be needed;
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    // Yuanguo: 为什么这里使用internal_key?
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    // Yuanguo: if file `f` contains ikey, returns false; else return true (keep searching);
    //   返回：要不要继续搜索，true继续，false停止；
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      //Yuanguo: state->stats的作用是什么？
      //    如果为了get k调用了多次Match()，就把第一次的f记录到state->stats中。
      //    如果只调用了一次Match()则不记 (为什么？)；
      // 如果记了，DBImpl::Get()中就会调用Version::UpdateStats():
      //    state->stats->seek_file.allowed_seeks--;
      // 如果递减之后，allowed_seeks为0了，则把这个文件以及它的level记录到
      //    Version::file_to_compact_,
      //    Version::file_to_compact_level_,
      // 等待compaction；
      // Version::RecordReadSample()类似。
      //
      // 一个file被seek次数很多次(allowed_seeks递减为0)，则要compact它。背后逻辑:
      //    根据局部性原理，以后它还会被seek很多次，
      //    compact它会让以后的seek更高效，
      //    所以要compact它。
      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;

  //Yuanguo: LookupKey的结构:
  //
  //           4字节                                                       1B       7B
  //   +--------------------+--------------------------------------------+----+-----------------+
  //   | "user_key的长度+8" |               user_key的数据               |type| seq(低字节在前) |
  //   +--------------------+--------------------------------------------+----+-----------------+
  //   ^    即后面总长度    ^                                                                   ^
  //   |                    |                                                                   |
  // start_              kstart_                                                               end_
  //
  //k.internal_key() : kstart_ 到 end_ 的部分，包括 type + seq;
  //k.user_key()     : user_key的数据部分，不包括 type + seq;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  //Yuanguo:
  //  1. 先找到overlapping的文件f (user_key可能在f中)，这里用user_key来找overlapping(为什么user_key)。
  //  2. 对文件f调用State::Match(&state, level, f);
  //          - f是一个(持久化的)Table;
  //          - 在f (Table) 中找第一个 >= internal_key的kv-pair {ik,v}，然后调用SaveValue(&saver, ik, v);
  //                  - SaveValue中再解析出ik (internal key) 中的user key, 并和这里的saver.user_key比较；
  //                  - 若相等，则看是Deleted还是Found；
  //                  - 若Found，则把v存到saver.value中；
  //  问题：LookupKey中的user_key和internal_key如何起作用的？看下面的例子。
  //
  //  例如我们get的目标是d:500。思路是这样的：
  //      - 对于一个"潜在的可能的文件f"，找f中的"第一个>=d:500的key"，记为TargetKey;
  //      - TargetKey可能是d:x(x<=500) ..., 也可能是e:x(x任意);
  //      - SaveValue函数对它进行判断：
  //            - 若是d:x(x<=500)，则找到了；
  //            - 否则，是e:x，则没找到；需要继续找别的文件；
  //
  //  如何找"潜在的可能的文件f"呢？对于第0层和第1-N层又不同：第0层可能有很多潜在的可能的文件；而第1-N层中，每层只有一个。
  //
  //  对于第0层：
  //      - 使用user_key来找overlapping的文件，它们都是潜在的可能的文件。
  //      - 按文件的新旧排序，最新的在最前，例如 f0 = [b:800, f:600], f1 = [c:300, g:400], f2 = [a:200, g:100]
  //      - ForEachOverlapping中调用func(state, 0, f)，即State::Match(state, 0, f):
  //            - 对于f = f0，假如其中有d:540，调用state->vset->table_cache_->Get(...)，传入的是ikey="d:500";
  //              TableCache::Get() --> Table::InternalGet()，因为传入的k="d:500"，所以跳过了"d:540"（因为d:540排在d:500之前）。
  //                   - 假如找到的"第一个>=d:500的key"，即TargetKey是e:x(x任意);
  //                   - 调用handle_result，即SaveValue(saver, "e:x", "vvvvv")：因为"d"!="e"，所以saver.state保持为kNotFound不变；
  //              所以State::Match()返回true，意思是：在f=f0中没有找到"d:x"(x<=500)，所以要继续；
  //            - 对于 f = f1，假如其中没有d:x，State::Match() --> TableCache::Get() --> Table::InternalGet()找到的"第一个>=d:500的key"，即TargetKey，是e.x:
  //              和上面一样。
  //            - 对于 f = f2，假如其中有两个d:x(x<=500): "d:188"和"d:187"，State::Match() --> TableCache::Get() --> Table::InternalGet()中找到的"第一个>=d:500的key"，
  //              即TargetKey，一定是"d:188"，调用handle_result，即SaveValue(saver, "d:188", ...)，因为"d"=="d"，所以saver.state被置为kFound/kDeleted；
  //              所以State::Match()返回false，意思是在f=f2中，找到"d:x"(x<=500)，不再继续；
  //
  //  上面是第0层成功找到的情况，假如第0成失败，
  //
  //  对于level = 1~N层：
  //      - 因为对于1~N层，文件是有序的，所以ForEachOverlapping直接调用FindFile(internal_comparator, files[level], internal_key="d:500")找：第一个满足
  //        "f.largest_internal_key>=d:500"的f；
  //
  //            例1，返回f1:
  //                f0:   [a:970, ......................, b:900]      -->    b:900 < d:500
  //                f1:   [c:239, ..., c:110, e:522, ..., e:419]      -->    e:419 > d:500，返回f1
  //                f2:   [f:539, ......................, g:430]
  //
  //            例2，返回f1:
  //                f0:   [a:970, ......................, b:900]      -->    b:900 < d:500
  //                f1:   [c:239, ..., d:610, d:413, ..., e:419]      -->    e:419 > d:500，返回f1
  //                f2:   [f:539, ......................, g:430]
  //
  //            例3，返回f2:
  //                f0:   [a:800, ......................, b:910]      -->    b:910 < d:500
  //                f1:   [b:909, ......................, d:540]      -->    d:540 < d:500
  //                f2:   [d:539, ..., d:520, d:500, ..., d:444]      -->    d:444 > d:500，返回f2
  //
  //            例4，返回f1:
  //                f0:   [a:800, ......................, c:910]      -->    c:910 < d:500
  //                f1:   [e:123, ......................, f:456]      -->    f:456 > d:500，返回f1
  //                f2:   [g:539, ......................, h:430]
  //
  //      - f有3种情况：
  //            case1. user_key有overlap(区间[c, e]包含d)，但其中没有"d:x"，例1；
  //            case2. user_key有overlap，且其中也有"d:x"，例2、3;
  //            case3. user_key没有overlap，例4；
  //        但，不管哪种，第level层中的"第一个>=d:500的key"，即TargetKey，一定在f中，因为：
  //            它前面的fx: fx.largest_internal_key  < d:500
  //            它后面的fy: fy.smallest_internal_key > d:500 (因为fy.smallest_internal_key > f.largest_internal_key >= d:500)
  //        即，当前层中，只有这个f是潜在的可能的文件。
  //
  //      - 所以，对于f:
  //            - case1和case2: user_key有overlap，则调用func，即State::Match() --> TableCache::Get() --> Table::InternalGet()，
  //              在f中找第一个大于>="d:500"的key，
  //                   可能是e:x即e:522 (case1-例1);
  //                   也可能是d:x，"d:413" (case2-例2)，"d:500" (case2-例3);
  //              由Table::InternalGet() --> handle_result(即SaveValue)进一步判断；
  //            - case3: user_key没有overlap，直接下一层。因为"第一个>=d:500的key"一定不是"d:x"，例4的"e:123"，
  //              让Table::InternalGet() --> handle_result(即SaveValue)判断也一定失败。
  //
  //  回到前面的问题：LookupKey中的user_key和internal_key如何起作用的？
  //     - 其实，无论对于Level-0还是Level-1到N，我们的目的都是找"第一个>=d:500的key"，即TargetKey。
  //     - 对于第1-N层，因为各个table file没有overlap，所以可以直接找第一个满足"f.largest_internal_key>=d:500"的f; TargetKey一定在其中，因为：
  //            它前面的fx: fx.largest_internal_key  < d:500
  //            它后面的fy: fy.smallest_internal_key > d:500 (因为fy.smallest_internal_key > f.largest_internal_key >= d:500)
  //       即，当前层中，只有这个f是潜在的可能的文件。所以直接使用internal_key去找f;
  //     - 对于第0层，因为各个table file是有overlap的，所以没法直接使用internal_key，而只好使用user_key，找和user_key有overlap的所有f，在按重新到旧排列，
  //       它们都是潜在的可能的文件，需要一一搜索。
  //
  //  粗略的想：Get()是找到某个key，而DBImpl::NewIterator()然后Seek到某个位置上，也是找到某个key；它们的消耗应该差不多。
  //  但实际上却不是这样的:
  //
  //  假如操作是这样的：put(k1), delete(k1), put(k2), delete(k2), ..., put(k8), delete(k8), put(k9);
  //     Seek(k1): 需要跳过k1:kDeleted, k1:kTypeValue, k2:kDeleted, k2:kTypeValue, ..., k8:kDeleted, k8:kTypeValue，
  //               最终Iterator指向有效的k9:kTypeValue。假如要跳过1亿条呢？
  //     Get(k1):  找到k1:kDeleted，就可以返回NotFound了。
  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

// Yuanguo: when a number of bytes (randomly picked, e.g. 1.5MB) has been read, this function will
//   be called once; see DBIter::ParseKey in db/db_iter.cc;
bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;

  // Yuanguo: for each file overlapping with `ikey`, call `State::Match`;
  //   if there're more than one
  //       state.matches will be >=2, and
  //       state.stats.seek_file = the first one;
  //   (note: multiple files overlapping with one key ==> compactable)
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).

    // Yuanguo: multiple files overlapping with one key ==> compactable
    //   but not set compactable immediately, instead,
    //        1. decrement `allowed_seeks`;
    //        2. when `allowed_seeks` down to 0, set compactable (`file_to_compact_` and `file_to_compact_level_`);
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

//Yuanguo: at which level shall we flush a memtable [smallest_user_key, largest_user_key]?
//    1. if the memtable overlaps with Level-0, then flush it at Level-0;
//    2. else, not overlap with Level-0, but overlap with Level-N+1, then flush it at Level-N;
//    3. else, not overlap with Level-N+1, but overlaps too many with Level-N+2, then flush it at Level-N;
//    otherwise, N++;
// 之前，我以为一定flush在level-0上。看来不是这样的：假如没有overlap，尽量放在kMaxMemCompactLevel=2层。为什么？
// 看kMaxMemCompactLevel的注释。
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  //Yuanguo: if overlap with Level-0, then reutrn 0; else ...
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      //Yuanguo: if overlap with Level `level+1`, return `level`;
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      //Yuanguo: not overlap with Level `level+1`, but overlap too many with `level+2`, return `level`;
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// Yuanguo: 这里引入一个定义：
//    第level层的文件列表是(肯定是按最小key的从小到大排列，第0层也是一样，见VersionSet::Builder::SaveTo函数，那里生成一个Version)：
//      [f1, f2, f3, f4];
//    GetOverlappingInputs()输出的inputs = [f2, f3]
//      - 若f2的最小key和f1的最大key的user-key部分相同，则称为左边界是模糊的，反之为称之为清晰的；
//      - 若f3的最大key和f4的最小key的user-key部分相同，则称为右边界是模糊的，反之为称之为清晰的；
// GetOverlappingInputs()输出的边界都可能是模糊的。
//
// 对于Level>0：
//           f1                f2                 f3                 f4             f5
//      [a:555, d:777]    [d:716,  g:553]    [h:812, k:654]    [k:650, m:800]   [n:828, p:700]
//
//      注：有没有可以产生这种分布呢？是有的。见DBImpl::DoCompactionWork()函数。
//
// 若begin=e:*; end=j:*;
// 则inputs=[f2, f3];
//
// 注意：把f2和f3压缩到第level+1层是错误的：
//    - 右边：k:654被压缩到第level+1层，而k:650留在第level层。Get的时候就会返回k:650（因为逐层搜索），这是一个过时版本。
//    - 左边：没有问题。
//    - 可见右边界清晰是压缩正确性的保障。
//
// 如何解决呢？答案是AddBoundaryInputs()函数，它确保右边界是清晰的(把f4加加进去)
//
// 对于Level=0:
//   因为table file之间可能有overlap，所以，本函数把user key overlap的所有文件都找出来。所以，也就不存在上述问题。
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      //Yuanguo: it's very clear for level != 0, the only overlapping file is f;
      inputs->push_back(f);

      //Yuanguo: for, level==0:
      //                           user_begin       user_end
      //                              |               |
      //           f1s         f1l    |               |
      // file-1    |------------|     |               |
      //               f2s            |       f2l     |
      // file-2         |-------------+--------|      |
      //                              |     f3s       |            f3l
      // file-3                       |      |--------+-------------|
      //                              |               |        f4s      f4l
      // file-4                       |               |         |--------|
      //                              |               |                       f5s    f5l
      // file-5                       |               |                         |-----|
      //
      //
      // a. file-1 is skipped at first (see above);
      // b. but, when file-2 is encountered `user_begin` is set to `f2s` (file-2-start), and search is restarted,
      //    so file-1 is included (and `user_begin` will be set to `f1s`).
      // c. similarly, when file3 is encountered, `user_end` is set to `f3l` (file3-limit);
      // d. as a result, file-4 will be included;
      // e. file-5 will never be included;
      //
      // why expand the range like that?
      // Answer: for compaction, it would be incorrect if we only compact file-3 and file-4 to level-1, leaving
      // file-1 and file-2 in level-0: file-3 or file-4 contains newer versions of keys than file-1 and file-2;
      // 类似于本函数前面注释的情形。
      //
      //Yuanguo: Version::PickLevelForMemTableOutput()调用本函数时，传入的level肯定 > 0；

      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
//
// Yuanguo:
//   VersionSet::Builder applies a sequence of edits (added files or deleted files) to version `base_` (or called current), 
//   forming a new version `v`. It's used in VersionSet::Recover() and VersionSet::LogAndApply();
//
//   VersionSet::Builder works like this:
//       a. expand the edits, and store them in `LevelState levels_`, where
//          each level keeps its added/deleted files (changes or edits), see function `VersionSet::Builder::Apply()`;
//       b. merge `base_` and these edits, saving to version `v`, see function `VersionSet::Builder::SaveTo()`;
//
//   that is, `base_` ----> edit1, edit2, edit3, ... editN ----> `v`;
//
//   注意：它不生成中间version，像这样:
//             `base_` ----> edit1 ----> `v1`
//       ----> edit2 ----> `v2`
//       ----> ...
//       ---> `v`
//
//   而是把edit1, edit2, edit3, ... editN先合并起来，再作用于`base_`上，得到`v`;
//             `base_` ----> edit1, edit2, edit3, ... editN
//       ----> `base_` ----> merged_edit{1..N} (就是VersionSet::Builder::LevelState levels_[config::kNumLevels]);
//       ----> `v`
//
//   把edit1, edit2, edit3, ... editN合并起来产生merged_edit{1..N} (就是VersionSet::Builder::LevelState levels_[config::kNumLevels])
//   的逻辑在函数VersionSet::Builder::Apply()中；
//
//   `base_` + merged_edit{1..N} = `v` 的逻辑在 VersionSet::Builder::SaveTo()中；
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  // Yuanguo: comparator for std::set
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      //Yuanguo: 拷贝构造一个新的 FileMetaData 对象；
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      //Yuanguo:
      //  对于当前level，
      //      base_files = [10, 12, 15, 18, 23, 28, 30, 31]
      //      added_files = [16, 29]
      //  则按如下顺序调用MaybeAddFile():
      //      10, 12, 15, {{16}}, 18, 23, 28, {{29}}, 30, 31
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        //
        // Yuanguo: std::upper_bound returns an iterator pointing to the first element in the range [first, last) that is greater than value,
        //   or last if no such element is found.
        //
        //   for example, base_files = [10, 12, 15, 18, 23, 28, 30, 31]
        //                added_file = 16;
        //                then bpos = 18;
        //   then [10, 12, 15] may be added to `v`
        //
        //   For next round of loop, added_file = 29,
        //                then bpos = 30
        //   then [18, 23, 28] may be added to `v`
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        // Yuanguo: add 16, 29 respectively
        MaybeAddFile(v, level, added_file);
      }

      // Yuanguo: add 30, 31
      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  //Yuanguo:
  //     +----prev---- dummy_versions_ ----next----+
  //     |                  ^   ^                  |
  //     |                  |   |                  |
  //     +------------------+   +------------------+
  AppendVersion(new Version(this));
  //Yuanguo:
  //       +-----prev---------->---------------->----------------+
  //       |                                                     |
  //       |                                                     |
  //       |                                        <----prev----+
  // dummy_versions_   <----prev----   current_     ----next---->+
  //       ^           ----next---->    empty                    |
  //       |                                                     |
  //       |                                                     |
  //       +-------------------<----------------<-----next-------+
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  //Yuanguo:
  //       +-----prev---------->---------------->----------------+
  //       |                                                     |
  //       |                                                     |
  //       |                                        <----prev----+
  // dummy_versions_   <----prev----   current_     ----next---->+
  //       ^           ----next---->    empty                    |
  //       |                                                     |
  //       |                                                     |
  //       +-------------------<----------------<-----next-------+
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  //Yuanguo: the last appended is the `current_`;
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
  //Yuanguo:
  //       +-------prev------------------------>----------------------------->-------------------------+
  //       |                                                                                           |
  //       |                                                                                           |
  //       |                                                                              <----prev----+
  // dummy_versions_   <----prev----      vX        ----next---->         current_(vY)    ----next---->+
  //       ^           ----next---->     empty      <----prev----                                      |
  //       |                                                                                           |
  //       |                                                                                           |
  //       +------<----------------------------<----------------------------next-----------------------+
}

// Yuanguo: `current_` = apply `edit` on `current_`;
//             (v2)                          (v1)
//
// 注意：下面说的SNAPSHOT和LOG是Raft中的语义；LevelDB中也有snapshot的概念，和这个不是一回事。
//
//     0. `mu` is locked before calling this method;
//     1. `v` = apply `edit` on `current_`; See Builder;
//     2. create file `new_manifest_file` (MANIFEST-XXXXXX);
//     3. save SNAPSHOT (that is the `current_`, the base version) into `new_manifest_file`; NOTICE: SNAPSHOT (a version)
//        is also expressed by a VersionEdit object, see function `WriteSnapshot`;
//     4. unlock `mu`;
//     5. save LOG (that is `edit`) into `new_manifest_file`;
//     6. sync `new_manifest_file` and save its name in "dbname/CURRENT" atomically (by save tmp file and rename);
//     7. lock `mu`;
//     8. AppendVersion(v), that is `current_ = v`;
// NOTICE:
//     a. version transition: persist it first, then update it in memory;
//     b. essentially, this is "SNAPSHOT + LOG" model; base version is SNAPSHOT, `edit` is a list of LOGs; When
//        recover (see function VersionSet::Recover):
//               read name of `new_manifest_file` from "dbname/CURRENT";
//               read content of `new_manifest_file`, which is "SNAPSHOT + LOGs";
//               apply the edit on base version, and then we get current version;
//
// Yuanguo:
//     dbname/CURRENT ----------------------->  +---------------------------------------+              +-----------+
//                                              |            MANIFEST-XXXXXX            |        +---->|    sst    |
//                                              +---------------------------------------+        |     +-----------+
//                                              |                                       |        |
//                                              |  SNAPSHOT(base_)                      |        |     +-----------+
//                                              |  LOG(edit, or modification to base_)  | -------+---->|    sst    |
//                                              |                                       |     +------->+-----------+
//                                              |                                       |     |  |
//                                              +---------------------------------------+     |  |     +-----------+
//                                                                                            |  +---->|    sst    |
//                                                                                            +--|---->+-----------+
//                                              +---------------------------------------+     |  |
//                                              |            MANIFEST-YYYYYY            |     |  +---->+-----------+
//                                              +---------------------------------------+     +------->|    sst    |
//                                              |                                       |     |        +-----------+
//                                              |  SNAPSHOT(base_)                      | ----+
//                                              |  LOG(edit, or modification to base_)  |     |        +-----------+
//                                              |                                       |     +------->|    sst    |
//                                              |                                       |              +-----------+
//                                              +---------------------------------------+
//
//                                              ......
//
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  //Yuanguo:
  //  上面已经的 `v` = `current_` apply `edit`，其实就是新version；
  //  下面保存新version的时候，保存的是`current_`(见WriteSnapshot函数)和`edit`；为什么不直接保存`v`呢？
  //      因为正常情况下，descriptor_log_ != nullptr，对应的manifest中，已经保存了current_;
  //      这里只是把增量追加到descriptor_log_(manifest)中；

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;

  //Yuanguo:
  //   只有Recover的时候descriptor_log_ == nullptr才成立:
  //        系统重启时，切换一下manifest文件。
  //   一般情况下，manifest文件不切换(除了重启时，还有其他切换manifest的场景吗？)。例如，一个table file产生时，
  //   要产生一个新Version，调用本函数时，descriptor_log_是current_manifest的句柄，不为nullptr，
  //        - 往里追加一条VersionEdit (持久化新Version);
  //        - 然后AppendVersion() (改内存，使新Version可见);
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Yuanguo: 已经把 version v 持久化了(持久化的是 之前的 `current`和`edit`，它们合起来就是`v`)，
  //     现在修改内存状态：v变成current_;
  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

//Yuanguo:
//打开LevelDB数据库(1.第一次创建;2.重启)的时候调用本函数，
//    DB::Open() -->
//    DBImpl::Recover() -->
//    VersionSet::Recover()
//
//调用本函数的前提状态：
//    - 调用本函数的时候，*this (VersionSet) 刚被DB::Open() -> new DBImpl() -> new VersionSet() 构造出来，
//      所以current_是空的，即这样：
//                                 +-----prev---------->---------------->----------------+
//                                 |                                                     |
//                                 |                                                     |
//                                 |                                        <----prev----+
//                           dummy_versions_   <----prev----   current_     ----next---->+
//                                 ^           ----next---->    empty                    |
//                                 |                                                     |
//                                 |                                                     |
//                                 +-------------------<----------------<-----next-------+
//    - 无论是LevelDB数据库第一次创建，还是重启，都满足(见DBImpl::Recover()中的注释):
//          - 都有dbname_/CURRENT文件;
//          - dbname_/CURRENT文件的内容是"MANIFEST-000000X\n"，指向manifest文件"dbname_/MANIFEST-000000X";
Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  //Yuanguo: 读dbname_/CURRENT，其内容是"MANIFEST-000000X\n";
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  //Yuanguo: remove the trailing '\n'，变成: "MANIFEST-000000X"
  current.resize(current.size() - 1);

  //Yuanguo: 在前面拼上"dbname_/"，变成: "dbname_/MANIFEST-000000X";
  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  //Yuanguo: *this (VersionSet) 刚被DB::Open() -> new DBImpl() -> new VersionSet() 构造出来，所以current_是空的。
  Builder builder(this, current_);

  {
    //Yuanguo: 读文件"dbname_/MANIFEST-000000X"，把读到的内容(内容是VersionEdit对象的列表)Apply到builder;
    //   - 若是LevelDB第一次创建，这里面只有一个VersionEdit{log_number_ = 0;  next_file_number_ = 2; last_sequence_ = 0; ...}记录，见DBImpl::NewDB();
    //   - 否则，若是重启，里面可能有多个VersionEdit记录，是VersionSet::LogAndApply()写入的；
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    //Yuanguo: buidler中是current_(empty) + 从dbname_/MANIFEST-000000X文件中读出的VersionEdit；
    //  所以，可以构造出重启之前的current_;
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    //Yuanguo: AppendVersion(v)之后，Version链表一定是这样的：
    //      dummy_versions_    <-------->  empty_version  <--------> current_(v)
    //
    //  empty_version : 是在DB::Open() -> new DBImpl() -> new VersionSet()构造的空的Version;
    //  current_(v)   : 是empty_version + dbname_/MANIFEST-000000X中的VersionEdit;
    //
    //  假如是重启：
    //        current_(v)：重启之前的current_;
    //  假如是第一次创建LevelDB:
    //        current_(v)：empty_version + DBImpl::NewDB()中创建的VersionEdit;
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      //Yuanguo: reuse manifest，即current_不变：
      //     - dbname_/CURRENT 的内容不变: "MANIFEST-000000X\n"
      //     - 不产生新的 dbname_/MANIFEST-000000Y 文件；
      //     - Version链表：dummy_versions_    <-------->  empty_version  <--------> current_
      //
      // 假如是新建的LevelDB数据库，一定会reuse，见ReuseManifest:
      //      判断manifest文件的大小;
      //      因为是新建的，所以dbname_/MANIFEST-0000001文件很小;
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

//Yuanguo: 把所有version的所有level引用的table file的number放进live；
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    //Yuanguo: level-0层内的文件可能重合，需要把所有可能重合的文件都找出来一起压缩，否则，
    //  只compact一部分到level-1，而留一部分在level-0，就可能导致某写key的过时版本被留在
    //  level-0，而最新版本被压缩到level-1；这样Get的时候就返回过时版本。
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
// Yuanguo: 例如 largest_key = foo:999:kTypeValue
//    在level_files找最小的f, 满足: f->smallest = foo:X:*   (X<999)
//
//    也就是说：如果level_files中有:
//        f1->smallest = foo:900:*
//        f2->smallest = foo:998:*
//        f3->smallest = foo:725:*
//    则返回f2；
//
//    注意，根据InternalKeyComparator的定义:
//        foo:999:* < foo:998:* < foo:900:* < foo:725:*
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
//Yuanguo: 先看GetOverlappingInputs()函数前面的注释。
//Yuanguo: 假如:
//   - level_层内有文件：f1, f2, f3, f4, f5, f6，按从小到大排列；
//   - c->inputs_[0]: 是level_层内将要参与comapct的文件，例如: f2,f3
//   - f3的最大key是u1，f4的最小key是l2，并且user_key(u1) = user_key(l2);
//   - 此时要把f4也加入compact；
//   - 如果f4和f5也满足同样的关系，则f5也要加入compact；
//   - 依次类推……
// 为什么呢？例如，
//   - f3的最大key是"foo:999:kTypeValue"; f4的最小key是"foo:998:kTypeValue"; 注意：InternalKeyComparator定义的顺序，user_key相同，seq大的在前小的在后，
//     即 foo:999:* < foo:998:*
//   - 最新版本应该是"foo:999:kTypeValue";
//   - 假如把f3 compact到level_ + 1 层，而f4还留在level_层，则Get会返回过时版本"foo:998:kTypeValue"，
//     因为Get是逐层搜索。
// 相反，左边不需要类似的扩展。
//Yuanguo: 见GetOverlappingInputs函数前面注释关于"清晰"和"模糊"的定义。右边界清晰是压缩正确性的保障。本函数就是确保右边界清晰。
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    // Yuanguo: 例如 largest_key = foo:999:kTypeValue
    //   FindSmallestBoundaryFile()在level_files找最小的f, 满足: f->smallest = foo:X:*  (X<999)
    //
    //    也就是说：如果level_files中有:
    //        f1->smallest = foo:900:*
    //        f2->smallest = foo:998:*
    //        f3->smallest = foo:725:*
    //    则返回f2；
    //
    //    注意，根据InternalKeyComparator的定义:
    //        foo:999:* < foo:998:* < foo:900:* < foo:725:*
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  //Yuanguo: 见GetOverlappingInputs函数前面注释关于"清晰"和"模糊"的定义。右边界清晰是压缩正确性的保障。这里就是确保右边界清晰。
  //Yuanguo: 假如:
  //   - level_层内有文件：f1, f2, f3, f4, f5, f6，按从小到大排列；
  //   - c->inputs_[0]: 是level_层内将要参与comapct的文件，例如: f2,f3
  //   - f3的最大key是u1，f4的最小key是l2，并且user_key(u1) = user_key(l2);
  //   - 此时要把f4也加入compact；
  //   - 如果f4和f5也满足同样的关系，则f5也要加入compact；
  //   - 依次类推……
  // 为什么呢？例如，
  //   - f3的最大key是"foo:999:kTypeValue"; f4的最小key是"foo:998:kTypeValue"; 注意：InternalKeyComparator定义的顺序，user_key相同，seq大的在前小的在后，
  //     即 foo:999:* < foo:998:*
  //   - 最新版本应该是"foo:999:kTypeValue";
  //   - 假如把f3 compact到level_ + 1 层，而f4还留在level_层，则Get会返回过时版本"foo:998:kTypeValue"，
  //     因为Get是逐层搜索。
  // 相反，左边不需要类似的扩展。
  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  //Yuanguo: 至此，第level层的table files已经确定了。
  //         经过GetOverlappingInputs和上面的扩展，smallest/largest可能已经变了 (和VersionSet::CompactRange中的begin/end相比);
  //         VersionSet::CompactRange中的begin/end只是选择level层table files的"提示信息"：实际选择的时候，可能解决overlap引入的扩展(level=0)，
  //         也可能截断(level>0)，还有上面为了使右边界清晰引入的扩展。
  //         无论如何，现在第level层的table files已经选定了，VersionSet::CompactRange中的begin/end也就没有用了。
  //         下面取出的`smallest`和`largest`是level层的精确边界：左边可能是模糊的，右边一定是清晰的 (关于模糊和清晰的定义，见GetOverlappingInputs前的注释)。
  GetRange(c->inputs_[0], &smallest, &largest);

  //Yuanguo: 现在有了level层的边界[smallest, largest]，
  //         在level+1层，找这个边界"占据"的区间。
  //
  //例1：
  //                                       smallest             largest
  //     level:                               |--f2--| ... |--f4--|
  //     level+1:               |---F1---|       |----F2-------|     |---F3---|
  //
  //     c->inputs_[0] = [f2, f3, f4]
  //     c->inputs_[1] = [F2]
  //
  //例2：
  //                                       smallest             largest
  //     level:                               |--f2--| ... |--f4--|
  //     level+1:                      |---F1---| |----F2-----| |---F3---|
  //
  //     c->inputs_[0] = [f2, f3, f4]
  //     c->inputs_[1] = [F1, F2, F3]
  //
  //例3：
  //     level:        f1                f2                 f3                 f4             f5
  //              [a:555, d:777]    [d:716,  g:553]    [h:812, k:654]    [k:650, m:800]   [n:828, p:700]
  //
  //     level+1:      F1               F2              F3               F4              F5               F6        
  //              [a:414, b:433]   [b:431, d:480]   [d:471, h:492]   [h:487, m:499]   [m:490, n:455]  [n:411,  q:400]
  //
  //     c->inputs_[0] = [f2, f3, f4]        ----> 左边模糊，右边清晰
  //        smallest = d:716
  //        largest  = m:800
  //     c->inputs_[1] = [F2, F3, F4, F5]    ----> 左右两边都模糊，没有关系
  //
  //     注意：
  //       1. "占据"是按user key来判断的(虽然smallest和largest都是internal key，但是GetOverlappingInputs只使用user key部分)，所以F2和F5都被占据。
  //       2. 压缩不能产生比b:433更新的b，即b:x(x>433)。否则，压缩之后level+1层变成下面这样，就不是有序的了：
  //                          F1          ...               F*                        ...
  //                      [a:414, b:433]  ...   [*:*, ..., b的更高版本, ..., *:*]     ...
  //          当然，不会出现这样的情况，因为：
  //                - c->inputs_[0]中没有b:*，否则F1就被"占据"了；
  //                - 而c->inputs_[1]中最新的是b:431，所以产生的最高版本的b是b:431；
  //       3. 压缩可以产生比n:411更新的n。在我们这个例子中就发生了：
  //                - n:455就比n:411更新;
  //                - 且F5中还可能有n:456, n:457, ...;
  //       4. c->inputs_[0]: 左边模糊右边清晰，保证了level层的正确性。
  //       5. c->inputs_[1]: 左右两边都模糊，没有关系，level+1层还是正确的，因为：
  //          压缩产生的新文件[Fx, ..., Fy]将"顶替"[F2, F3, F4, F5]的位置，且其中
  //                - 最新版本的b是b:431 ( > F1.largest_internal_key)，因为c->inputs_[0]中没有b:*，否则F1被"占据"；
  //                - 最老版本的n是n:455 ( < F6.smallest_internal_key)，因为c->inputs_[0]中没有n:*，否则F6被"占据"；
  //           故level+1层还是有序无overlap的。
  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);

  //Yuanguo: 下面是一个优化而已：
  //       若在level+1层选择的文件不变的情况下，能够扩展一下level层，还满足上面所述的关系，那就扩展一下。
  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  //
  // Yuanguo:
  // Case-1：
  //     level:         |--f1--|   |--f2--|   |--f3--|    |--f4--|    |--f5--|      |--f6--|
  //     level+1:                |------------------fc----------------------------|
  //
  //     c->inputs_[0] = [f3, f4]
  //     c->inputs_[1] = [fc]
  // 可以把c->inputs_[0]扩展为 [f2, f3, f4, f5]，而c->inputs_[1]保持不变。
  //
  // Case-2：
  //     level:         |--f1--|   |--f2--|   |--f3--|    |--f4--|    |--f5--|      |--f6--|
  //     level+1:         |-----fb---| |------------fc----------------------------|
  //
  //     c->inputs_[0] = [f3, f4]
  //     c->inputs_[1] = [fc]
  // 可以把c->inputs_[0]扩展为 [f3, f4, f5]，而c->inputs_[1]保持不变。但不能把f2扩进去。
  //
  // Case-3：
  //     level:         |--f1--|   |--f2--|   |--f3--|    |--f4--|    |--f5--|      |--f6--|
  //     level+1:         |-----fb---| |------------fc---------------------|
  //
  //     c->inputs_[0] = [f3, f4]
  //     c->inputs_[1] = [fc]
  // 不可以把c->inputs_[0]扩展。
  //
  // 注意：扩展时要确保右边界是清晰的(调用AddBoundaryInputs函数)，因为这是正确性的保障。
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Yuanguo:
  //     - c->inputs_[0]的右边界是清晰的；左边界可能模糊；
  //     - c->inputs_[1]的两边都可能模糊；
  //     - c->grandparents_的两边都可能模糊；
  //
  // Yuanguo: 我通过实验，能够证实"c->inputs_[0]左边界可能模糊，c->inputs_[1]的两边都可能模糊"。实验是这样的：
  //     - 线程T1: 循环put k-0000000000 到 k-0099999999; val为1KB；
  //     - 线程T2: 循环db->GetSnapshot并持有0-20分钟(随机)；
  //               因为只有持有snapshot，压缩中才需要保留多版本，见DBImpl::DoCompactionWork()及其中的注释；
  //               只有保留了多版本，才能形成模糊的边界；
  //     - 线程T3: 循环随机生成一个range，调用db->CompactRange(...)来压缩它。然后睡眠10-30分钟。
  //     经过一段时间之后，发现:
  //
  //       1. c->inputs_[0]左边界是模糊的案例：level 2->3的压缩，level=2中的[fx]参与压缩，fx-1没参与压缩;
  //                                               fx-1                                                   fx
  //          level=2: ...,  [k-0067640127:1224513964, k-0069030877:1225202609]   [k-0069030877:1223966066, k-0072713310:1224705779], ...
  //
  //       2. c->inputs_[1]左边界是模糊的案例：level 2->3的压缩，level=3中的[Fy]参与压缩，Fy-1没参与压缩。
  //                                               Fy-1                                                  Fy
  //          level=3: ..., [k-0054891798:1127586311, k-0055046139:1147581221]    [k-0055046139:1136429341, k-0055103531:1137547804], ...
  //
  //       3. c->inputs_[1]右边界是模糊的案例：level 2->3的压缩，level=3中的[Fz]参与压缩，Fz+1没参与压缩。
  //                                                Fz                                                   Fz+1
  //          level=3: ..., [k-0049863471:1138372165, k-0050074683:1139138136]    [k-0050074683:1135341691, k-0050171661:1139083604], ...

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

//Yuanguo: 尽管本函数名字叫做CompactRange，它其实一点压缩的工作也没做，只是
// 在level层和level+1层各选择一些table file参与压缩而已。
//      level:    fM, fM+1, ..., fM+x
//      level+1:  fN, fN+1, ..., fN+y
// 这些文件会被压缩成:
//      level+1:  fO, fO+1, ..., fO+z
//
// 它是这么选择table files的：
//     step-1: 根据begin和end在第level层选择一些table files；注：
//                 若level=0，由于table file之间可能overlap，所以会扩展(扩展逻辑在GetOverlappingInputs中)；
//                 若level>0，若选择的table file的总size太大，则截断一部分(截断逻辑在本函数中)；
//     step-2: 若第level层选择的table files的右边界是"模糊"的，使之"清晰"(逻辑在SetupOtherInputs->AddBoundaryInputs中)
//     step-3: 现在第level层的table files确定了；根据它们，确定第level+1层的table files;
Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  // Yuanguo: 对于level>0，本来GetOverlappingInputs()选择的table file列表就可能是"右边界模糊"的(见GetOverlappingInputs前面的注释)。
  //   经过下面的"截断"，右边界仍然是模糊的。
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  //Yuanguo: 假如current_ (current version)在compact的过程中，又产生了新的version（例如immutable memtable flush下来），怎么办呢？
  //
  //                               +------ compact ----------> version.x+1
  //                               |
  //   current_ (version.x)  ------+
  //                               |
  //                               +------ new version ------> version.x+1
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
