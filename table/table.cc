// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

//Yuanguo:
//  Table只有一个数据成员：Rep* const rep_;
//  Rep是Table的数据状态信息；
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

//Yuanguo: the table:
//             +============================================+
//             |                                            |
//             |                                            |
//             +--------------------------------------------+ <----------+
//             |/////////////////// block-0 ////////////////|            |
//             +--------------------------------------------+            |
//             |                                            |            |
//             +--------------------------------------------+ <-----+    |
//             |/////////////////// block-1 ////////////////|       |    |
//             +--------------------------------------------+       |    |
//             |                                            |       |    |
//             |                                            |       |    |
//             +--------------------------------------------+ <-----+----+------+
//             |///////////// index block //////////////////|       |    |      |
//             |// key1:block-0-handle (offset,size) ///////| ------+----+      |
//             |// key2:block-1-handle (offset,size) ///////| ------+           |
//             |// ... /////////////////////////////////////|                   |
//             +--------------------------------------------+                   |
//             |                                            |                   |
//             |                                            |                   |
//             +--------------------------------------------+ <----+            |
//             |////////////////////////////////////////////|      |            |
//             |////////////// filter block ////////////////|      |            |
//             |////////////////////////////////////////////|      |            |
//             +--------------------------------------------+      |            |
//             |                                            |      |            |
//             |                                            |      |            |
//             +--------------------------------------------+ <----+-------+    |
//             |//////////// metaindex block ///////////////|      |       |    |
//             |////////////////////////////////////////////|      |       |    |
//             |////// filter.: filter handle  /////////////| -----+       |    |
//             |////////////////////////////////////////////|              |    |
//             +--------------------------------------------+              |    |
//             |                                            |              |    |
//             |                                            |              |    |
//             |                                            |              |    |
//             +--------------------------------------------+              |    |
//   Footer    |   metaindex handle (offset,size)           | -------------+    |
//             |   index handle (offset,size)               | ------------------+
//             +============================================+
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  //Yuanguo: file是随机访问的文件；从它的结尾处读出Footer;
  //  把一个table看做一棵树的话，Footer是根，
  //     - 从它找到index block，进而找到data block；
  //     - 从它找到metaindex block，进而找到filter block;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // Yuanguo: 这是虽然能server requests，但其实只读出了index block(本函数)和filters(调用ReadMeta)，
    //    真正的data block没有读，get/list kv的时候再读，读完了之后放进options.block_cache里。
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //Yuanguo: 这里读出的是metaindex block，里面只有一个指向filter block的handle (offset, size)；
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  //Yuanguo: seek到"filter."之后，iter指向的Entry就是 key="filter." value={filter-block-handle，即filter block的offset, size}
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    //Yuanguo: 读filter block，输入参数是filter block的handle (offset, size);
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

//Yuanguo: 读filter block，输入参数是filter block的handle (offset, size);
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  //Yuanguo: 这里读出的是filter block；其结构见FilterBlockReader::FilterBlockReader()函数前的注释。
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  //Yuanguo: FilterBlockReader对象并不再读file，它已经拥有filter block的全部数据；它也没有file的指针。
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// Yuanguo: given table (arg) and block handle (index_value), return the iterator
//   of the block;
//     step-1. 获取block(默认有block_cache)
//               a. 若block_cache命中，则不用读file;
//               b. 若block_cache未命中，读file，构造block，并放入block_cache;
//     step-2. 构造block上的Iter;
//  可见：Block是读和缓存的单位，即读和缓存都是按整block进行。
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      // Yuanguo: block在cache中的key = 所属table的cache_id (Table::Rep::cache_id) + block在table中的offset;
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      // Yuanguo: there is no block cache, so delete block when iterator is destroyed;
      // 在Iterator上注册一个callback DeleteBlock，这个Iterator析构的时候调用DeleteBlock: 直接释放block占的内存；
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      // Yuanguo: there is block cache, release block (release the ref held by the iterator) when 
      //   iterator is destroyed;
      // 在Iterator上注册一个callback ReleaseBlock，这个Iterator析构的时候调用ReleaseBlock: 递减block的引用计数；
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// Yuanguo: return an iterator for this table, which is a two level iterator:
//   upper level: iterate over block handles in this table; each of the block handles is used to create the lower level
//                iterator;
//   lower level: iterate over a block (lower level iterator is created by Table::BlockReader, given this table and a
//                block handle);
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

//Yuanguo:
//  - Table::InternalGet()找"第一个>=k且和k有着相同user-key的kv-pair"，若找到，则对其调用handle_result；若找不到满足的，
//    就什么也不做。
//  - 但由于false-positive的作用，也可能找到user-key>k的kv-pair (只满足第一个条件，不满足第二个条件)，并对其调用handle_result;
//  - 所以，handle_result必须能够甄别并处理false-positive; handle_result的一个例子是db/version_set.cc : SaveValue();
//
//  以k=k08:99为例。本意是找: 第一个k08:x(x<99)。但由于以下两个原因，它做不到:
//        1. filter(我们只假设使用BloomFilterPolicy)具有false-positive的属性。也就是说，本来希望找第一个k08:x(x<99)，但有一
//           定的概率找到"k09:*", k10:*, ...
//        2. 没有使用filter：只要找到k08:x(x<99), k09:*, k10:*, ..., 就都算找到。
//  可以认为"没有使用filter"是使用了一种false-positive率为100%的filter；所以，统一认为是false-positive导致的。
//
//  总之，Table::InternalGet对外表现的行为是：在table中找"第一个>=k的kv-pair"，不一定和k有相同的user-key，然后对其调用handle_result。
//  为了保证正确性，handle_result必须能够甄别并处理false-positive。
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  // Yuanguo:
  // index_block内是一个个的index; index也是kv-pair: {IndexKey=>IndexVal}:
  //     - IndexVal是一个data-block的offset+len，我们说：IndexVal是一个data-block-handle，或者说IndexVal指向一个data-block;
  //     - IndexKey满足: 被指向的data-block的最大key <= IndexKey < 后一个data-block的最小key;
  //       为了简单, 认为"IndexKey=被指向的data-block的最大key"即可。通常也就是这样的。
  //     - 举个例子(注意，这里是Table内部，存的是"user-key:sequence"，是有序且不重复的)
  //
  //                 data-block-0     k00:378 => v
  //                                  k01:92  => v
  //                                  k02:523 => v
  //
  //                 data-block-1     k02:520 => v
  //                                  k03:327 => v
  //                                  k03:310 => v
  //
  //                 data-block-2     k04:35  => v
  //                                  k07:155 => v
  //                                  k07:154 => v
  //                                  k09:211 => v
  //
  //
  //                 data-block-3     k11:13  => v
  //                                  k12:680 => v
  //                                  k13:790 => v
  //
  //                 --------------------------------------------------------------------------------------------
  //
  //                 index_block      k02:523 => data-block-0的offset+len
  //                                  k03:300 => data-block-1的offset+len   注意，k03:300满足IndexKey的条件
  //                                  k09:211 => data-block-2的offset+len
  //                                  k13:790 => data-block-3的offset+len
  //
  // 以k="k08:99"为例:
  //
  //     - iiter->Seek(k)是在index_block内找: "第一个>=k08:99的index"，结果是{k09:211 => data-block-2的offset+len};
  //     - "第一个>=k08:99的kv-pair"一定在data-block-2内。因为：
  //          - 前面的data-block的最大key <= 它们的IndexKey < k08:99。例如:
  //                data-block-0的最大key(k02:523) = k02:523 < k08:99
  //                data-block-1的最大key(k03:310) < k03:300 < k08:99
  //          - 后面的data-block的最小key(k11:13) > k09:211 (IndexKey的定义：k09:211是data-block-2的IndexKey，小于后一个data-block的最小key)
  //            当然也>k08；
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    // Yuanguo: 第一个 >=k 的index {IndexKey=>IndexVal}存在; IndexVal(handle_value)就是data-block-handle，即offset+len;
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // Yuanguo: decode the block handle and test `k` by filter;
    //     filter测试结果若是不存在，则一定不存在；
    //     filter测试结果若是存在，则可能存在，也可能不存在(false-positive);
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
      // Yuanguo: 我们也不是要get k，而是找"第一个>=k的kv-pair"，filter如何判断"通过"还是"不通过"呢？
      //   答: 我们是要找"第一个>=k且和k有着相同user-key的kv-pair"；
      //       filter判断block内有没有"和k有着相同user-key的kv-pair":
      //          - 假如没有，那也不用找第一个>=k的了；
      //          - 假如filter认为有，那就找；但实际上也可能没有，进而找到的是k09:* (false-positive);
      //   注意：用户指定的BloomFilterPolicy，要经过InternalFilterPolicy包装一层，这一层是把InternalKey的sequence+type剥掉，
      //         只留下user-key存到bloom里。见DBImpl::internal_filter_policy_;
      //   另外：leveldb把整个user-key放进bloom里。
      //         RocksDB提供基于key-prefix的bloom (把key的prefix存到bloom，prefix的长度由用户指定)。
    } else {
      // Yuanguo: seek by block iterator; if valid, invoke callback on 'current' kv-pair;
      // 这里可能发生读file(若block在cache未命中)；
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      //Yuanguo: Seek找到的是"第一个>=k的kv-pair"。前面说过，由于false-positive，可能
      //            block_iter的user-key不一定和k的user-key相同。
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

//Yuanguo: table中第一个 >= k 的 kv-pair的大概位置(偏移);
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  // Yuanguo: seek by index iterator; if valid, index iterator points to the potential block;
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  // Yuanguo:
  //    - 在index block内找第一个 >=k 的kv-pair；
  //    - 这个kv-pair的value是一个data block handle，即指向一个data block;
  //    - 第一个 >=k 的kv-pair 若存在，只可能在这个data block中；
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    // Yuanguo: 第一个 >=k 的kv-pair 存在，返回它所在的data block的offset；
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      // Yuanguo: return the offset of the block containing the given key;
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // Yuanguo: 第一个 >=k 的kv-pair 不存在，那就是说 >=k 的kv-pair在本table的末尾;
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
