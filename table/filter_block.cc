// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// Yuanguo: multiple data blocks may be merged and share a single filter; for example,
//          block_offset_0 / kFilterBase = 0
//          block_offset_1 / kFilterBase = 0
//          block_offset_2 / kFilterBase = 0
//          block_offset_3 / kFilterBase = 1
// then data block 0 to 2 share filter-0; when block_offset_3 is encountered, `GenerateFilter` is
// called and filter-0 is generated;
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // Yuanguo: which 2KB-boundary `block_offset` is in?
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  // Yuanguo: in a new 2KB-boundary, so new filter should be created;
  // Yuanguo: the prev data block is too big, 10KB for example, then 
  //          5 2KB-boundaries are passed, thus fill it with empty 
  //          filters;
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

//Yuanguo: the filter block:
//
//   DataBlock-0 offset = 0;
//   DataBlock-1 offset = 0.5K;
//   DataBlock-2 offset = 1.2K;  //very large, till 13K-1;
//   DataBlock-3 offset = 13K;
//   DataBlock-4 offset = 14.8K;
//   DataBlock-5 offset = 15.1K;
//
//   1. 0/2K = 0.5K/2K = 1.2K/2K = 0; so, DataBlock 0,1,2 share the filter-0;
//   2. Since DataBlock-3 starts at 13K, then 5 empty filters are filled in;
//
//   Why fill in the empty filters? 
//   To make `offset_` an array, which can be looked up by index: Suppose we know a DataBlock whose offset is 13K,
//   and we want to get the its filter. Then we can: 
//       index = 13K/2K = 6;     //`index = block_offset >> base_lg_` in FilterBlockReader::KeyMayMatch();
//       start = offset_[index]; //`start = DecodeFixed32(offset_ + index * 4)` in FilterBlockReader::KeyMayMatch();
//
//  data_ ---> +--------------------------------------------+
//             |filter-0: for block 0,1,2                   |
//             +--------------------------------------------+
//             |filter-6: for block 3                       |
//             +--------------------------------------------+
//             |filter-7: for block 4,5                     |
//             +--------------------------------------------+
//             |                  ......                    |
//             +--------------------------------------------+
//             |filter-N: for block M,M+1,...               |
// offset_ --> +--------------------------------------------+
//             |offset_[0]  filter-0 offset (4B)            |
//             |offset_[1]  empty (4B)                      |
//             |offset_[2]  empty (4B)                      |
//             |offset_[3]  empty (4B)                      |
//             |offset_[4]  empty (4B)                      |
//             |offset_[5]  empty (4B)                      |
//             |offset_[6]  filter-6 offset (4B)            |
//             |offset_[7]  filter-7 offset (4B)            |
//             |            ......                          |
//             |offset_[N]  filter-N offset (4B)            |
//             +--------------------------------------------+
//             |last_word: gap between data_ and offset_(4B)|
//             +--------------------------------------------+
//             |base_lg_ (1B)                               |
//             +--------------------------------------------+
//
//             block-X => filter-Y  if  block-X-offset >> base_lg_ == Y
//
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // Yuanguo: index of the filter;
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // Yuanguo: 
    //    start: offset of filter data;
    //    limit: offset of next filter data, also the end position of current filter data;
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
