#pragma once

#ifndef ROCKSDB_LITE

#include <vector>

#include "rocksdb/slice.h"
#include "utilities/persistent_cache/block_cache_tier_file_buffer.h"


namespace rocksdb {

class OnDiskRecord {
 public:
  virtual ~OnDiskRecord() {}

  virtual bool Serialize(std::vector<CacheWriteBuffer*>* bufs, size_t* woff) = 0;
  virtual bool Deserialize(const Slice& buf) = 0;
};

//
// CacheRecord
//
// Cache record represents the record on disk
//
// +--------+---------+----------+------------+---------------+-------------+
// | magic  | crc     | key size | value size | key data      | value data  |
// +--------+---------+----------+------------+---------------+-------------+
// <-- 4 --><-- 4  --><-- 4   --><-- 4     --><-- key size  --><-- v-size -->
//
class CacheRecord : public OnDiskRecord {
 public:
  CacheRecord() {}
  explicit CacheRecord(const Slice& _key, const Slice& _val)
    : key_(_key), val_(_val) {}

  bool Serialize(std::vector<CacheWriteBuffer*>* bufs, size_t* woff) override;
  bool Deserialize(const Slice& buf) override;

  static uint32_t CalcSize(const Slice& _key, const Slice& _val) {
    return sizeof(uint32_t) /* magic_ */
           + sizeof(uint32_t) /* off_ */
           + sizeof(uint32_t) + _key.size() /* key_ */
           + sizeof(uint32_t) + _val.size(); /* val_ */
  }

  Slice& key() { return key_; }
  Slice& val() { return val_; }

 private:
  static const uint32_t MAGIC = 0xfefa;

  uint32_t ComputeCRC() const;
  std::string ToString() const;

  uint32_t magic_ = MAGIC;
  uint32_t crc_ = 0;
  Slice key_;
  Slice val_;
};

//
// Index Record
//
class IndexRecord : public OnDiskRecord {
 public:
  explicit IndexRecord(const Slice& key, const uint64_t off,
                       const uint32_t size)
    : key_(key), off_(off), size_(size) {}

  bool Serialize(std::vector<CacheWriteBuffer*>* bufs, size_t* woff) override;
  bool Deserialize(const Slice& buf) override;

  static size_t CalcSize(const Slice& key) {
    return sizeof(uint32_t) /* magic_ */
           + sizeof(uint32_t) /* crc_ */
           + sizeof(uint32_t) + key.size() /* key_ */
           + sizeof(uint64_t) /* off_ */
           + sizeof(uint32_t); /* size_ */
  }

 private:
  static const uint32_t MAGIC = 0xfefb;

  uint32_t ComputeCRC() const;
  std::string ToString() const;

  uint32_t magic_ = MAGIC;
  uint32_t crc_ = 0;
  Slice key_;
  uint64_t off_ = 0xbaddbaddULL;
  uint32_t size_ = 0xbadd;
};

//
// Footer Record
//
class FooterRecord : public OnDiskRecord {
 public:
  FooterRecord() {}
  explicit FooterRecord(const uint64_t _cache_off, const uint64_t _cache_nentry,
                        const uint64_t _cache_pad_off,
                        const uint64_t _index_off,
                        const uint64_t _index_nentry,
                        const uint64_t _index_pad_off)
    : cache_off_(_cache_off), cache_nentry_(_cache_nentry)
    , cache_pad_off_(_cache_pad_off), index_off_(_index_off)
    , index_nentry_(_index_nentry), index_pad_off_(_index_pad_off) {}

  bool Serialize(std::vector<CacheWriteBuffer*>* bufs, size_t* woff) override;
  bool Deserialize(const Slice& buf) override;

  uint64_t& cache_nentry() { return cache_nentry_; }
  uint64_t& cache_pad_off() { return cache_pad_off_; }
  uint64_t& index_off() { return index_off_; }
  uint64_t& index_nentry() { return cache_nentry_; }

  static size_t CalcSize() {
    const size_t size  = sizeof(uint32_t) /* magic_ */
           + sizeof(uint32_t) /* crc_ */
           + sizeof(uint32_t) /* ver_ */
           + sizeof(uint32_t) /* alignment_size_ */
           + sizeof(uint64_t) /* cache_off_ */
           + sizeof(uint64_t) /* cache_nentry_ */
           + sizeof(uint64_t) /* cache_pad_off_ */
           + sizeof(uint64_t) /* index_off_ */
           + sizeof(uint64_t) /* index_nentry_ */
           + sizeof(uint64_t); /* index_pad_off_ */
    return size ;
  }

 private:
  static const uint32_t MAGIC = 0xfefc;

  uint32_t ComputeCRC() const;
  std::string ToString() const;

  uint32_t magic_ = MAGIC;
  uint32_t crc_ = 0;
  uint32_t ver_ = 0;
  uint32_t alignment_size_ = 0;
  uint64_t cache_off_ = 0;
  uint64_t cache_nentry_ = 0;
  uint64_t cache_pad_off_ = 0;
  uint64_t index_off_ = 0;
  uint64_t index_nentry_ = 0;
  uint64_t index_pad_off_ = 0;
};

}

#endif // #ifndef ROCKSDB_LITE
