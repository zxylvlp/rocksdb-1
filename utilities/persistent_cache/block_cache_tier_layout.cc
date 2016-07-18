#include "utilities/persistent_cache/block_cache_tier_layout.h"

#include <vector>
#include <sstream>

#include "util/crc32c.h"

namespace rocksdb {

class DiskLayoutHelper {
public:
  typedef std::vector<CacheWriteBuffer*>* bufs_t;

  static bool Append(bufs_t bufs, size_t* woff, const char* data,
                     const size_t size);

  template<class T>
  static bool Append(bufs_t bufs, size_t* woff, const T* t) {
    return Append(bufs, woff, reinterpret_cast<const char*>(t), sizeof(T));
  }

  static bool AppendSlice(bufs_t bufs, size_t* woff, const Slice& data) {
    uint32_t data_size = data.size();
    return Append(bufs, woff, &data_size) &&
           Append(bufs, woff, data.data(), data_size);
  }

  static bool Read(const Slice& data, size_t* roff, char* result,
                   const size_t result_size);

  template<class T>
  static bool Read(const Slice& data, size_t* roff, T* t) {
    return Read(data, roff, reinterpret_cast<char*>(t), sizeof(T));
  }

  static bool ReadSlice(const Slice& data, size_t *roff, Slice* result) {
    uint32_t result_size;
    if (!Read(data, roff, &result_size)) {
      return false;
    }

    *result = Slice(data.data() + *roff, result_size);
    *roff += result_size;
    return true;
  }

  template<class T>
  static uint32_t crc32cExtend(const uint32_t crc, const T& t) {
    return crc32c::Extend(crc, reinterpret_cast<const char*>(&t), sizeof(t));
  }

  static uint32_t crc32cExtend(const uint32_t crc, const Slice& data) {
    uint32_t result = crc;
    result = crc32cExtend(result, data.size());
    result = crc32c::Extend(result, reinterpret_cast<const char*>(data.data()),
                            data.size());
    return result;
  }

};

bool DiskLayoutHelper::Append(bufs_t bufs, size_t* woff, const char* data,
                              const size_t data_size) {
  assert(*woff < bufs->size());

  const char* p = data;
  size_t size = data_size;

  while (size && *woff < bufs->size()) {
    CacheWriteBuffer* buf = (*bufs)[*woff];
    const size_t free = buf->Free();
    if (size <= free) {
      buf->Append(p, size);
      size = 0;
    } else {
      buf->Append(p, free);
      p += free;
      size -= free;
      assert(!buf->Free());
      assert(buf->Used() == buf->Capacity());
    }

    if (!buf->Free()) {
      *woff += 1;
    }
  }

  assert(!size);
  return !size;
}

bool DiskLayoutHelper::Read(const Slice& data, size_t* off,
                            char* result, const size_t result_size) {
  assert(data.size()  >= *off + result_size);
  if (data.size() < *off + result_size) {
    return false;
  }

  void* ptr = memcpy(result, data.data() + *off, result_size);
  if (!ptr) {
    return false;
  }

  *off += result_size;
  return true;
}

//
// CacheRecord implementation
//
uint32_t CacheRecord::ComputeCRC() const {
  uint32_t crc = 0;
  crc = DiskLayoutHelper::crc32cExtend(crc, magic_);
  crc = DiskLayoutHelper::crc32cExtend(crc, key_);
  crc = DiskLayoutHelper::crc32cExtend(crc, val_);
  return crc;
}

bool CacheRecord::Serialize(std::vector<CacheWriteBuffer*>* bufs,
                            size_t* woff) {
  assert(bufs->size());
  crc_ = ComputeCRC();
  return DiskLayoutHelper::Append(bufs, woff, &magic_) &&
         DiskLayoutHelper::Append(bufs, woff, &crc_) &&
         DiskLayoutHelper::AppendSlice(bufs, woff, key_) &&
         DiskLayoutHelper::AppendSlice(bufs, woff, val_);
}

bool CacheRecord::Deserialize(const Slice& data) {
  const size_t min_size = CalcSize(/*key=*/ Slice(), /*val=*/ Slice());
  assert(data.size() >= min_size);
  if (data.size() < min_size) {
    return false;
  }

  size_t off = 0;
  bool ok = DiskLayoutHelper::Read(data, &off, &magic_) &&
            DiskLayoutHelper::Read(data, &off, &crc_) &&
            DiskLayoutHelper::ReadSlice(data, &off, &key_) &&
            DiskLayoutHelper::ReadSlice(data, &off, &val_);

  assert(ok);
  if (!ok) {
    return false;
  }

  uint32_t computed_crc = ComputeCRC();
  if (magic_ != MAGIC || computed_crc != crc_) {
    fprintf(stderr, "Inconsistent cache record. %s \n", ToString().c_str());
  }

  assert(magic_ == MAGIC && computed_crc == crc_);
  return magic_ == MAGIC && computed_crc == crc_;
}

std::string CacheRecord::ToString() const {
  std::ostringstream ss;
  ss << "Cache Record: " << std::endl
     << "** magic " << magic_ << std::endl
     << "** crc " << crc_ << std::endl
     << "** computed crc " << ComputeCRC() << std::endl
     << "** key size " << key_.size() << std::endl 
     << "** key " << key_.ToString(/*hex=*/ true) << std::endl
     << "** val size " << val_.size() << std::endl
     << "** val " << val_.ToString(/*hex=*/ true) << std::endl;
  return ss.str();
}

//
// IndexRecord implementation
//
uint32_t IndexRecord::ComputeCRC() const {
  uint32_t crc = 0;
  crc = DiskLayoutHelper::crc32cExtend(crc, magic_);
  crc = DiskLayoutHelper::crc32cExtend(crc, key_);
  crc = DiskLayoutHelper::crc32cExtend(crc, off_);
  crc = DiskLayoutHelper::crc32cExtend(crc, size_);
  return crc;
}

bool IndexRecord::Serialize(std::vector<CacheWriteBuffer*>* bufs,
                            size_t* woff) {
  assert(bufs->size());
  crc_ = ComputeCRC();
  return DiskLayoutHelper::Append(bufs, woff, &magic_) &&
         DiskLayoutHelper::Append(bufs, woff, &crc_) &&
         DiskLayoutHelper::AppendSlice(bufs, woff, key_) &&
         DiskLayoutHelper::Append(bufs, woff, &off_) &&
         DiskLayoutHelper::Append(bufs, woff, &size_);
}

bool IndexRecord::Deserialize(const Slice& data) {
  const size_t min_size = CalcSize(Slice());
  assert(data.size() >= min_size);
  if (data.size() < min_size) {
    return false;
  }

  size_t off = 0;
  bool ok = DiskLayoutHelper::Read(data, &off, &magic_) &&
            DiskLayoutHelper::Read(data, &off, &crc_) &&
            DiskLayoutHelper::ReadSlice(data, &off, &key_) &&
            DiskLayoutHelper::Read(data, &off, &off_) &&
            DiskLayoutHelper::Read(data, &off, &size_);

  if (!ok) {
    return false;
  }

  const uint32_t computed_crc = ComputeCRC();
  if (magic_ != MAGIC || crc_ != computed_crc) {
    fprintf(stderr, "Inconsistent index record read. %s \n", 
            ToString().c_str());
  }

  assert(magic_ == MAGIC && crc_ == computed_crc);
  return magic_ == MAGIC && crc_ == computed_crc;
}

std::string IndexRecord::ToString() const {
  std::ostringstream ss;
  ss << "Index Record: " << std::endl
     << "** magic " << magic_ << std::endl
     << "** crc " << crc_ << std::endl
     << "** computed crc " << ComputeCRC() << std::endl
     << "** off " << off_ << std::endl
     << "** size " << size_ << std::endl
     << "** key " << key_.ToString(/*hex=*/ true) << std::endl;
  return ss.str();
}

//
// FooterRecord implementation
//
uint32_t FooterRecord::ComputeCRC() const {
  uint32_t crc = 0;
  crc = DiskLayoutHelper::crc32cExtend(crc, magic_);
  crc = DiskLayoutHelper::crc32cExtend(crc, alignment_size_);
  crc = DiskLayoutHelper::crc32cExtend(crc, ver_);
  crc = DiskLayoutHelper::crc32cExtend(crc, cache_off_);
  crc = DiskLayoutHelper::crc32cExtend(crc, cache_nentry_);
  crc = DiskLayoutHelper::crc32cExtend(crc, cache_pad_off_);
  crc = DiskLayoutHelper::crc32cExtend(crc, index_off_);
  crc = DiskLayoutHelper::crc32cExtend(crc, index_nentry_);
  crc = DiskLayoutHelper::crc32cExtend(crc, index_pad_off_);
  return crc;
}

bool FooterRecord::Serialize(std::vector<CacheWriteBuffer*>* bufs,
                             size_t* woff) {
  assert(bufs->size());
  crc_ = ComputeCRC();
  return DiskLayoutHelper::Append(bufs, woff, &magic_) &&
         DiskLayoutHelper::Append(bufs, woff, &crc_) &&
         DiskLayoutHelper::Append(bufs, woff, &ver_) &&
         DiskLayoutHelper::Append(bufs, woff, &alignment_size_) &&
         DiskLayoutHelper::Append(bufs, woff, &cache_off_) &&
         DiskLayoutHelper::Append(bufs, woff, &cache_nentry_) &&
         DiskLayoutHelper::Append(bufs, woff, &cache_pad_off_) &&
         DiskLayoutHelper::Append(bufs, woff, &index_off_) &&
         DiskLayoutHelper::Append(bufs, woff, &index_nentry_) &&
         DiskLayoutHelper::Append(bufs, woff, &index_pad_off_);
}

bool FooterRecord::Deserialize(const Slice& data) {
  const size_t min_size = CalcSize();
  assert(data.size() >= min_size);
  if (data.size() < min_size) {
    return false;
  }

  size_t off = 0;
  bool ok = DiskLayoutHelper::Read(data, &off, &magic_) &&
            DiskLayoutHelper::Read(data, &off, &crc_) &&
            DiskLayoutHelper::Read(data, &off, &ver_) &&
            DiskLayoutHelper::Read(data, &off, &alignment_size_) &&
            DiskLayoutHelper::Read(data, &off, &cache_off_) &&
            DiskLayoutHelper::Read(data, &off, &cache_nentry_) &&
            DiskLayoutHelper::Read(data, &off, &cache_pad_off_) &&
            DiskLayoutHelper::Read(data, &off, &index_off_) &&
            DiskLayoutHelper::Read(data, &off, &index_nentry_) &&
            DiskLayoutHelper::Read(data, &off, &index_pad_off_);

  assert(ok);
  if (!ok) {
    return false;
  }

  const uint32_t computed_crc = ComputeCRC();
  if (magic_ != MAGIC || crc_ != computed_crc) {
    fprintf(stderr, "Inconsistet footer record read. %s \n",
            ToString().c_str());
  }

  assert(magic_ == MAGIC && crc_ == computed_crc);
  return magic_ == MAGIC && crc_ == computed_crc;
}

std::string FooterRecord::ToString() const {
  std::ostringstream ss;
  ss << "Footer Record: " << std::endl
     << "** magic " << magic_ << std::endl
     << "** crc " << crc_ << std::endl
     << "** computed crc " << ComputeCRC() << std::endl
     << "** ver " << ver_ << std::endl
     << "** alignement_size " << alignment_size_ << std::endl
     << "** cache_off " << cache_off_ << std::endl
     << "** cache_nentry " << cache_nentry_ << std::endl
     << "** cache_pad_off " << cache_pad_off_ << std::endl
     << "** index_off " << index_off_ << std::endl
     << "** index_nentry " << index_nentry_ << std::endl
     << "** index_pad_off " << index_pad_off_ << std::endl;
  return ss.str();
}

} // namespace rocksdb
