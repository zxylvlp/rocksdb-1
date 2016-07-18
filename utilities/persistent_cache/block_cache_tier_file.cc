//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/block_cache_tier_file.h"

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <memory>
#include <vector>

#include "util/crc32c.h"
#include "utilities/persistent_cache/block_cache_tier_layout.h"

namespace rocksdb {

//
// File creation factories
//
Status NewWritableCacheFile(Env* const env, const std::string& filepath,
                            std::unique_ptr<WritableFile>* file,
                            const bool use_direct_writes = false) {
  EnvOptions opt;
  opt.use_direct_writes = use_direct_writes;
  Status s = env->NewWritableFile(filepath, file, opt);
  return s;
}

Status NewRandomAccessCacheFile(Env* const env, const std::string& filepath,
                                std::unique_ptr<RandomAccessFile>* file,
                                const bool use_direct_reads = true) {
  EnvOptions opt;
  opt.use_direct_reads = use_direct_reads;
  Status s = env->NewRandomAccessFile(filepath, file, opt);
  return s;
}

//
// BlockCacheFile
//
Status BlockCacheFile::Delete(uint64_t* size) {
  Status status = env_->GetFileSize(Path(), size);
  if (!status.ok()) {
    return status;
  }
  return env_->DeleteFile(Path());
}

//
// RandomAccessFile
//

bool RandomAccessCacheFile::Open(const bool enable_direct_reads) {
  WriteLock _(&rwlock_);
  return OpenImpl(enable_direct_reads);
}

bool RandomAccessCacheFile::OpenImpl(const bool enable_direct_reads) {
  rwlock_.AssertHeld();

  Debug(log_, "Opening cache file %s", Path().c_str());

  Status status = NewRandomAccessCacheFile(env_, Path(), &file_);
  if (!status.ok()) {
    Error(log_, "Error opening random access file %s. %s", Path().c_str(),
          status.ToString().c_str());
    return false;
  }

  return true;
}

bool RandomAccessCacheFile::Read(const LBA& lba, Slice* key, Slice* val,
                                 char* scratch) {
  ReadLock _(&rwlock_);

  assert(lba.cache_id_ == cache_id_);
  assert(file_);

  Slice result;
  Status s = file_->Read(lba.off_, lba.size_, &result, scratch);
  if (!s.ok()) {
    Error(log_, "Error reading from file %s. %s", Path().c_str(),
          s.ToString().c_str());
    return false;
  }

  assert(result.data() == scratch);

  return ParseRec(lba, key, val, scratch);
}

bool RandomAccessCacheFile::ParseRec(const LBA& lba, Slice* key, Slice* val,
                                     char* scratch) {
  Slice data(scratch, lba.size_);

  CacheRecord rec;
  if (!rec.Deserialize(data)) {
    assert(!"Error deserializing data");
    Error(log_, "Error de-serializing record from file %s off %d",
          Path().c_str(), lba.off_);
    return false;
  }

  *key = Slice(rec.key());
  *val = Slice(rec.val());

  return true;
}

//
// WriteableCacheFile
//

WriteableCacheFile::~WriteableCacheFile() {
  WriteLock _(&rwlock_);
  if (!eof_) {
    // This file never flushed. We give priority to shutdown since this is a
    // cache
    // TODO(krad): Figure a way to flush the pending data
    assert(file_);

    assert(refs_ == 1);
    --refs_;
  }
  ClearBuffers();
}

bool WriteableCacheFile::Create(const bool enable_direct_writes,
                                const bool enable_direct_reads) {
  WriteLock _(&rwlock_);

  enable_direct_reads_ = enable_direct_reads;

  Debug(log_, "Creating new cache %s (max size is %d B)", Path().c_str(),
        max_size_);

  Status s = env_->FileExists(Path());
  if (s.ok()) {
    Warn(log_, "File %s already exists. %s", Path().c_str(),
         s.ToString().c_str());
  }

  s = NewWritableCacheFile(env_, Path(), &file_);
  if (!s.ok()) {
    Warn(log_, "Unable to create file %s. %s", Path().c_str(),
         s.ToString().c_str());
    return false;
  }

  assert(!refs_);
  ++refs_;

  return true;
}

bool WriteableCacheFile::Append(const Slice& key, const Slice& val, LBA* lba) {
  WriteLock _(&rwlock_);

  if (eof_) {
    // We can't append since the file is full
    return false;
  }

  // estimate the space required to store the (key, val)
  const uint32_t rec_size = CacheRecord::CalcSize(key, val);
  const uint32_t index_rec_size = IndexRecord::CalcSize(key);
  const uint32_t footer_rec_size = FooterRecord::CalcSize();

  if (!ExpandBuffer(bufs_, rec_size)
      || !ExpandBuffer(index_, index_rec_size)
      || !ExpandBuffer(footer_, footer_rec_size)) {
    // unable to expand the buffer
    Debug(log_, "Error expanding buffers. size=%d", rec_size);
    return false;
  }

  lba->cache_id_ = cache_id_;
  lba->off_ = disk_woff_;
  lba->size_ = rec_size;

  CacheRecord cache_rec(key, val);
  if (!cache_rec.Serialize(&bufs_.data_, &bufs_.woff_)) {
    // unexpected error: unable to serialize the data
    assert(!"error serializing cache record");
    return false;
  }

  footer_record_.cache_nentry()++;

  IndexRecord index_rec(key, disk_woff_, rec_size);
  if (!index_rec.Serialize(&index_.data_, &index_.woff_)) {
    // unexpected error
    assert(!"error serializing index record");
    return false;
  }

  footer_record_.index_nentry()++;

  disk_woff_ += rec_size;
  eof_ = disk_woff_ >= max_size_;

  if (eof_) {
    Finalize();
  }

  // dispatch buffer for flush
  DispatchBuffer();

  return true;
}

bool WriteableCacheFile::ExpandBuffer(ElasticBuffer& bufs, const size_t size) {
  rwlock_.AssertHeld();
  assert(!eof_);

  // determine if there is enough space
  size_t free = 0;  // compute the free space left in buffer
  for (size_t i = bufs.woff_; i < bufs.data_.size(); ++i) {
    free += bufs.data_[i]->Free();
    if (size <= free) {
      // we have enough space in the buffer
      return true;
    }
  }

  // expand the buffer until there is enough space to write `size` bytes
  assert(free < size);
  while (free < size) {
    CacheWriteBuffer* const buf = alloc_->Allocate();
    if (!buf) {
      Debug(log_, "Unable to allocate buffers");
      return false;
    }

    size_ += static_cast<uint32_t>(buf->Free());
    free += buf->Free();
    bufs.data_.push_back(buf);
  }

  assert(free >= size);
  return true;
}

void WriteableCacheFile::DispatchBuffer() {
  rwlock_.AssertHeld();

  assert(bufs_.data_.size());
  assert(bufs_doff_ <= bufs_.woff_);
  assert(bufs_.woff_ <= bufs_.data_.size());

  if (pending_ios_) {
    return;
  }

  if (!eof_ && bufs_doff_ == bufs_.woff_) {
    // dispatch buffer is pointing to write buffer and we haven't hit eof
    return;
  }

  assert(eof_ || bufs_doff_ < bufs_.woff_);
  assert(bufs_doff_ < bufs_.data_.size());
  assert(file_);

  auto* buf = bufs_.data_[bufs_doff_];
  const uint64_t file_off = bufs_doff_ * alloc_->BufferSize();

  assert(!buf->Free() ||
         (eof_ && bufs_doff_ == bufs_.woff_ && bufs_.woff_ < bufs_.data_.size()));
  // we have reached end of file, and there is space in the last buffer
  // pad it with zero for direct IO
  buf->FillTrailingZeros();

  assert(buf->Used() % kFileAlignmentSize == 0);

  writer_->Write(file_.get(), buf, file_off,
                 std::bind(&WriteableCacheFile::BufferWriteDone, this));
  pending_ios_++;
  bufs_doff_++;
}

void WriteableCacheFile::BufferWriteDone() {
  WriteLock _(&rwlock_);

  assert(bufs_.data_.size());

  pending_ios_--;

  if (bufs_doff_ < bufs_.data_.size()) {
    DispatchBuffer();
  }

  if (eof_ && bufs_doff_ >= bufs_.data_.size() && !pending_ios_) {
    // end-of-file reached, move to read mode
    CloseAndOpenForReading();
  }
}

void WriteableCacheFile::Finalize() {
  assert(eof_);
  assert(!bufs_.data_.empty());
  assert(!index_.data_.empty());
  assert(!footer_.data_.empty());

  footer_record_.cache_pad_off() = disk_woff_;
  disk_woff_ += bufs_.data_[bufs_.woff_]->FillTrailingZeros();

  footer_record_.index_off() = disk_woff_;

  bufs_.data_.insert(bufs_.data_.end(), index_.data_.begin(),
                     index_.data_.end());
  disk_woff_ += index_.data_.size() * alloc_->BufferSize(); 

  footer_record_.index_pad_off() = disk_woff_;

  disk_woff_ += index_.data_[index_.woff_]->FillTrailingZeros();

  SerializeFooter();
  assert(!footer_.data_[footer_.woff_]->FillTrailingZeros());

  bufs_.data_.insert(bufs_.data_.end(), footer_.data_.begin(),
                     footer_.data_.end());
  disk_woff_ += footer_.data_.size() * alloc_->BufferSize();

  bufs_.woff_ += index_.data_.size();
  bufs_.woff_ += footer_.data_.size();

  index_.data_.clear();
  footer_.data_.clear();
}

void WriteableCacheFile::CloseAndOpenForReading() {
  // Our env abstraction do not allow reading from a file opened for appending
  // We need close the file and re-open it for reading
  Close();
  RandomAccessCacheFile::OpenImpl(enable_direct_reads_);
}

bool WriteableCacheFile::ReadBuffer(const LBA& lba, Slice* key, Slice* block,
                                    char* scratch) {
  rwlock_.AssertHeld();

  if (!ReadBuffer(lba, scratch)) {
    Error(log_, "Error reading from buffer. cache=%d off=%d", cache_id_,
          lba.off_);
    return false;
  }

  return ParseRec(lba, key, block, scratch);
}

bool WriteableCacheFile::ReadBuffer(const LBA& lba, char* data) {
  rwlock_.AssertHeld();

  assert(lba.off_ < disk_woff_);

  // we read from the buffers like reading from a flat file. The list of buffers
  // are treated as contiguous stream of data

  char* tmp = data;
  size_t pending_nbytes = lba.size_;
  // start buffer
  size_t start_idx = lba.off_ / alloc_->BufferSize();
  // offset into the start buffer
  size_t start_off = lba.off_ % alloc_->BufferSize();

  assert(start_idx <= bufs_.woff_);

  for (size_t i = start_idx; pending_nbytes && i < bufs_.data_.size(); ++i) {
    assert(i <= bufs_.woff_);
    auto* buf = bufs_.data_[i];
    assert(i == bufs_.woff_ || !buf->Free());
    // bytes to write to the buffer
    size_t nbytes = pending_nbytes > (buf->Used() - start_off)
                        ? (buf->Used() - start_off)
                        : pending_nbytes;
    memcpy(tmp, buf->Data() + start_off, nbytes);

    // left over to be written
    pending_nbytes -= nbytes;
    start_off = 0;
    tmp += nbytes;
  }

  assert(!pending_nbytes);
  if (pending_nbytes) {
    return false;
  }

  assert(tmp == data + lba.size_);
  return true;
}

void WriteableCacheFile::Close() {
  rwlock_.AssertHeld();

  assert(size_ >= max_size_);
  assert(disk_woff_ >= max_size_);
  assert(bufs_doff_ == bufs_.data_.size());
  assert(bufs_.data_.size() - bufs_.woff_ <= 1);
  assert(!pending_ios_);

  Info(log_, "Closing file %s. size=%d written=%d", Path().c_str(), size_,
       disk_woff_);

  ClearBuffers();
  file_.reset();

  assert(refs_);
  --refs_;
}

void WriteableCacheFile::ClearBuffers() {
  for (size_t i = 0; i < bufs_.data_.size(); ++i) {
    alloc_->Deallocate(bufs_.data_[i]);
  }

  bufs_.data_.clear();
}

//
// ThreadedFileWriter implementation
//
ThreadedWriter::ThreadedWriter(PersistentCacheTier* const cache,
                               const size_t qdepth, const size_t io_size)
    : Writer(cache), io_size_(io_size) {
  for (size_t i = 0; i < qdepth; ++i) {
    std::thread th(&ThreadedWriter::ThreadMain, this);
    threads_.push_back(std::move(th));
  }
}

void ThreadedWriter::Stop() {
  // notify all threads to exit
  for (size_t i = 0; i < threads_.size(); ++i) {
    q_.Push(IO(/*signal=*/true));
  }

  // wait for all threads to exit
  for (auto& th : threads_) {
    th.join();
    assert(!th.joinable());
  }
  threads_.clear();
}

void ThreadedWriter::Write(WritableFile* const file, CacheWriteBuffer* buf,
                           const uint64_t file_off,
                           const std::function<void()> callback) {
  q_.Push(IO(file, buf, file_off, callback));
}

void ThreadedWriter::ThreadMain() {
  while (true) {
    // Fetch the IO to process
    IO io(q_.Pop());
    if (io.signal_) {
      // that's secret signal to exit
      break;
    }

    // Reserve space for writing the buffer
    while (!cache_->Reserve(io.buf_->Used())) {
      // We can fail to reserve space if every file in the system
      // is being currently accessed
      /* sleep override */
      Env::Default()->SleepForMicroseconds(1000000);
    }

    DispatchIO(io);

    io.callback_();
  }
}

void ThreadedWriter::DispatchIO(const IO& io) {
  size_t written = 0;
  while (written < io.buf_->Used()) {
    Slice data(io.buf_->Data() + written, io_size_);
    Status s = io.file_->Append(data);
    assert(s.ok());
    if (!s.ok()) {
      // That is definite IO error to device. There is not much we can
      // do but ignore the failure. This can lead to corruption of data on
      // disk, but the cache will skip while reading
      fprintf(stderr, "Error writing data to file. %s\n", s.ToString().c_str());
    }
    written += io_size_;
  }
}

}  // namespace rocksdb

#endif
