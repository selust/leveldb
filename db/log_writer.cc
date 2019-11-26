// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

// 注意这里对kBlockSize进行了取模
// 这里不是表示的是文件的偏移量？
// 那表示的是什么？
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {// 可能不成立。
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    // 余下还需要多少才能填满一个块。
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);

    // 如果剩下的空间已经很小了。连个头部都放不下了。
    // 这里需要分两种case.
    // 1. leftover == 0
    // 2. 0 < leftover < kHeaderSize
    if (leftover < kHeaderSize) {
      // Switch to a new block
      // Case 2. 如果不等于0，那么还是需要把剩下的空间用0来填满的。
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // Case 1. 如果余下的等于0，那么也就什么都不用做了

      // 最后：
      // 移动到一个新的block的头上
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    // 这里有个恒等不变式
    // A. 也就是余下的空间必须要能够放一个header.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 这里不能直接换掉上面的assert
    // 这里用的是size_t，如果换到上面，恒等不变式是一直成立的。
    // avail指的就是当前这个block里面的可用空间
    // 如果直接看这里的可用空间，就是kBlockSize - 0 - kHeaderSize
    // 所以也只是把当前record的header占用的空间扣掉了。
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // left指的是sliceSizeToWrite.也就是dataToWrite
    // fragment_length指的就是后面需要写的数据量。主要是指当前这个bloc
    //    - 应该是在需要写入的数据量， 当前余下空间里面取最小值。
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 根据能写的数据的情况，来决定当前的这个record的类型。
    const bool end = (left == fragment_length);
    // 如果是从头开始写的，并且又可以直接把slice数据写完。
    // 那么肯定是fullType.
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      // 不能写完，但是是从头开始写
      type = kFirstType;
    } else if (end) {
      // 不是从头开始写，但是可以把数据写完
      type = kLastType;
    } else {
      // 不能从头开始写，也不能把数据写完
      type = kMiddleType;
    }

    // 这里提交一个物理的记录
    // 注意：可能这里并没有把一个slice写完。
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    // 移动写入指针。
    ptr += fragment_length;
    // 需要写入的数据相应减少
    left -= fragment_length;
    // 当然也不是从头开始写了。因为已经写过一次了。
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

//调用方需要保证不超出block的大小
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  // 根据前面的代码 fragment_length = min(avail, left)
  // 这里加上这个限制，应该是为了防止后面
  // block_offset_ + kHeaderSize + n溢出
  assert(length <= 0xffff);  // Must fit in two bytes
  // block_offset_是类成员变量。记录了在一个Block里面的偏移量。
  // block_offset_一定不能溢出。
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  // leveldb是一种小端写磁盘的情况
  // LevelDB使用的是小端字节序存储，低位字节排放在内存的低地址端
  // buf前面那个int是用来存放crc32的。
  char buf[kHeaderSize];
  // 写入长度: 这里先写入低8位
  buf[4] = static_cast<char>(length & 0xff);
  // 再写入高8位
  buf[5] = static_cast<char>(length >> 8);
  // 再写入类型
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // 这里是计算header和数据区的CRC32的值。具体过程不去关心。
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    // 当写完一个record之后，这里就立马flush
    // 但是有可能这个slice并不是完整的。
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  // 在一个block里面的写入位置往后移。
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
