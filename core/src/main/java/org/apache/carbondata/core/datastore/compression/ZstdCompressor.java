/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.compression;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.memory.UnsafeMemoryManager;

import com.github.luben.zstd.Zstd;

public class ZstdCompressor extends AbstractCompressor {
  private static final int COMPRESS_LEVEL = 3;

  public ZstdCompressor() {
  }

  @Override
  public String getName() {
    return "zstd";
  }

  @Override
  public ByteBuffer compressByte(ByteBuffer compInput) {
    compInput.flip();
    try {
      if (compInput.isDirect()) {
        return Zstd.compress(compInput, COMPRESS_LEVEL);
      } else {
        return ByteBuffer.wrap(Zstd.compress(compInput.array(), COMPRESS_LEVEL));
      }
    } finally {
      UnsafeMemoryManager.destroyDirectByteBuffer(compInput);
    }
  }

  @Override
  public ByteBuffer compressByte(byte[] unCompInput) {
    return ByteBuffer.wrap(Zstd.compress(unCompInput, COMPRESS_LEVEL));
  }

  @Override
  public byte[] compressByte(byte[] unCompInput, int byteSize) {
    return Zstd.compress(unCompInput, COMPRESS_LEVEL);
  }

  @Override
  public byte[] unCompressByte(byte[] compInput) {
    long decompressedSize = Zstd.decompressedSize(compInput);
    return Zstd.decompress(compInput, (int) decompressedSize);
  }

  @Override
  public byte[] unCompressByte(byte[] compInput, int offset, int length) {
    // todo: how to avoid memory copy
    byte[] dstBytes = new byte[length];
    System.arraycopy(compInput, offset, dstBytes, 0, length);
    return unCompressByte(dstBytes);
  }

  @Override
  public long rawUncompress(byte[] input, byte[] output) {
    return Zstd.decompress(output, input);
  }

  @Override
  public long maxCompressedLength(long inputSize) {
    return Zstd.compressBound(inputSize);
  }

  /**
   * currently java version of zstd does not support this feature.
   * It may support it in upcoming release 1.3.5-3, then we can optimize this accordingly.
   */
  @Override
  public int unCompressedLength(byte[] data, int offset, int length) {
    throw new RuntimeException("Unsupported operation Exception");
  }

  @Override
  public int rawUncompress(byte[] data, int offset, int length, byte[] output) {
    throw new RuntimeException("Unsupported operation Exception");
  }
}
