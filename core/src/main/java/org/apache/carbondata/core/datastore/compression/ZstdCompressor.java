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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.github.luben.zstd.Zstd;

public class ZstdCompressor implements Compressor {
  private static final int COMPRESS_LEVEL = 3;

  public ZstdCompressor() {
  }

  @Override
  public String getName() {
    return "zstd";
  }

  @Override
  public byte[] compressByte(byte[] unCompInput) {
    return Zstd.compress(unCompInput, COMPRESS_LEVEL);
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
  public byte[] compressShort(short[] unCompInput) {
    // short use 2 bytes
    byte[] unCompArray = new byte[unCompInput.length * 2];
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    for (short input : unCompInput) {
      unCompBuffer.putShort(input);
    }
    return Zstd.compress(unCompBuffer.array(), COMPRESS_LEVEL);
  }

  @Override
  public short[] unCompressShort(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    short[] shorts = new short[unCompArray.length / 2];
    for (int i = 0; i < shorts.length; i++) {
      shorts[i] = unCompBuffer.getShort();
    }
    return shorts;
  }

  @Override
  public byte[] compressInt(int[] unCompInput) {
    // int use 4 bytes
    byte[] unCompArray = new byte[unCompInput.length * 4];
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    for (int input : unCompInput) {
      unCompBuffer.putInt(input);
    }
    return Zstd.compress(unCompBuffer.array(), COMPRESS_LEVEL);
  }

  @Override
  public int[] unCompressInt(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    int[] ints = new int[unCompArray.length / 4];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = unCompBuffer.getInt();
    }
    return ints;
  }

  @Override
  public byte[] compressLong(long[] unCompInput) {
    // long use 8 bytes
    byte[] unCompArray = new byte[unCompInput.length * 8];
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    for (long input : unCompInput) {
      unCompBuffer.putLong(input);
    }
    return Zstd.compress(unCompBuffer.array(), COMPRESS_LEVEL);
  }

  @Override
  public long[] unCompressLong(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    long[] longs = new long[unCompArray.length / 8];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = unCompBuffer.getLong();
    }
    return longs;
  }

  @Override
  public byte[] compressFloat(float[] unCompInput) {
    // float use 4 bytes
    byte[] unCompArray = new byte[unCompInput.length * 4];
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    for (float input : unCompInput) {
      unCompBuffer.putFloat(input);
    }
    return Zstd.compress(unCompBuffer.array(), COMPRESS_LEVEL);
  }

  @Override
  public float[] unCompressFloat(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    float[] floats = new float[unCompArray.length / 4];
    for (int i = 0; i < floats.length; i++) {
      floats[i] = unCompBuffer.getFloat();
    }
    return floats;
  }

  @Override
  public byte[] compressDouble(double[] unCompInput) {
    // double use 8 bytes
    byte[] unCompArray = new byte[unCompInput.length * 8];
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    for (double input : unCompInput) {
      unCompBuffer.putDouble(input);
    }
    return Zstd.compress(unCompBuffer.array(), COMPRESS_LEVEL);
  }

  @Override
  public double[] unCompressDouble(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ByteBuffer unCompBuffer = ByteBuffer.wrap(unCompArray);
    double[] doubles = new double[unCompArray.length / 8];
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = unCompBuffer.getDouble();
    }
    return doubles;
  }

  @Override
  public long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException {
    throw new RuntimeException("Not implemented rawCompress for zstd yet");
  }

  @Override
  public long rawUncompress(byte[] input, byte[] output) throws IOException {
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
  public boolean supportUnsafe() {
    return false;
  }
}
