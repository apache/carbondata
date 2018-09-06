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
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import org.apache.carbondata.core.util.ByteUtil;

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
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_SHORT);
    unCompBuffer.asShortBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public short[] unCompressShort(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ShortBuffer unCompBuffer = ByteBuffer.wrap(unCompArray).asShortBuffer();
    short[] shorts = new short[unCompArray.length / ByteUtil.SIZEOF_SHORT];
    unCompBuffer.get(shorts);
    return shorts;
  }

  @Override
  public byte[] compressInt(int[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_INT);
    unCompBuffer.asIntBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public int[] unCompressInt(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    IntBuffer unCompBuffer = ByteBuffer.wrap(unCompArray).asIntBuffer();
    int[] ints = new int[unCompArray.length / ByteUtil.SIZEOF_INT];
    unCompBuffer.get(ints);
    return ints;
  }

  @Override
  public byte[] compressLong(long[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_LONG);
    unCompBuffer.asLongBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public long[] unCompressLong(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    LongBuffer unCompBuffer = ByteBuffer.wrap(unCompArray).asLongBuffer();
    long[] longs = new long[unCompArray.length / ByteUtil.SIZEOF_LONG];
    unCompBuffer.get(longs);
    return longs;
  }

  @Override
  public byte[] compressFloat(float[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_FLOAT);
    unCompBuffer.asFloatBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public float[] unCompressFloat(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    FloatBuffer unCompBuffer = ByteBuffer.wrap(unCompArray).asFloatBuffer();
    float[] floats = new float[unCompArray.length / ByteUtil.SIZEOF_FLOAT];
    unCompBuffer.get(floats);
    return floats;
  }

  @Override
  public byte[] compressDouble(double[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_DOUBLE);
    unCompBuffer.asDoubleBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public double[] unCompressDouble(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    DoubleBuffer unCompBuffer = ByteBuffer.wrap(unCompArray).asDoubleBuffer();
    double[] doubles = new double[unCompArray.length / ByteUtil.SIZEOF_DOUBLE];
    unCompBuffer.get(doubles);
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
