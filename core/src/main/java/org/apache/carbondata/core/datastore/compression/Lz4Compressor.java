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
import java.util.Arrays;

import org.apache.carbondata.core.util.ByteUtil;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * This class is implementation of lz4 compressor for column compression.
 *
 * Since lz4 decompression needs original size of data,
 * we specially add it at the beginning of compressed data
 */
public class Lz4Compressor implements Compressor {

  private LZ4Compressor compressor;
  private LZ4FastDecompressor decompressor;

  public Lz4Compressor() {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    compressor = factory.fastCompressor();
    decompressor = factory.fastDecompressor();
  }

  @Override
  public String getName() {
    return "lz4";
  }

  /**
   * here add original size in the very beginning for decompress
   * implementation refers to https://github.com/lz4/lz4-java/issues/119
   */
  @Override
  public byte[] compressByte(byte[] unCompInput) {
    // get max compressed length
    int maxCompressedLength = compressor.maxCompressedLength(unCompInput.length);
    // prepare space, add 4 bytes for length
    byte[] compressed = new byte[maxCompressedLength + ByteUtil.SIZEOF_INT];
    // compress to temp space
    int compressedLength = compressor.compress(unCompInput, 0, unCompInput.length,
        compressed, ByteUtil.SIZEOF_INT , maxCompressedLength);
    // add src length in the very beginning
    compressed[0] = (byte)unCompInput.length;
    compressed[1] = (byte)(unCompInput.length >> 8);
    compressed[2] = (byte)(unCompInput.length >> 16);
    compressed[3] = (byte)(unCompInput.length >> 24);
    return Arrays.copyOf(compressed, compressedLength + ByteUtil.SIZEOF_INT);
  }

  @Override
  public byte[] compressByte(byte[] unCompInput, int byteSize) {
    return compressor.compress(unCompInput);
  }

  /**
   * Returns the decompressed length of compressed data
   */
  public int getDecompressedLength(byte[] src, int srcOff) {
    return (src[srcOff] & 0xFF) | (src[srcOff + 1] & 0xFF) << 8 |
        (src[srcOff + 2] & 0xFF) << 16 | src[srcOff + 3] << 24;
  }

  @Override
  public byte[] unCompressByte(byte[] compInput) {
    int destLen = getDecompressedLength(compInput, 0);
    return decompressor.decompress(compInput, ByteUtil.SIZEOF_INT, destLen);
  }

  @Override
  public byte[] unCompressByte(byte[] compInput, int offset, int length) {
    int destLen = getDecompressedLength(compInput, offset);
    return decompressor.decompress(compInput, offset + ByteUtil.SIZEOF_INT, destLen);
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
    throw new RuntimeException("Not implemented rawCompress for lz4 yet");
  }

  @Override
  public long rawUncompress(byte[] input, byte[] output) throws IOException {
    decompressor.decompress(input, output);
    return getDecompressedLength(input, 0);
  }

  @Override
  public long maxCompressedLength(long inputSize) {
    // input value will be checked inside the method
    return compressor.maxCompressedLength((int)inputSize);
  }

  @Override
  public boolean supportUnsafe() {
    return false;
  }
}
