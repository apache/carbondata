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
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import org.apache.carbondata.core.util.ByteUtil;

public abstract class AbstractCompressor implements Compressor {

  @Override
  public byte[] compressShort(short[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_SHORT);
    unCompBuffer.order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public short[] unCompressShort(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    ShortBuffer unCompBuffer =
        ByteBuffer.wrap(unCompArray).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
    short[] shorts = new short[unCompArray.length / ByteUtil.SIZEOF_SHORT];
    unCompBuffer.get(shorts);
    return shorts;
  }

  @Override
  public byte[] compressInt(int[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_INT);
    unCompBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public int[] unCompressInt(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    IntBuffer unCompBuffer =
        ByteBuffer.wrap(unCompArray).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    int[] ints = new int[unCompArray.length / ByteUtil.SIZEOF_INT];
    unCompBuffer.get(ints);
    return ints;
  }

  @Override
  public byte[] compressLong(long[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_LONG);
    unCompBuffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public long[] unCompressLong(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    LongBuffer unCompBuffer =
        ByteBuffer.wrap(unCompArray).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    long[] longs = new long[unCompArray.length / ByteUtil.SIZEOF_LONG];
    unCompBuffer.get(longs);
    return longs;
  }

  @Override
  public byte[] compressFloat(float[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_FLOAT);
    unCompBuffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public float[] unCompressFloat(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    FloatBuffer unCompBuffer =
        ByteBuffer.wrap(unCompArray).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    float[] floats = new float[unCompArray.length / ByteUtil.SIZEOF_FLOAT];
    unCompBuffer.get(floats);
    return floats;
  }

  @Override
  public byte[] compressDouble(double[] unCompInput) {
    ByteBuffer unCompBuffer = ByteBuffer.allocate(unCompInput.length * ByteUtil.SIZEOF_DOUBLE);
    unCompBuffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().put(unCompInput);
    return compressByte(unCompBuffer.array());
  }

  @Override
  public double[] unCompressDouble(byte[] compInput, int offset, int length) {
    byte[] unCompArray = unCompressByte(compInput, offset, length);
    DoubleBuffer unCompBuffer =
        ByteBuffer.wrap(unCompArray).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
    double[] doubles = new double[unCompArray.length / ByteUtil.SIZEOF_DOUBLE];
    unCompBuffer.get(doubles);
    return doubles;
  }

  @Override
  public long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException {
    throw new RuntimeException("Not implemented rawCompress for " + this.getName());
  }

  @Override public boolean supportReusableBuffer() {
    return false;
  }

}
