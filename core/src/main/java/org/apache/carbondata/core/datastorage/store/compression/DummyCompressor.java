/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.datastorage.store.compression;

import java.io.IOException;


public class DummyCompressor implements Compressor {
  @Override
  public byte[] compressByte(byte[] unCompInput) {
    return new byte[0];
  }

  @Override
  public byte[] unCompressByte(byte[] compInput) {
    return new byte[0];
  }

  @Override
  public byte[] compressShort(short[] unCompInput) {
    return new byte[0];
  }

  @Override
  public short[] unCompressShort(byte[] compInput) {
    return new short[0];
  }

  @Override
  public byte[] compressInt(int[] unCompInput) {
    return new byte[0];
  }

  @Override
  public int[] unCompressInt(byte[] compInput) {
    return new int[0];
  }

  @Override
  public byte[] compressLong(long[] unCompInput) {
    return new byte[0];
  }

  @Override
  public long[] unCompressLong(byte[] compInput) {
    return new long[0];
  }

  @Override
  public byte[] compressFloat(float[] unCompInput) {
    return new byte[0];
  }

  @Override
  public float[] unCompressFloat(byte[] compInput) {
    return new float[0];
  }

  @Override
  public byte[] compressDouble(double[] unCompInput) {
    byte[] output = new byte[unCompInput.length * 8];
    for (int i = 0; i < unCompInput.length; i++) {
      doubleToByteArray(unCompInput[i], output, i * 8);
    }
    return output;
  }

  @Override
  public double[] unCompressDouble(byte[] compInput) {
    double[] output = new double[compInput.length / 8];
    for (int i = 0; i < output.length; i++) {
      output[i] = byteArrayToDouble(compInput, i * 8);
    }
    return output;
  }

  private static void doubleToByteArray(double input, byte[] output, int start) {
    long l = Double.doubleToLongBits(input);
    output[start] = (byte)((l >> 56) & 0xff);
    output[start + 1] = (byte)((l >> 48) & 0xff);
    output[start + 2] = (byte)((l >> 40) & 0xff);
    output[start + 3] = (byte)((l >> 32) & 0xff);
    output[start + 4] = (byte)((l >> 24) & 0xff);
    output[start + 5] = (byte)((l >> 16) & 0xff);
    output[start + 6] = (byte)((l >> 8) & 0xff);
    output[start + 7] = (byte)((l >> 0) & 0xff);
  }

  private static double byteArrayToDouble(byte[] bytes, int start) {
    long l = ((long)bytes[start] << 56) +
        ((long)bytes[start + 1] << 48) +
        ((long)bytes[start + 2] << 40) +
        ((long)bytes[start + 3] << 32) +
        ((long)bytes[start + 4] << 24) +
        ((long)bytes[start + 5] << 16) +
        ((long)bytes[start + 6] << 8) +
        ((long)bytes[start + 7]);
    return Double.longBitsToDouble(l);
  }

}

