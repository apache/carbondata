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

import org.apache.carbondata.common.Bytes;

public class DummyCompressor implements Compressor {
  @Override
  public byte[] compressByte(byte[] unCompInput) {
    return unCompInput;
  }

  @Override
  public byte[] unCompressByte(byte[] compInput) {
    return compInput;
  }

  @Override
  public byte[] compressShort(short[] unCompInput) {
    byte[] output = new byte[unCompInput.length * Bytes.SIZEOF_SHORT];
    for (int i = 0; i < unCompInput.length; i++) {
      Bytes.toBytes(unCompInput[i], output, i * Bytes.SIZEOF_SHORT);
    }
    return output;
  }

  @Override
  public short[] unCompressShort(byte[] compInput) {
    short[] output = new short[compInput.length / Bytes.SIZEOF_SHORT];
    for (int i = 0; i < output.length; i++) {
      output[i] = Bytes.toShort(compInput, i * Bytes.SIZEOF_SHORT);
    }
    return output;
  }

  @Override
  public byte[] compressInt(int[] unCompInput) {
    byte[] output = new byte[unCompInput.length * Bytes.SIZEOF_INT];
    for (int i = 0; i < unCompInput.length; i++) {
      Bytes.toBytes(unCompInput[i], output, i * Bytes.SIZEOF_INT);
    }
    return output;
  }

  @Override
  public int[] unCompressInt(byte[] compInput) {
    int[] output = new int[compInput.length / Bytes.SIZEOF_INT];
    for (int i = 0; i < output.length; i++) {
      output[i] = Bytes.toInt(compInput, i * Bytes.SIZEOF_INT);
    }
    return output;
  }

  @Override
  public byte[] compressLong(long[] unCompInput) {
    byte[] output = new byte[unCompInput.length * Bytes.SIZEOF_LONG];
    for (int i = 0; i < unCompInput.length; i++) {
      Bytes.toBytes(unCompInput[i], output, i * Bytes.SIZEOF_LONG);
    }
    return output;
  }

  @Override
  public long[] unCompressLong(byte[] compInput) {
    long[] output = new long[compInput.length / Bytes.SIZEOF_LONG];
    for (int i = 0; i < output.length; i++) {
      output[i] = Bytes.toLong(compInput, i * Bytes.SIZEOF_LONG);
    }
    return output;
  }

  @Override
  public byte[] compressFloat(float[] unCompInput) {
    byte[] output = new byte[unCompInput.length * Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < unCompInput.length; i++) {
      Bytes.toBytes(unCompInput[i], output, i * Bytes.SIZEOF_FLOAT);
    }
    return output;
  }

  @Override
  public float[] unCompressFloat(byte[] compInput) {
    float[] output = new float[compInput.length / Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < output.length; i++) {
      output[i] = Bytes.toFloat(compInput, i * Bytes.SIZEOF_FLOAT);
    }
    return output;
  }

  @Override
  public byte[] compressDouble(double[] unCompInput) {
    byte[] output = new byte[unCompInput.length * Bytes.SIZEOF_DOUBLE];
    for (int i = 0; i < unCompInput.length; i++) {
      Bytes.toBytes(unCompInput[i], output, i * Bytes.SIZEOF_DOUBLE);
    }
    return output;
  }

  @Override
  public double[] unCompressDouble(byte[] compInput) {
    double[] output = new double[compInput.length / Bytes.SIZEOF_DOUBLE];
    for (int i = 0; i < output.length; i++) {
      output[i] = Bytes.toDouble(compInput, i * Bytes.SIZEOF_DOUBLE);
    }
    return output;
  }

}

