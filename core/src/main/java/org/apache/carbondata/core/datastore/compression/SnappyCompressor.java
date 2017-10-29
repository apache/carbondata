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
import java.lang.reflect.Field;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyNative;

public class SnappyCompressor implements Compressor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SnappyCompressor.class.getName());

  private final SnappyNative snappyNative;

  public SnappyCompressor() {
    Snappy snappy = new Snappy();
    Field privateField = null;
    try {
      privateField = snappy.getClass().getDeclaredField("impl");
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
    privateField.setAccessible(true);
    try {
      snappyNative = (SnappyNative) privateField.get(snappy);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "snappy";
  }

  @Override public byte[] compressByte(byte[] unCompInput) {
    try {
      return Snappy.rawCompress(unCompInput, unCompInput.length);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] compressByte(byte[] unCompInput, int byteSize) {
    try {
      return Snappy.rawCompress(unCompInput, byteSize);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] unCompressByte(byte[] compInput) {
    try {
      return Snappy.uncompress(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return compInput;
  }

  @Override public byte[] unCompressByte(byte[] compInput, int offset, int length) {
    int uncompressedLength = 0;
    byte[] data = null;
    try {
      uncompressedLength = Snappy.uncompressedLength(compInput, offset, length);
      data = new byte[uncompressedLength];
      Snappy.uncompress(compInput, offset, length, data, 0);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return data;
  }

  @Override public byte[] compressShort(short[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public short[] unCompressShort(byte[] compInput) {
    try {
      return Snappy.uncompressShortArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public short[] unCompressShort(byte[] compInput, int offset, int lenght) {
    try {
      return Snappy.uncompressShortArray(compInput, offset, lenght);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public byte[] compressInt(int[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public int[] unCompressInt(byte[] compInput) {
    try {
      return Snappy.uncompressIntArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public int[] unCompressInt(byte[] compInput, int offset, int length) {
    try {
      return Snappy.uncompressIntArray(compInput, offset, length);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public byte[] compressLong(long[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public long[] unCompressLong(byte[] compInput) {
    try {
      return Snappy.uncompressLongArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public long[] unCompressLong(byte[] compInput, int offset, int length) {
    try {
      return Snappy.uncompressLongArray(compInput, offset, length);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public byte[] compressFloat(float[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public float[] unCompressFloat(byte[] compInput) {
    try {
      return Snappy.uncompressFloatArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public float[] unCompressFloat(byte[] compInput, int offset, int length) {
    try {
      return Snappy.uncompressFloatArray(compInput, offset, length);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public byte[] compressDouble(double[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public double[] unCompressDouble(byte[] compInput) {
    try {
      return Snappy.uncompressDoubleArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public double[] unCompressDouble(byte[] compInput, int offset, int length) {
    try {
      int uncompressedLength = Snappy.uncompressedLength(compInput, offset, length);
      double[] result = new double[uncompressedLength / 8];
      snappyNative.rawUncompress(compInput, offset, length, result, 0);
      return result;
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override
  public long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException {
    return snappyNative.rawCompress(inputAddress, inputSize, outputAddress);
  }

  @Override
  public int maxCompressedLength(int inputSize) {
    return snappyNative.maxCompressedLength(inputSize);
  }
}
