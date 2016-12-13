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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.xerial.snappy.Snappy;

public class SnappyCompressor implements Compressor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SnappyCompressor.class.getName());

  @Override
  public byte[] compressByte(byte[] unCompInput) {
    try {
      return Snappy.rawCompress(unCompInput, unCompInput.length);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override
  public byte[] unCompressByte(byte[] compInput) {
    try {
      return Snappy.uncompress(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return compInput;
  }

  @Override
  public byte[] compressShort(short[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override
  public short[] unCompressShort(byte[] compInput) {
    try {
      return Snappy.uncompressShortArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override
  public byte[] compressInt(int[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override
  public int[] unCompressInt(byte[] compInput) {
    try {
      return Snappy.uncompressIntArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override
  public byte[] compressLong(long[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override
  public long[] unCompressLong(byte[] compInput) {
    try {
      return Snappy.uncompressLongArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override
  public byte[] compressFloat(float[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override
  public float[] unCompressFloat(byte[] compInput) {
    try {
      return Snappy.uncompressFloatArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override
  public byte[] compressDouble(double[] unCompInput) {
    try {
      return Snappy.compress(unCompInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override
  public double[] unCompressDouble(byte[] compInput) {
    try {
      return Snappy.uncompressDoubleArray(compInput);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }
}
