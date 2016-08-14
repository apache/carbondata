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

public class SnappyCompression {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SnappyCompression.class.getName());

  /**
   * SnappyByteCompression.
   */
  public static enum SnappyByteCompression implements Compressor<byte[]> {
    /**
     *
     */
    INSTANCE;

    /**
     * wrapper method for compressing byte[] unCompInput.
     */
    public byte[] compress(byte[] unCompInput) {
      try {
        return Snappy.rawCompress(unCompInput, unCompInput.length);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return null;
      }
    }

    /**
     * wrapper method for unCompress byte[] compInput.
     *
     * @return byte[].
     */
    public byte[] unCompress(byte[] compInput) {
      try {
        return Snappy.uncompress(compInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      }
      return compInput;
    }
  }

  /**
   * enum class for SnappyDoubleCompression.
   */
  public static enum SnappyDoubleCompression implements Compressor<double[]> {
    /**
     *
     */
    INSTANCE;

    /**
     * wrapper method for compressing double[] unCompInput.
     */
    public byte[] compress(double[] unCompInput) {
      try {
        return Snappy.compress(unCompInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return null;
      }
    }

    /**
     * wrapper method for unCompress byte[] compInput.
     *
     * @param compInput byte[].
     * @return double[].
     */
    public double[] unCompress(byte[] compInput) {
      try {
        return Snappy.uncompressDoubleArray(compInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      }
      return null;
    }

  }

  /**
   * enum class for SnappyShortCompression.
   *
   * @author S71955
   */
  public static enum SnappyShortCompression implements Compressor<short[]> {
    /**
     *
     */
    INSTANCE;

    /**
     * wrapper method for compress short[] unCompInput.
     *
     * @param unCompInput short[].
     * @return byte[].
     */
    public byte[] compress(short[] unCompInput) {
      try {
        return Snappy.compress(unCompInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return null;
      }
    }

    /**
     * wrapper method for uncompressShortArray.
     *
     * @param compInput byte[].
     * @return short[].
     */
    public short[] unCompress(byte[] compInput) {
      try {
        return Snappy.uncompressShortArray(compInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      }
      return null;
    }
  }

  /**
   * enum class for SnappyIntCompression.
   */
  public static enum SnappyIntCompression implements Compressor<int[]> {
    /**
     *
     */
    INSTANCE;

    /**
     * wrapper method for compress int[] unCompInput.
     *
     * @param unCompInput int[].
     * @return byte[].
     */
    public byte[] compress(int[] unCompInput) {
      try {
        return Snappy.compress(unCompInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return null;
      }
    }

    /**
     * wrapper method for uncompressIntArray.
     *
     * @param compInput byte[].
     * @return int[].
     */
    public int[] unCompress(byte[] compInput) {
      try {
        return Snappy.uncompressIntArray(compInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      }
      return null;
    }
  }

  /**
   * enum class for SnappyLongCompression.
   */
  public static enum SnappyLongCompression implements Compressor<long[]> {
    /**
     *
     */
    INSTANCE;

    /**
     * wrapper method for compress long[] unCompInput.
     *
     * @param unCompInput long[].
     * @return byte[].
     */
    public byte[] compress(long[] unCompInput) {
      try {
        return Snappy.compress(unCompInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return null;
      }
    }

    /**
     * wrapper method for uncompressLongArray.
     *
     * @param compInput byte[].
     * @return long[].
     */
    public long[] unCompress(byte[] compInput) {
      try {
        return Snappy.uncompressLongArray(compInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      }
      return null;
    }
  }

  /**
   * enum class for SnappyFloatCompression.
   */

  public static enum SnappyFloatCompression implements Compressor<float[]> {
    /**
     *
     */
    INSTANCE;

    /**
     * wrapper method for compress float[] unCompInput.
     *
     * @param unCompInput float[].
     * @return byte[].
     */
    public byte[] compress(float[] unCompInput) {
      try {
        return Snappy.compress(unCompInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return null;
      }
    }

    /**
     * wrapper method for uncompressFloatArray.
     *
     * @param compInput byte[].
     * @return float[].
     */
    public float[] unCompress(byte[] compInput) {
      try {
        return Snappy.uncompressFloatArray(compInput);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      }
      return null;
    }
  }

}
