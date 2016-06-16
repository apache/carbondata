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

package org.carbondata.core.datastorage.store.compression;

import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * ValueCompressonHolder class.
 */
public final class ValueCompressonHolder {

  /**
   * byteCompressor.
   */
  private static Compressor<byte[]> byteCompressor =
      SnappyCompression.SnappyByteCompression.INSTANCE;

  /**
   * shortCompressor.
   */
  private static Compressor<short[]> shortCompressor =
      SnappyCompression.SnappyShortCompression.INSTANCE;

  /**
   * intCompressor.
   */
  private static Compressor<int[]> intCompressor = SnappyCompression.SnappyIntCompression.INSTANCE;

  /**
   * longCompressor.
   */
  private static Compressor<long[]> longCompressor =
      SnappyCompression.SnappyLongCompression.INSTANCE;

  /**
   * floatCompressor
   */
  private static Compressor<float[]> floatCompressor =
      SnappyCompression.SnappyFloatCompression.INSTANCE;
  /**
   * doubleCompressor.
   */
  private static Compressor<double[]> doubleCompressor =
      SnappyCompression.SnappyDoubleCompression.INSTANCE;

  private ValueCompressonHolder() {

  }

  /**
   * @param dataType
   * @param value
   * @param data
   */
  public static void unCompress(DataType dataType, UnCompressValue value, byte[] data) {
    switch (dataType) {
      case DATA_BYTE:

        value.setValue(byteCompressor.unCompress(data));
        break;

      case DATA_SHORT:

        value.setValue(shortCompressor.unCompress(data));
        break;

      case DATA_INT:

        value.setValue(intCompressor.unCompress(data));
        break;

      case DATA_LONG:
      case DATA_BIGINT:

        value.setValue(longCompressor.unCompress(data));
        break;

      case DATA_FLOAT:

        value.setValue(floatCompressor.unCompress(data));
        break;
      default:

        value.setValue(doubleCompressor.unCompress(data));
        break;

    }
  }

  /**
   * interface for  UnCompressValue<T>.
   *
   * @param <T>
   */

  public interface UnCompressValue<T> extends Cloneable {
    //        Object getValue(int index, int decimal, double maxValue);

    void setValue(T value);

    void setValueInBytes(byte[] value);

    UnCompressValue<T> getNew();

    UnCompressValue compress();

    UnCompressValue uncompress(DataType dataType);

    byte[] getBackArrayData();

    UnCompressValue getCompressorObject();

    CarbonReadDataHolder getValues(int decimal, Object maxValue);

  }

}
