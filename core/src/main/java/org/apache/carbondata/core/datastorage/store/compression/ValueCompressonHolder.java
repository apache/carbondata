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

import java.math.BigDecimal;

import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * ValueCompressonHolder class.
 */
public final class ValueCompressonHolder {

  /**
   * byteCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private ValueCompressonHolder() {

  }

  /**
   * @param dataType
   * @param value
   * @param data
   */
  public static void unCompress(DataType dataType, UnCompressValue value, byte[] data, int offset,
      int length, int decimal, Object maxValueObject) {
    switch (dataType) {
      case DATA_BYTE:
        value.setUncomressValue(compressor.unCompressByte(data, offset, length), decimal,
            maxValueObject);
        break;
      case DATA_SHORT:
        value.setUncomressValue(compressor.unCompressShort(data, offset, length), decimal,
            maxValueObject);
        break;
      case DATA_INT:
        value.setUncomressValue(compressor.unCompressInt(data, offset, length), decimal,
            maxValueObject);
        break;
      case DATA_LONG:
      case DATA_BIGINT:
        value.setUncomressValue(compressor.unCompressLong(data, offset, length), decimal,
            maxValueObject);
        break;
      case DATA_FLOAT:
        value.setUncomressValue(compressor.unCompressFloat(data, offset, length), decimal,
            maxValueObject);
        break;
      default:
        value.setUncomressValue(compressor.unCompressDouble(data, offset, length), decimal,
            maxValueObject);
        break;
    }
  }

  /**
   * interface for UnCompressValue<T>.
   */
  public interface UnCompressValue<T> extends Cloneable {

    void setValue(T value);

    void setValueInBytes(byte[] value);

    UnCompressValue<T> getNew();

    UnCompressValue compress();

    UnCompressValue uncompress(DataType dataType, byte[] compressData, int offset, int length,
        int decimal, Object maxValueObject);

    void setUncomressValue(T data, int decimal, Object maxValueObject);

    byte[] getBackArrayData();

    UnCompressValue getCompressorObject();

    long getLongValue(int index);

    double getDoubleValue(int index);

    BigDecimal getBigDecimalValue(int index);

    void freeMemory();

  }

}
