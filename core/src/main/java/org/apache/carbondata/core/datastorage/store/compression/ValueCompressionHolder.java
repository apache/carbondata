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

import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * ValueCompressionHolder is the base class for handling
 * compression / decompression of the measure data chunk
 */
public abstract class ValueCompressionHolder<T> {

  /**
   * compressedValue
   */
  protected byte[] compressedValue;

  /**
   * @param compressor the compressor used to decompress the data
   * @param dataType data type of the data
   * @param data compressed data
   */
  public void unCompress(Compressor compressor, DataType dataType, byte[] data) {
    switch (dataType) {
      case DATA_BYTE:
        setValue((T)compressor.unCompressByte(data));
        break;
      case DATA_SHORT:
        setValue((T)compressor.unCompressShort(data));
        break;
      case DATA_INT:
        setValue((T)compressor.unCompressInt(data));
        break;
      case DATA_LONG:
      case DATA_BIGINT:
        setValue((T)compressor.unCompressLong(data));
        break;
      case DATA_FLOAT:
        setValue((T)compressor.unCompressFloat(data));
        break;
      default:
        setValue((T)compressor.unCompressDouble(data));
        break;
    }
  }

  /**
   * @param compressor the compressor used to compress the data
   * @param dataType data type of the data
   * @param data original data
   */
  public byte[] compress(Compressor compressor, DataType dataType, Object data) {
    switch (dataType) {
      case DATA_BYTE:
        return compressor.compressByte((byte[])data);
      case DATA_SHORT:
        return compressor.compressShort((short[])data);
      case DATA_INT:
        return compressor.compressInt((int[])data);
      case DATA_LONG:
      case DATA_BIGINT:
        return compressor.compressLong((long[])data);
      case DATA_FLOAT:
        return compressor.compressFloat((float[])data);
      case DATA_DOUBLE:
      default:
        return compressor.compressDouble((double[])data);
    }
  }

  public abstract void setValue(T value);

  public abstract T getValue();

  public abstract void setValueInBytes(byte[] value);

  public abstract void compress();

  public abstract void uncompress(DataType dataType, byte[] data);

  public byte[] getCompressedData() { return compressedValue; }

  public abstract CarbonReadDataHolder getValues(int decimal, Object maxValue);

}
