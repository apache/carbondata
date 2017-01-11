/*
3 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.carbondata.core.datastorage.store.compression.decimal;

import java.math.BigDecimal;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionMaxMinByte extends ValueCompressionHolder<byte[]> {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionMaxMinByte.class.getName());

  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  /**
   * value.
   */
  protected byte[] value;

  private MeasureDataChunkStore<byte[]> measureChunkStore;

  /**
   * actual data type
   */
  protected DataType actualDataType;

  private double maxValue;

  public CompressionMaxMinByte(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public byte[] getValue() {return this.value; }

  @Override public void setValue(byte[] value) {
    this.value = value;
  }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_BYTE, value);
  }

  @Override
  public void uncompress(DataType dataType, byte[] compressedData, int offset, int length,
      int decimalPlaces, Object maxValueObject) {
    super.unCompress(compressor, dataType, compressedData, offset, length);
    setUncompressedValues(value, maxValueObject);

  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;
  }

  @Override public long getLongValue(int index) {
    byte byteValue = measureChunkStore.getByte(index);
    return (long) (maxValue - byteValue);
  }

  @Override public double getDoubleValue(int index) {
    byte byteValue = measureChunkStore.getByte(index);
    return (maxValue - byteValue);
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException(
      "Big decimal value is not defined for CompressionMaxMinByte");
  }

  private void setUncompressedValues(byte[] data, Object maxValueObject) {
    this.measureChunkStore =
        MeasureChunkStoreFactory.INSTANCE.getMeasureDataChunkStore(DataType.DATA_BYTE, data.length);
    this.measureChunkStore.putData(data);
    if (maxValueObject instanceof Long) {
      this.maxValue = (long)maxValueObject;
    } else {
      this.maxValue = (double) maxValueObject;
    }
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
