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

package org.apache.carbondata.core.datastorage.store.compression.decimal;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionMaxMinInt extends ValueCompressionHolder<int[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionMaxMinInt.class.getName());

  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private MeasureDataChunkStore<int[]> measureChunkStore;

  /**
   * value.
   */
  private int[] value;

  private DataType actualDataType;

  private double maxValue;

  public CompressionMaxMinInt(DataType actualType) {
    this.actualDataType = actualType;
  }

  @Override public void setValue(int[] value) {
    this.value = value;
  }

  @Override public int[] getValue() { return this.value; }

  @Override
  public void uncompress(DataType dataType, byte[] compressedData, int offset, int length,
      int decimalPlaces, Object maxValueObject) {
    super.unCompress(compressor, dataType, compressedData, offset, length);
    setUncompressedValues(value, maxValueObject);
  }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_INT, value);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToIntArray(buffer, value.length);
  }

  @Override public long getLongValue(int index) {
    int intValue = measureChunkStore.getInt(index);
    return (long) (maxValue - intValue);
  }

  @Override public double getDoubleValue(int index) {
    int intValue = measureChunkStore.getInt(index);
    return maxValue - intValue;
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException(
      "Big decimal value is not defined for CompressionMaxMinInt");
  }

  private void setUncompressedValues(int[] data, Object maxValueObject) {
    this.measureChunkStore =
        MeasureChunkStoreFactory.INSTANCE.getMeasureDataChunkStore(DataType.DATA_INT, data.length);
    this.measureChunkStore.putData(data);
    if (maxValueObject instanceof Long) {
      this.maxValue = (long) maxValueObject;
    } else {
      this.maxValue = (double) maxValueObject;
    }
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
