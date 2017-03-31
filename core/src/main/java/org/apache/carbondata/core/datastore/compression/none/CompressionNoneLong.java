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

package org.apache.carbondata.core.datastore.compression.none;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionNoneLong extends ValueCompressionHolder<long[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionNoneLong.class.getName());
  /**
   * longCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();
  /**
   * value.
   */
  protected long[] value;

  private DataType actualDataType;

  private MeasureDataChunkStore<long[]> measureChunkStore;

  public CompressionNoneLong(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public void setValue(long[] value) {
    this.value = value;
  }

  @Override public long[] getValue() {
    return this.value;
  }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_LONG, value);
  }

  @Override
  public void uncompress(DataType dataType, byte[] data, int offset, int length, int decimalPlaces,
      Object maxValueObject, int numberOfRows) {
    super.unCompress(compressor, dataType, data, offset, length, numberOfRows, maxValueObject,
        decimalPlaces);
  }

  @Override public void setValueInBytes(byte[] byteValue) {
    ByteBuffer buffer = ByteBuffer.wrap(byteValue);
    this.value = ValueCompressionUtil.convertToLongArray(buffer, byteValue.length);
  }

  @Override public long getLongValue(int index) {
    return measureChunkStore.getLong(index);
  }

  @Override public double getDoubleValue(int index) {
    return measureChunkStore.getLong(index);
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException("Get big decimal is not supported");
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }

  @Override
  public void setValue(long[] data, int numberOfRows, Object maxValueObject, int decimalPlaces) {
    this.measureChunkStore = MeasureChunkStoreFactory.INSTANCE
        .getMeasureDataChunkStore(DataType.DATA_LONG, numberOfRows);
    this.measureChunkStore.putData(data);

  }
}
