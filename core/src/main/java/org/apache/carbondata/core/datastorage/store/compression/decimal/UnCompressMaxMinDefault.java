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
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressMaxMinDefault implements ValueCompressonHolder.UnCompressValue<double[]> {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressMaxMinDefault.class.getName());

  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();
  /**
   * value.
   */
  private double[] value;

  private MeasureDataChunkStore<double[]> measureChunkStore;

  /**
   * actual data type
   */
  private DataType actualDataType;

  private double maxValue;

  public UnCompressMaxMinDefault(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public void setValue(double[] value) {
    this.value = value;

  }

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException ex5) {
      LOGGER.error(ex5, ex5.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressMaxMinByte byte1 = new UnCompressMaxMinByte(actualDataType);
    byte1.setValue(compressor.compressDouble(value));
    return byte1;
  }

  @Override
  public UnCompressValue uncompress(DataType dataType, byte[] compressData, int offset, int length,
      int decimalPlaces, Object maxValueObject) {
    return null;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToDoubleArray(buffer, value.length);
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressMaxMinByte(actualDataType);
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException("Get long value is not supported");
  }

  @Override public double getDoubleValue(int index) {
    double doubleValue = measureChunkStore.getDouble(index);
    if (doubleValue == 0) {
      return maxValue;
    } else {
      return maxValue - doubleValue;
    }
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException("Get big decimal value is not supported");
  }

  @Override
  public void setUncompressValues(double[] data, int decimalPlaces, Object maxValueObject) {
    this.measureChunkStore = MeasureChunkStoreFactory.INSTANCE
        .getMeasureDataChunkStore(DataType.DATA_DOUBLE, data.length);
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
