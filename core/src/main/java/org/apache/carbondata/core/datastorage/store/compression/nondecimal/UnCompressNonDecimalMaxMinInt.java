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

package org.apache.carbondata.core.datastorage.store.compression.nondecimal;

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

public class UnCompressNonDecimalMaxMinInt implements UnCompressValue<int[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNonDecimalMaxMinInt.class.getName());
  /**
   * intCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();
  /**
   * value.
   */
  private int[] value;

  private MeasureDataChunkStore<int[]> measureChunkStore;
  private double divisionFactor;
  private BigDecimal maxValue;

  @Override public void setValue(int[] value) {
    this.value = value;

  }

  @Override public UnCompressValue getNew() {
    try {
      return (UnCompressValue) clone();
    } catch (CloneNotSupportedException ex1) {
      LOGGER.error(ex1, ex1.getMessage());
    }
    return null;
  }

  @Override public UnCompressValue compress() {

    UnCompressNonDecimalMaxMinByte byte1 = new UnCompressNonDecimalMaxMinByte();
    byte1.setValue(compressor.compressInt(value));
    return byte1;

  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToIntArray(buffer, value.length);
  }

  @Override public UnCompressValue getCompressorObject() {
    return new UnCompressNonDecimalMaxMinByte();
  }

  @Override
  public ValueCompressonHolder.UnCompressValue uncompress(DataType dataType, byte[] compressData,
      int offset, int length, int decimalPlaces, Object maxValueObject) {
    return null;
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException("Get long value is not supported");
  }

  @Override public double getDoubleValue(int index) {
    int intValue = measureChunkStore.getInt(index);
    if (intValue == 0) {
      return maxValue.doubleValue();
    } else {
      BigDecimal diff = BigDecimal.valueOf(intValue / this.divisionFactor);
      return maxValue.subtract(diff).doubleValue();
    }
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException("Get big decimal value is not supported");
  }

  @Override public void setUncompressValues(int[] data, int decimalPlaces, Object maxValueObject) {
    this.measureChunkStore =
        MeasureChunkStoreFactory.INSTANCE.getMeasureDataChunkStore(DataType.DATA_INT, data.length);
    this.measureChunkStore.putData(data);
    this.maxValue = BigDecimal.valueOf((double) maxValueObject);
    this.divisionFactor = Math.pow(10, decimalPlaces);

  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
