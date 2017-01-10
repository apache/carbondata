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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressNonDecimalMaxMinByte
    implements ValueCompressonHolder.UnCompressValue<byte[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNonDecimalMaxMinByte.class.getName());
  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();
  /**
   * value.
   */
  private byte[] value;

  private MeasureDataChunkStore<byte[]> measureChunkStore;

  private BigDecimal maxValue;

  private double divisionFactor;

  @Override public ValueCompressonHolder.UnCompressValue<byte[]> getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue<byte[]>) clone();
    } catch (CloneNotSupportedException cloneNotSupportedException) {
      LOGGER.error(cloneNotSupportedException, cloneNotSupportedException.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressNonDecimalMaxMinByte byte1 = new UnCompressNonDecimalMaxMinByte();
    byte1.setValue(compressor.compressByte(value));
    return byte1;
  }

  @Override
  public ValueCompressonHolder.UnCompressValue uncompress(DataType dataType, byte[] compressData,
      int offset, int length, int decimalPlaces, Object maxValueObject) {
    ValueCompressonHolder.UnCompressValue byte1 =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(dataType);
    ValueCompressonHolder
        .unCompress(dataType, byte1, compressData, offset, length, decimalPlaces, maxValueObject);
    return byte1;
  }

  @Override public byte[] getBackArrayData() {
    return value;
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressNonDecimalMaxMinByte();
  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;
  }

  @Override public void setValue(byte[] value) {
    this.value = value;
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException("Get long value is not supported");
  }

  @Override public double getDoubleValue(int index) {
    byte byteValue = measureChunkStore.getByte(index);
    if (byteValue == 0) {
      return this.maxValue.doubleValue();
    } else {
      BigDecimal diff = BigDecimal.valueOf(byteValue / this.divisionFactor);
      return maxValue.subtract(diff).doubleValue();
    }
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException("Get bigdecimal value is not supported");
  }

  @Override public void setUncompressValues(byte[] data, int decimalPlaces, Object maxValueObject) {
    this.measureChunkStore =
        MeasureChunkStoreFactory.INSTANCE.getMeasureDataChunkStore(DataType.DATA_BYTE, data.length);
    this.measureChunkStore.putData(data);
    this.maxValue = BigDecimal.valueOf((double) maxValueObject);
    this.divisionFactor = Math.pow(10, decimalPlaces);
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
