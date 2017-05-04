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

package org.apache.carbondata.core.datastore.compression.nondecimal;

import java.math.BigDecimal;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class CompressionNonDecimalMaxMinByte extends ValueCompressionHolder<byte[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionNonDecimalMaxMinByte.class.getName());

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

  @Override public void uncompress(DataType dataType, byte[] compressedData, int offset, int length,
      int decimalPlaces, Object maxValueObject, int numberOfRows) {
    super.unCompress(compressor, dataType, compressedData, offset, length, numberOfRows,
        maxValueObject, decimalPlaces);
  }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.BYTE, value);
  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;
  }

  @Override public void setValue(byte[] value) {
    this.value = value;
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException(
        "Long value is not defined for CompressionNonDecimalMaxMinByte");
  }

  @Override public double getDoubleValue(int index) {
    byte byteValue = measureChunkStore.getByte(index);
    BigDecimal diff = BigDecimal.valueOf(byteValue / this.divisionFactor);
    return maxValue.subtract(diff).doubleValue();

  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException(
        "Big decimal value is not defined for CompressionNonDecimalMaxMinByte");
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }

  @Override public byte[] getValue() {
    return this.value;
  }

  @Override
  public void setValue(byte[] data, int numberOfRows, Object maxValueObject, int decimalPlaces) {
    this.measureChunkStore = MeasureChunkStoreFactory.INSTANCE
        .getMeasureDataChunkStore(DataType.BYTE, numberOfRows);
    this.measureChunkStore.putData(data);
    this.maxValue = BigDecimal.valueOf((double) maxValueObject);
    this.divisionFactor = Math.pow(10, decimalPlaces);
  }
}
