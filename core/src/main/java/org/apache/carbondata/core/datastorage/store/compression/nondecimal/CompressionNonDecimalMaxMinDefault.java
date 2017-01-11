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
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionNonDecimalMaxMinDefault extends ValueCompressionHolder<double[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionNonDecimalMaxMinDefault.class.getName());

  /**
   * doubleCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  /**
   * value.
   */
  private double[] value;

  private MeasureDataChunkStore<double[]> measureChunkStore;

  private BigDecimal maxValue;

  private double divisionFactor;

  @Override public void setValue(double[] value) {
    this.value = value;
  }

  @Override public double[] getValue() { return this.value; }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_DOUBLE, value);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToDoubleArray(buffer, value.length);
  }

  @Override
  public void uncompress(DataType dataType, byte[] compressedData,
      int offset, int length, int decimalPlaces, Object maxValueObject) {
    super.unCompress(compressor,dataType,compressedData, offset, length);
    setUncompressedValues(value, decimalPlaces, maxValueObject);
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException(
      "Long value is not defined for CompressionNonDecimalMaxMinDefault");
  }

  @Override public double getDoubleValue(int index) {
    double doubleValue = measureChunkStore.getDouble(index);
    BigDecimal diff = BigDecimal.valueOf(doubleValue / this.divisionFactor);
    return maxValue.subtract(diff).doubleValue();
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException(
      "Big decimal value is not defined for CompressionNonDecimalMaxMinDefault");
  }

  private void setUncompressedValues(double[] data, int decimalPlaces, Object maxValueObject) {
    this.measureChunkStore = MeasureChunkStoreFactory.INSTANCE
        .getMeasureDataChunkStore(DataType.DATA_DOUBLE, data.length);
    this.measureChunkStore.putData(data);
    this.maxValue = BigDecimal.valueOf((double) maxValueObject);
    this.divisionFactor = Math.pow(10, decimalPlaces);
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
