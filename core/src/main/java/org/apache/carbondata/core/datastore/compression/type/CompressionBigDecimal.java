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
package org.apache.carbondata.core.datastore.compression.type;


import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.util.BigDecimalCompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Big decimal compression
 */
public class CompressionBigDecimal<T> extends ValueCompressionHolder<T> {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(CompressionBigDecimal.class.getName());

  private BigDecimalCompressionFinder compressionFinder;

  /**
   * leftPart before decimal
   */
  private ValueCompressionHolder leftPart;

  /**
   * rightPart after decimal
   */
  private ValueCompressionHolder rightPart;

  private double divisionFactor;

  private boolean isDecimalPlacesNotZero;

  public CompressionBigDecimal(BigDecimalCompressionFinder compressionFinder,
      ValueCompressionHolder leftPart, ValueCompressionHolder rightPart) {
    this.compressionFinder = compressionFinder;
    this.leftPart = leftPart;
    this.rightPart = rightPart;
  }

  @Override public void setValue(T value) {
    Object[] values = (Object[]) value;
    leftPart.setValue(values[0]);
    rightPart.setValue(values[1]);
  }

  @Override
  public void uncompress(DataType dataType, byte[] data, int offset, int length,
      int decimalPlaces, Object maxValueObject) {
    if (decimalPlaces > 0) {
      this.isDecimalPlacesNotZero = true;
    }
    this.divisionFactor = Math.pow(10, decimalPlaces);

    ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
    int leftPathLength = buffer.getInt();
    int rightPartLength = length - leftPathLength - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    Long[] maxValue = (Long[]) maxValueObject;
    leftPart.uncompress(compressionFinder.getLeftConvertedDataType(), data,
        offset + CarbonCommonConstants.INT_SIZE_IN_BYTE, leftPathLength, decimalPlaces,
        maxValue[0]);
    rightPart.uncompress(compressionFinder.getRightConvertedDataType(), data,
        offset + CarbonCommonConstants.INT_SIZE_IN_BYTE + leftPathLength, rightPartLength,
        decimalPlaces, maxValue[1]);
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException(
      "Long is not defined for CompressionBigDecimal");
  }

  @Override public double getDoubleValue(int index) {
    throw new UnsupportedOperationException(
      "Double is not defined for CompressionBigDecimal");
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    long leftValue = leftPart.getLongValue(index);
    long rightValue = 0;
    if (isDecimalPlacesNotZero) {
      rightValue = rightPart.getLongValue(index);
    }
    String decimalPart = Double.toString(rightValue / this.divisionFactor);
    String bigdStr = Long.toString(leftValue) + CarbonCommonConstants.POINT + decimalPart
            .substring(decimalPart.indexOf(".") + 1, decimalPart.length());
    return new BigDecimal(bigdStr);
  }

  @Override public T getValue() {
    Object[] values = new Object[2];
    values[0] = leftPart;
    values[1] = rightPart;
    return (T) values;
  }

  @Override public void setValueInBytes(byte[] value) {
    LOGGER.error("setValueInBytes() is not defined for CompressionBigDecimal");
  }

  @Override public void compress() {
    leftPart.compress();
    rightPart.compress();
  }

  @Override
  public byte[] getCompressedData() {
    byte[] leftdata = leftPart.getCompressedData();
    byte[] rightdata = rightPart.getCompressedData();
    ByteBuffer byteBuffer = ByteBuffer
        .allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE + leftdata.length
            + rightdata.length);
    byteBuffer.putInt(leftdata.length);
    byteBuffer.put(leftdata);
    byteBuffer.put(rightdata);
    byteBuffer.flip();
    return byteBuffer.array();
  }

  @Override public void freeMemory() {
    leftPart.freeMemory();
    rightPart.freeMemory();
  }

}
