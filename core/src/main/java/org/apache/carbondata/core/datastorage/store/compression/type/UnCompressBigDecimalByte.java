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
package org.apache.carbondata.core.datastorage.store.compression.type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.util.BigDecimalCompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Big decimal compression/uncompression
 */
public class UnCompressBigDecimalByte<T> implements UnCompressValue<T> {

  private BigDecimalCompressionFinder compressionFinder;

  private UnCompressValue leftPart;

  private UnCompressValue rightPart;

  private double divisionFactor;

  private long leftMaxValue;

  private long rightMaxValue;

  private boolean isDecimalPlacesNotZero;

  public UnCompressBigDecimalByte(BigDecimalCompressionFinder compressionFinder,
      UnCompressValue leftPart, UnCompressValue rightPart, int decimalPalacs,
      Object maxValueObject) {
    this.compressionFinder = compressionFinder;
    this.leftPart = leftPart;
    this.rightPart = rightPart;
    if (decimalPalacs > 0) {
      this.isDecimalPlacesNotZero = true;
    }
    this.divisionFactor = Math.pow(10, decimalPalacs);
    if (null != maxValueObject) {
      this.leftMaxValue = ((Long[]) maxValueObject)[0];
      this.rightMaxValue = ((Long[]) maxValueObject)[1];
    }
  }

  @Override public void setValue(T value) {
    byte[] values = (byte[]) value;
    ByteBuffer buffer = ByteBuffer.wrap(values);
    buffer.rewind();
    int leftPartLen = buffer.getInt();
    int rightPartLen = values.length - leftPartLen - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    byte[] leftValue = new byte[leftPartLen];
    byte[] rightValue = new byte[rightPartLen];
    buffer.get(leftValue);
    buffer.get(rightValue);
    leftPart.setValue(leftValue);
    rightPart.setValue(rightValue);
  }

  @Override public void setValueInBytes(byte[] value) {
    // TODO Auto-generated method stub

  }

  @Override public UnCompressValue<T> getNew() {
    UnCompressValue leftUnCompressClone = leftPart.getNew();
    UnCompressValue rightUnCompressClone = rightPart.getNew();
    return new UnCompressBigDecimal(compressionFinder, leftUnCompressClone, rightUnCompressClone);
  }

  @Override public UnCompressValue compress() {
    UnCompressBigDecimal byt =
        new UnCompressBigDecimal<>(compressionFinder, leftPart.compress(), rightPart.compress());
    return byt;
  }

  @Override
  public UnCompressValue uncompress(DataType dataType, byte[] data, int offset, int length,
      int decimalPlaces, Object maxValueObject) {
    ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
    int leftPathLength = buffer.getInt();
    int rightPartLength = length - leftPathLength - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    Long[] maxValue = (Long[])maxValueObject;
    ValueCompressonHolder.UnCompressValue left = leftPart
        .uncompress(compressionFinder.getLeftChangedDataType(), data,
            offset + CarbonCommonConstants.INT_SIZE_IN_BYTE, leftPathLength, decimalPlaces,
            maxValue[0]);
    ValueCompressonHolder.UnCompressValue right = rightPart
        .uncompress(compressionFinder.getRightChangedDataType(), data,
            offset + CarbonCommonConstants.INT_SIZE_IN_BYTE + leftPathLength, rightPartLength,
            decimalPlaces, maxValue[1]);
    return new UnCompressBigDecimalByte<>(compressionFinder, left, right, decimalPlaces,
        maxValueObject);
  }

  @Override public byte[] getBackArrayData() {
    return null;
  }

  @Override public UnCompressValue getCompressorObject() {
    return new UnCompressBigDecimal<>(compressionFinder, leftPart.getCompressorObject(),
        rightPart.getCompressorObject());
  }

  @Override public void setUncomressValue(T data, int decimalPlaces, Object maxValueObject) {
    //. do nothing
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException("Get long is not supported");
  }

  @Override public double getDoubleValue(int index) {
    throw new UnsupportedOperationException("Get double is not supported");
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

  @Override public void freeMemory() {
    leftPart.freeMemory();
    rightPart.freeMemory();
  }

}
