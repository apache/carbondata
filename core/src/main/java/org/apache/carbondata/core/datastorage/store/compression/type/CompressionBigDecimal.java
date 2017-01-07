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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
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

  @Override public void uncompress(DataType dataType, byte[] data) {
    byte[] values = data;
    ByteBuffer buffer = ByteBuffer.wrap(values);
    buffer.rewind();
    int leftPartLen = buffer.getInt();
    int rightPartLen = values.length - leftPartLen
            - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    byte[] leftValue = new byte[leftPartLen];
    byte[] rightValue = new byte[rightPartLen];
    buffer.get(leftValue);
    buffer.get(rightValue);
    leftPart.uncompress(compressionFinder.getLeftConvertedDataType(), leftValue);
    rightPart.uncompress(compressionFinder.getRightConvertedDataType(), rightValue);
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

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValue) {
    Long[] maxValues = (Long[]) maxValue;
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    CarbonReadDataHolder leftDataHolder = leftPart.getValues(decimal,
          maxValues[0]);
    long[] leftVals = leftDataHolder.getReadableLongValue();
    int size = leftVals.length;
    long[] rightVals = new long[size];
    if (decimal > 0) {
      CarbonReadDataHolder rightDataHolder = rightPart.getValues(decimal,
            maxValues[1]);
      rightVals = rightDataHolder.getReadableLongValue();
    }
    BigDecimal[] values = new BigDecimal[size];
    for (int i = 0; i < size; i++) {
      String decimalPart = Double.toString(rightVals[i]/Math.pow(10, decimal));
      String bigdStr = Long.toString(leftVals[i])
            + CarbonCommonConstants.POINT
            + decimalPart.substring(decimalPart.indexOf(".")+1, decimalPart.length());
      BigDecimal bigdVal = new BigDecimal(bigdStr);
      values[i] = bigdVal;
    }
    dataHolder.setReadableBigDecimalValues(values);
    return dataHolder;
  }

}
