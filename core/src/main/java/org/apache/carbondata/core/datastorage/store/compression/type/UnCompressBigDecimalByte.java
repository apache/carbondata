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
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.BigDecimalCompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Big decimal compression/uncompression
 */
public class UnCompressBigDecimalByte<T> implements UnCompressValue<T> {

  private BigDecimalCompressionFinder compressionFinder;

  private UnCompressValue leftPart;

  private UnCompressValue rightPart;

  public UnCompressBigDecimalByte(
      BigDecimalCompressionFinder compressionFinder, UnCompressValue leftPart,
      UnCompressValue rightPart) {
    this.compressionFinder = compressionFinder;
    this.leftPart = leftPart;
    this.rightPart = rightPart;
  }

  @Override
  public void setValue(T value) {
    byte[] values = (byte[]) value;
    ByteBuffer buffer = ByteBuffer.wrap(values);
    buffer.rewind();
    int leftPartLen = buffer.getInt();
    int rightPartLen = values.length - leftPartLen
        - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    byte[] leftValue = new byte[leftPartLen];
    byte[] rightValue = new byte[rightPartLen];
    buffer.get(leftValue);
    buffer.get(rightValue);
    leftPart.setValue(leftValue);
    rightPart.setValue(rightValue);
  }

  @Override
  public void setValueInBytes(byte[] value) {
    // TODO Auto-generated method stub

  }

  @Override
  public UnCompressValue<T> getNew() {
    UnCompressValue leftUnCompressClone = leftPart.getNew();
    UnCompressValue rightUnCompressClone = rightPart.getNew();
    return new UnCompressBigDecimal(compressionFinder, leftUnCompressClone,
        rightUnCompressClone);
  }

  @Override
  public UnCompressValue compress() {
    UnCompressBigDecimal byt = new UnCompressBigDecimal<>(compressionFinder,
        leftPart.compress(), rightPart.compress());
    return byt;
  }

  @Override
  public UnCompressValue uncompress(DataType dataType) {
    ValueCompressonHolder.UnCompressValue left = leftPart
        .uncompress(compressionFinder.getLeftConvertedDataType());
    ValueCompressonHolder.UnCompressValue right = rightPart
        .uncompress(compressionFinder.getRightConvertedDataType());
    return new UnCompressBigDecimalByte<>(compressionFinder, left, right);
  }

  @Override
  public byte[] getBackArrayData() {
    return null;
  }

  @Override
  public UnCompressValue getCompressorObject() {
    return new UnCompressBigDecimal<>(compressionFinder,
        leftPart.getCompressorObject(), rightPart.getCompressorObject());
  }

  @Override
  public CarbonReadDataHolder getValues(int decimal, Object maxValue) {
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
