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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.BigDecimalCompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Big decimal compression/uncompression
 */
public class UnCompressBigDecimal<T> implements UnCompressValue<T> {

  private BigDecimalCompressionFinder compressionFinder;

  /**
   * leftPart before decimal
   */
  private UnCompressValue leftPart;

  /**
   * rightPart after decimal
   */
  private UnCompressValue rightPart;

  public UnCompressBigDecimal(BigDecimalCompressionFinder compressionFinder,
      UnCompressValue leftPart, UnCompressValue rightPart) {
    this.compressionFinder = compressionFinder;
    this.leftPart = leftPart;
    this.rightPart = rightPart;
  }

  @Override
  public void setValue(T value) {
    Object[] values = (Object[]) value;
    leftPart.setValue(values[0]);
    rightPart.setValue(values[1]);
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBackArrayData() {
    byte[] leftdata = leftPart.getBackArrayData();
    byte[] rightdata = rightPart.getBackArrayData();
    ByteBuffer byteBuffer = ByteBuffer
        .allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE + leftdata.length
            + rightdata.length);
    byteBuffer.putInt(leftdata.length);
    byteBuffer.put(leftdata);
    byteBuffer.put(rightdata);
    byteBuffer.flip();
    return byteBuffer.array();
  }

  @Override
  public UnCompressValue getCompressorObject() {
    return new UnCompressBigDecimalByte<>(compressionFinder,
        leftPart.getCompressorObject(), rightPart.getCompressorObject());
  }

  @Override
  public CarbonReadDataHolder getValues(int decimal, Object maxValue) {
    // TODO Auto-generated method stub
    return null;
  }
}
