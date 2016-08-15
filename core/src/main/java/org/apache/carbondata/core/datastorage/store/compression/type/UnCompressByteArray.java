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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil;

public class UnCompressByteArray implements ValueCompressonHolder.UnCompressValue<byte[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressMaxMinByte.class.getName());
  /**
   * byteCompressor.
   */
  private static Compressor<byte[]> byteCompressor =
      SnappyCompression.SnappyByteCompression.INSTANCE;
  private ByteArrayType arrayType;
  /**
   * value.
   */
  private byte[] value;

  public UnCompressByteArray(ByteArrayType type) {
    if (type == ByteArrayType.BYTE_ARRAY) {
      arrayType = ByteArrayType.BYTE_ARRAY;
    } else {
      arrayType = ByteArrayType.BIG_DECIMAL;
    }

  }

  @Override public void setValue(byte[] value) {
    this.value = value;

  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;

  }

  @Override public ValueCompressonHolder.UnCompressValue<byte[]> getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressByteArray byte1 = new UnCompressByteArray(arrayType);
    byte1.setValue(byteCompressor.compress(value));
    return byte1;
  }

  @Override
  public ValueCompressonHolder.UnCompressValue uncompress(ValueCompressionUtil.DataType dataType) {
    ValueCompressonHolder.UnCompressValue byte1 = new UnCompressByteArray(arrayType);
    byte1.setValue(byteCompressor.unCompress(value));
    return byte1;
  }

  @Override public byte[] getBackArrayData() {
    return this.value;
  }

  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressByteArray(arrayType);
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    List<byte[]> valsList = new ArrayList<byte[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    ByteBuffer buffer = ByteBuffer.wrap(value);
    buffer.rewind();
    int length = 0;
    byte[] actualValue = null;
    //CHECKSTYLE:OFF    Approval No:Approval-367
    while (buffer.hasRemaining()) {//CHECKSTYLE:ON
      length = buffer.getInt();
      actualValue = new byte[length];
      buffer.get(actualValue);
      valsList.add(actualValue);

    }
    CarbonReadDataHolder holder = new CarbonReadDataHolder();
    byte[][] value = new byte[valsList.size()][];
    valsList.toArray(value);
    if (arrayType == ByteArrayType.BIG_DECIMAL) {
      BigDecimal[] bigDecimalValues = new BigDecimal[value.length];
      for (int i = 0; i < value.length; i++) {
        bigDecimalValues[i] = DataTypeUtil.byteToBigDecimal(value[i]);
      }
      holder.setReadableBigDecimalValues(bigDecimalValues);
      return holder;
    }
    holder.setReadableByteValues(value);
    return holder;
  }

  public static enum ByteArrayType {
    BYTE_ARRAY,
    BIG_DECIMAL
  }

}
