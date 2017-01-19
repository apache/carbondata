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

package org.apache.carbondata.core.datastore.compression.decimal;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressByteArray extends ValueCompressionHolder<byte[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressByteArray.class.getName());
  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private MeasureDataChunkStore<byte[]> measureChunkStore;

  private ByteArrayType arrayType;
  
  private double maxValue;
  /**
   * value.
   */
  private byte[] value;

  public CompressByteArray(ByteArrayType type) {
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

  @Override public void compress() {
    compressedValue = compressor.compressByte(value);
  }

  @Override
  public void uncompress(DataType dataType, byte[] compressedData, int offset,
      int length, int decimal, Object maxValueObject) {
    super.unCompress(compressor, dataType, compressedData, offset, length);
    setUncompressedValues(value, maxValueObject);
  }

  @Override public byte[] getValue() {
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
   /* byte[][] value = new byte[valsList.size()][];
    valsList.toArray(value);
    if (arrayType == ByteArrayType.BIG_DECIMAL) {
      BigDecimal[] bigDecimalValues = new BigDecimal[value.length];
      for (int i = 0; i < value.length; i++) {
        bigDecimalValues[i] = DataTypeUtil.byteToBigDecimal(value[i]);
      }
    }*/
    return valsList.get(0);
  }

  private void setUncompressedValues(byte[] data, Object maxValueObject) {
    this.measureChunkStore =
        MeasureChunkStoreFactory.INSTANCE.getMeasureDataChunkStore(DataType.DATA_BYTE, data.length);
    this.measureChunkStore.putData(data);
    if (maxValueObject instanceof Long) {
      this.maxValue = (long)maxValueObject;
    } else {
      this.maxValue = (double) maxValueObject;
    }
  }
  public static enum ByteArrayType {
    BYTE_ARRAY,
    BIG_DECIMAL
  }
  @Override public long getLongValue(int index) {
    byte byteValue = measureChunkStore.getByte(index);
    return (long) (maxValue - byteValue);
  }

  @Override public double getDoubleValue(int index) {
    byte byteValue = measureChunkStore.getByte(index);
    return (maxValue - byteValue);
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException(
      "Big decimal value is not defined for CompressionMaxMinByte");
  }

  @Override
  public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
