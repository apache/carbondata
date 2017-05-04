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


package org.apache.carbondata.core.datastore.compression.decimal;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.MeasureChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class CompressByteArray extends ValueCompressionHolder<byte[]> {

  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private MeasureDataChunkStore<byte[]> measureChunkStore;

  /**
   * value.
   */
  private byte[] value;

  @Override public void setValue(byte[] value) {
    this.value = value;

  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;

  }

  @Override public void compress() {
    compressedValue = compressor.compressByte(value);
  }

  @Override public void uncompress(DataType dataType, byte[] compressedData, int offset, int length,
      int decimal, Object maxValueObject, int numberOfRows) {
    super.unCompress(compressor, dataType, compressedData, offset, length, numberOfRows,
        maxValueObject, decimal);
  }

  @Override public byte[] getValue() {
    List<byte[]> valsList = new ArrayList<byte[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    ByteBuffer buffer = ByteBuffer.wrap(value);
    buffer.rewind();
    int length = 0;
    byte[] actualValue = null;
    //CHECKSTYLE:OFF    Approval No:Approval-367
    while (buffer.hasRemaining()) { //CHECKSTYLE:ON
      length = buffer.getInt();
      actualValue = new byte[length];
      buffer.get(actualValue);
      valsList.add(actualValue);
    }
    return valsList.get(0);
  }

  @Override public long getLongValue(int index) {
    throw new UnsupportedOperationException("Get long value is not defined for CompressByteArray");
  }

  @Override public double getDoubleValue(int index) {
    throw new UnsupportedOperationException(
        "Get double value is not defined for CompressByteArray");
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    return this.measureChunkStore.getBigDecimal(index);
  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }

  @Override
  public void setValue(byte[] data, int numberOfRows, Object maxValueObject, int decimalPlaces) {
    this.measureChunkStore = MeasureChunkStoreFactory.INSTANCE
        .getMeasureDataChunkStore(DataType.DECIMAL, numberOfRows);
    this.measureChunkStore.putData(data);
  }
}
