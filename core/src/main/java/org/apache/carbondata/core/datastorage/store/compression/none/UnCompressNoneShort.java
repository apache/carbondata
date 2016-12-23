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

package org.apache.carbondata.core.datastorage.store.compression.none;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.carbon.datastore.chunk.store.impl.ShortMeasureChunkStore;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressNoneShort implements ValueCompressonHolder.UnCompressValue<short[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNoneShort.class.getName());

  /**
   * shortCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance().getCompressor();

  /**
   * value.
   */
  private short[] shortValue;

  private MeasureDataChunkStore<short[]> measureChunkStore;

  private DataType actualDataType;

  public UnCompressNoneShort(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public void setValue(short[] shortValue) {
    this.shortValue = shortValue;
  }

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException cns1) {
      LOGGER.error(cns1, cns1.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressNoneByte byte1 = new UnCompressNoneByte(actualDataType);
    byte1.setValue(compressor.compressShort(shortValue));
    return byte1;
  }

  @Override
  public UnCompressValue uncompress(DataType dataType, byte[] data, int offset, int length,
      int decimalPlaces, Object maxValueObject) {
    return null;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(shortValue);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    shortValue = ValueCompressionUtil.convertToShortArray(buffer, value.length);
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressNoneByte(this.actualDataType);
  }

  @Override public long getLongValue(int index) {
    return measureChunkStore.getShort(index);
  }

  @Override public double getDoubleValue(int index) {
    return measureChunkStore.getShort(index);
  }

  @Override public BigDecimal getBigDecimalValue(int index) {
    throw new UnsupportedOperationException("Get big decimal is not supported");
  }

  @Override public void setUncomressValue(short[] data, int decimalPlaces, Object maxValueObject) {
    this.measureChunkStore = new ShortMeasureChunkStore(data.length);
    this.measureChunkStore.putData(data);

  }

  @Override public void freeMemory() {
    this.measureChunkStore.freeMemory();
  }
}
