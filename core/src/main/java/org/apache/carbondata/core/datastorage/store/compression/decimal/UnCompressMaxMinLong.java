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

package org.apache.carbondata.core.datastorage.store.compression.decimal;

import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressMaxMinLong implements ValueCompressonHolder.UnCompressValue<long[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressMaxMinLong.class.getName());
  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance();
  /**
   * value.
   */
  protected long[] value;

  protected DataType actualDataType;

  public UnCompressMaxMinLong(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressMaxMinByte unCompressByte = new UnCompressMaxMinByte(actualDataType);
    unCompressByte.setValue(compressor.compressLong(value));
    return unCompressByte;
  }

  @Override public void setValue(long[] value) {
    this.value = value;

  }

  @Override
  public ValueCompressonHolder.UnCompressValue uncompress(ValueCompressionUtil.DataType dataType) {
    return null;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToLongArray(buffer, value.length);
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressMaxMinByte(actualDataType);
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    switch (actualDataType) {
      case DATA_BIGINT:
        return unCompressLong(maxValueObject);
      default:
        return unCompressDouble(maxValueObject);
    }
  }

  private CarbonReadDataHolder unCompressDouble(Object maxValueObject) {
    double maxValue = (double) maxValueObject;
    double[] vals = new double[value.length];
    CarbonReadDataHolder data = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      vals[i] = maxValue - value[i];
    }
    data.setReadableDoubleValues(vals);
    return data;
  }

  private CarbonReadDataHolder unCompressLong(Object maxValueObject) {
    long maxValue = (long) maxValueObject;
    long[] vals = new long[value.length];
    CarbonReadDataHolder data = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      vals[i] = maxValue - value[i];
    }
    data.setReadableLongValues(vals);
    return data;
  }

}
