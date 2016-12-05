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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompressor;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressMaxMinShort implements ValueCompressonHolder.UnCompressValue<short[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressMaxMinShort.class.getName());
  /**
   * shortCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance();
  /**
   * value.
   */
  private short[] value;

  private DataType actualDataType;

  public UnCompressMaxMinShort(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public void setValue(short[] value) {
    this.value = value;

  }

  @Override public ValueCompressonHolder.UnCompressValue uncompress(DataType dataType) {
    return null;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException ex3) {
      LOGGER.error(ex3, ex3.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressMaxMinByte byte1 = new UnCompressMaxMinByte(actualDataType);
    byte1.setValue(compressor.compressShort(value));
    return byte1;
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToShortArray(buffer, value.length);
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
        return unCompressLong(decimal, maxValueObject);
      default:
        return unCompressDouble(decimal, maxValueObject);
    }

  }

  private CarbonReadDataHolder unCompressDouble(int decimal, Object maxValueObject) {
    double maxValue = (double) maxValueObject;
    double[] vals = new double[value.length];
    CarbonReadDataHolder carbonDataHolderObj = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      if (value[i] == 0) {
        vals[i] = maxValue;
      } else {
        vals[i] = maxValue - value[i];
      }

    }
    carbonDataHolderObj.setReadableDoubleValues(vals);
    return carbonDataHolderObj;
  }

  private CarbonReadDataHolder unCompressLong(int decimal, Object maxValueObject) {
    long maxValue = (long) maxValueObject;
    long[] vals = new long[value.length];
    CarbonReadDataHolder carbonDataHolderObj = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      if (value[i] == 0) {
        vals[i] = maxValue;
      } else {
        vals[i] = maxValue - value[i];
      }

    }
    carbonDataHolderObj.setReadableLongValues(vals);
    return carbonDataHolderObj;
  }

}
