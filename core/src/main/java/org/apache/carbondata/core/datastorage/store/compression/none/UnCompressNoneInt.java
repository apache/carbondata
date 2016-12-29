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

import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressNoneInt implements ValueCompressonHolder.UnCompressValue<int[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNoneInt.class.getName());
  /**
   * intCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance();
  /**
   * value.
   */
  private int[] value;

  private DataType actualDataType;

  public UnCompressNoneInt(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public void setValue(int[] value) {
    this.value = value;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException exc) {
      LOGGER.error(exc, exc.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressNoneByte byte1 = new UnCompressNoneByte(actualDataType);
    byte1.setValue(compressor.compressInt(value));
    return byte1;
  }

  @Override public ValueCompressonHolder.UnCompressValue uncompress(DataType dataType) {
    return null;
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToIntArray(buffer, value.length);
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressNoneByte(this.actualDataType);
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    switch (actualDataType) {
      case DATA_SHORT:
        return unCompressShort();
      case DATA_INT:
        return unCompressInt();
      case DATA_LONG:
      case DATA_BIGINT:
        return unCompressLong();
      default:
        return unCompressDouble();
    }
  }

  private CarbonReadDataHolder unCompressShort() {
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    short[] vals = new short[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = (short)value[i];
    }
    dataHolder.setReadableShortValues(vals);
    return dataHolder;
  }

  private CarbonReadDataHolder unCompressInt() {
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    dataHolder.setReadableIntValues(value);
    return dataHolder;
  }

  private CarbonReadDataHolder unCompressDouble() {
    CarbonReadDataHolder dataHolderInfoObj = new CarbonReadDataHolder();
    double[] vals = new double[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i];
    }
    dataHolderInfoObj.setReadableDoubleValues(vals);
    return dataHolderInfoObj;
  }

  private CarbonReadDataHolder unCompressLong() {
    CarbonReadDataHolder dataHolderInfoObj = new CarbonReadDataHolder();
    long[] vals = new long[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i];
    }

    dataHolderInfoObj.setReadableLongValues(vals);
    return dataHolderInfoObj;
  }
}
