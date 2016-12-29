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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressNoneByte implements UnCompressValue<byte[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNoneByte.class.getName());

  /**
   * byteCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance();

  /**
   * value.
   */
  private byte[] value;

  /**
   * actual data type
   */
  private DataType actualDataType;

  public UnCompressNoneByte(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }

  @Override public UnCompressValue getNew() {
    try {
      return (UnCompressValue) clone();
    } catch (CloneNotSupportedException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public void setValue(byte[] value) {
    this.value = value;
  }

  @Override public UnCompressValue uncompress(DataType dataType) {
    UnCompressValue byte1 = ValueCompressionUtil.getUnCompressNone(dataType, actualDataType);
    ValueCompressonHolder.unCompress(dataType, byte1, value);
    return byte1;
  }

  @Override public UnCompressValue compress() {
    UnCompressNoneByte byte1 = new UnCompressNoneByte(actualDataType);
    byte1.setValue(compressor.compressByte(value));
    return byte1;
  }

  @Override public byte[] getBackArrayData() {
    return value;
  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public UnCompressValue getCompressorObject() {
    return new UnCompressNoneByte(actualDataType);
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    switch (actualDataType) {
      case DATA_BIGINT:
        return unCompressLong();
      case DATA_FLOAT:
        return unCompressFloat();
      default:
        return unCompressDouble();
    }

  }

  private CarbonReadDataHolder unCompressFloat() {
    CarbonReadDataHolder dataHldr = new CarbonReadDataHolder();
    float[] vals = new float[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i];
    }
    dataHldr.setReadableFloatValues(vals);
    return dataHldr;
  }

  private CarbonReadDataHolder unCompressDouble() {
    CarbonReadDataHolder dataHldr = new CarbonReadDataHolder();
    double[] vals = new double[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i];
    }
    dataHldr.setReadableDoubleValues(vals);
    return dataHldr;
  }

  private CarbonReadDataHolder unCompressLong() {
    CarbonReadDataHolder dataHldr = new CarbonReadDataHolder();
    long[] vals = new long[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i];
    }
    dataHldr.setReadableLongValues(vals);
    return dataHldr;
  }

}
