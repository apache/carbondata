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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressNonDecimalByte implements ValueCompressonHolder.UnCompressValue<byte[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNonDecimalByte.class.getName());
  /**
   * byteCompressor.
   */
  private static Compressor<byte[]> byteCompressor =
      SnappyCompression.SnappyByteCompression.INSTANCE;
  /**
   * value.
   */
  private byte[] value;

  @Override public void setValue(byte[] value) {
    this.value = value;
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
    UnCompressNonDecimalByte byte1 = new UnCompressNonDecimalByte();
    byte1.setValue(byteCompressor.compress(value));
    return byte1;
  }

  @Override public ValueCompressonHolder.UnCompressValue uncompress(DataType dataType) {
    ValueCompressonHolder.UnCompressValue byte1 =
        ValueCompressionUtil.unCompressNonDecimal(dataType, dataType);
    ValueCompressonHolder.unCompress(dataType, byte1, value);
    return byte1;
  }

  @Override public void setValueInBytes(byte[] value) {
    this.value = value;
  }

  @Override public byte[] getBackArrayData() {
    return value;
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressNonDecimalByte();
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    double[] vals = new double[value.length];
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i] / Math.pow(10, decimal);
    }
    dataHolder.setReadableDoubleValues(vals);
    return dataHolder;
  }
}
