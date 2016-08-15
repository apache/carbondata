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
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class UnCompressNonDecimalLong implements UnCompressValue<long[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNonDecimalLong.class.getName());

  /**
   * longCompressor.
   */
  private static Compressor<long[]> longCompressor =
      SnappyCompression.SnappyLongCompression.INSTANCE;

  /**
   * value.
   */
  private long[] value;

  @Override public void setValue(long[] value) {
    this.value = value;
  }

  @Override public UnCompressValue compress() {
    UnCompressNonDecimalByte byte1 = new UnCompressNonDecimalByte();
    byte1.setValue(longCompressor.compress(value));
    return byte1;
  }

  @Override public UnCompressValue getNew() {
    try {
      return (UnCompressValue) clone();
    } catch (CloneNotSupportedException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public UnCompressValue uncompress(DataType dataType) {
    return null;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public void setValueInBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    this.value = ValueCompressionUtil.convertToLongArray(buffer, bytes.length);
  }

  /**
   * @see ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public UnCompressValue getCompressorObject() {
    return new UnCompressNonDecimalByte();
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    double[] vals = new double[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i] / Math.pow(10, decimal);
    }
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    dataHolder.setReadableDoubleValues(vals);
    return dataHolder;
  }

}
