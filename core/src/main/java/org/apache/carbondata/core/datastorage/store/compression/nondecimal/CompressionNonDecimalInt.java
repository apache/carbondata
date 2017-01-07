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

package org.apache.carbondata.core.datastorage.store.compression.nondecimal;

import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionNonDecimalInt extends ValueCompressionHolder<int[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionNonDecimalInt.class.getName());
  /**
   * intCompressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance();
  /**
   * value.
   */
  private int[] value;

  @Override public void setValue(int[] value) {
    this.value = value;
  }

  @Override public int[] getValue() { return this.value; }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_INT, value);
  }

  @Override public void uncompress(DataType dataType, byte[] data) {
    super.unCompress(compressor, dataType,data);
  }

  @Override public void setValueInBytes(byte[] bytesArr) {
    ByteBuffer buffer = ByteBuffer.wrap(bytesArr);
    this.value = ValueCompressionUtil.convertToIntArray(buffer, bytesArr.length);
  }

  @Override public CarbonReadDataHolder getValues(int mantissa, Object maxValueObject) {
    double[] vals = new double[value.length];
    for (int k = 0; k < vals.length; k++) {
      vals[k] = value[k] / Math.pow(10, mantissa);
    }
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    dataHolder.setReadableDoubleValues(vals);
    return dataHolder;
  }
}
