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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.CompressorFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionNonDecimalMaxMinShort extends ValueCompressionHolder<short[]> {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionNonDecimalMaxMinShort.class.getName());
  /**
   * compressor.
   */
  private static Compressor compressor = CompressorFactory.getInstance();
  /**
   * value.
   */
  private short[] value;

  @Override public void setValue(short[] value) { this.value = value; }

  @Override public short[] getValue() { return this.value; }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_SHORT, value);
  }

  @Override public void uncompress(DataType dataTypeVal, byte[] data) {
    super.unCompress(compressor, dataTypeVal,data);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToShortArray(buffer, value.length);
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    double maxValue = (double) maxValueObject;
    double[] vals = new double[value.length];
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i] / Math.pow(10, decimal);

      if (value[i] == 0) {
        vals[i] = maxValue;
      } else {
        BigDecimal diff = BigDecimal.valueOf(value[i] / Math.pow(10, decimal));
        BigDecimal max = BigDecimal.valueOf(maxValue);
        vals[i] = max.subtract(diff).doubleValue();
      }

    }
    dataHolder.setReadableDoubleValues(vals);
    return dataHolder;
  }

}
