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
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class CompressionMaxMinFloat extends ValueCompressionHolder<float[]> {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompressionMaxMinFloat.class.getName());
  /**
   * floatCompressor
   */
  private static Compressor compressor = CompressorFactory.getInstance();
  /**
   * value.
   */
  private float[] value;

  /**
   * actual data type
   */
  private DataType actualDataType;

  public CompressionMaxMinFloat(DataType actualDataType) {
    this.actualDataType = actualDataType;
  }


  @Override public void setValue(float[] value) {
    this.value = value;

  }

  @Override public float[] getValue() {return this.value; }

  @Override public void compress() {
    compressedValue = super.compress(compressor, DataType.DATA_FLOAT, value);
  }

  @Override public void uncompress(DataType dTypeVal, byte[] data) {
    super.unCompress(compressor, dTypeVal, data);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    this.value = ValueCompressionUtil.convertToFloatArray(buffer, value.length);
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    double maxValue = (double) maxValueObject;
    double[] vals = new double[value.length];
    CarbonReadDataHolder dataHolderVal = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      vals[i] = maxValue - value[i];
    }
    dataHolderVal.setReadableDoubleValues(vals);
    return dataHolderVal;
  }
}
