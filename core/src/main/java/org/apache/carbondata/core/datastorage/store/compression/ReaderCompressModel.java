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

package org.apache.carbondata.core.datastorage.store.compression;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.ValueCompressionUtil;

// Used in read path for decompression preparation
public class ReaderCompressModel {
  private ValueEncoderMeta valueEncoderMeta;

  private ValueCompressionUtil.DataType changedDataType;

  private ValueCompressonHolder.UnCompressValue unCompressValues;

  private ValueCompressionUtil.COMPRESSION_TYPE compType;

  public void setValueEncoderMeta(ValueEncoderMeta valueEncoderMeta) {
    this.valueEncoderMeta = valueEncoderMeta;
  }

  public void setCompType(ValueCompressionUtil.COMPRESSION_TYPE compType) {
    this.compType = compType;
  }

  public ValueCompressionUtil.DataType getChangedDataType() {
    return changedDataType;
  }

  public void setChangedDataType(ValueCompressionUtil.DataType changedDataType) {
    this.changedDataType = changedDataType;
  }

  public Object getMaxValue() {
    return valueEncoderMeta.getMaxValue();
  }

  public int getDecimal() {
    return valueEncoderMeta.getDecimal();
  }

  public ValueCompressonHolder.UnCompressValue getUnCompressValues() {
    return unCompressValues;
  }

  public void setUnCompressValues(ValueCompressonHolder.UnCompressValue unCompressValues) {
    this.unCompressValues = unCompressValues;
  }

}
