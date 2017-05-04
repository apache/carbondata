/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.compression;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;

// Used in read path for decompression preparation
public class ReaderCompressModel {
  private ValueEncoderMeta valueEncoderMeta;

  private DataType convertedDataType;

  private ValueCompressionHolder valueHolder;

  public void setValueEncoderMeta(ValueEncoderMeta valueEncoderMeta) {
    this.valueEncoderMeta = valueEncoderMeta;
  }

  public DataType getConvertedDataType() {
    return convertedDataType;
  }

  public void setConvertedDataType(DataType convertedDataType) {
    this.convertedDataType = convertedDataType;
  }

  public Object getMaxValue() {
    return valueEncoderMeta.getMaxValue();
  }

  public int getMantissa() {
    return valueEncoderMeta.getDecimal();
  }

  public ValueCompressionHolder getValueCompressionHolder() {
    return valueHolder;
  }

  public void setValueCompressionHolder(ValueCompressionHolder valueHolder) {
    this.valueHolder = valueHolder;
  }

}
