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
package org.apache.carbondata.core.compression;

import org.apache.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil.COMPRESSION_TYPE;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Measure compressor
 */
public abstract class ValueCompressor {

  /**
   *
   * @param compType
   * @param dataHolder
   * @param changedDataType
   * @param maxValue
   * @param decimal
   * @return compressed data
   */
  public Object getCompressedValues(COMPRESSION_TYPE compType, CarbonWriteDataHolder dataHolder,
      DataType changedDataType, Object maxValue, int decimal) {
    switch (compType) {
      case NONE:
        return compressNone(changedDataType, dataHolder);
      case MAX_MIN_DECIMAL:
        return compressMaxMin(changedDataType, dataHolder, maxValue);
      case NON_DECIMAL_CONVERT:
        return compressNonDecimal(changedDataType, dataHolder, decimal);
      default:
        return compressNonDecimalMaxMin(changedDataType, dataHolder, decimal, maxValue);
    }
  }

  /**
   *
   * @param changedDataType
   * @param dataHolder
   * @param decimal
   * @param maxValue
   * @return compressed data
   */
  protected abstract Object compressNonDecimalMaxMin(DataType changedDataType,
      CarbonWriteDataHolder dataHolder, int decimal, Object maxValue);

  /**
   *
   * @param changedDataType
   * @param dataHolder
   * @param decimal
   * @return compressed data
   */
  protected abstract Object compressNonDecimal(DataType changedDataType,
      CarbonWriteDataHolder dataHolder, int decimal);

  /**
   *
   * @param changedDataType
   * @param dataHolder
   * @param maxValue
   * @return compressed data
   */
  protected abstract Object compressMaxMin(DataType changedDataType,
      CarbonWriteDataHolder dataHolder, Object maxValue);

  /**
   *
   * @param changedDataType
   * @param dataHolder
   * @return compressed data
   */
  protected abstract Object compressNone(DataType changedDataType,
      CarbonWriteDataHolder dataHolder);
}
