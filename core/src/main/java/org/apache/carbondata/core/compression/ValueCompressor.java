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
package org.apache.carbondata.core.compression;

import org.apache.carbondata.core.datastore.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.COMPRESSION_TYPE;
/**
 * Measure compressor
 */
public abstract class ValueCompressor {

  public Object getCompressedValues(CompressionFinder compressionFinder,
      CarbonWriteDataHolder dataHolder, Object maxValue, int decimal) {
    return getCompressedValues(compressionFinder.getCompType(),
        dataHolder,
        compressionFinder.getConvertedDataType(),
        maxValue, decimal);
  }

  /**
   *
   * @param compType
   * @param dataHolder
   * @param convertedDataType
   * @param maxValue
   * @param decimal
   * @return compressed data
   */
  public Object getCompressedValues(COMPRESSION_TYPE compType, CarbonWriteDataHolder dataHolder,
      DataType convertedDataType, Object maxValue, int decimal) {
    switch (compType) {
      case ADAPTIVE:
        return compressAdaptive(convertedDataType, dataHolder);
      case DELTA_DOUBLE:
        return compressMaxMin(convertedDataType, dataHolder, maxValue);
      case BIGINT:
        return compressNonDecimal(convertedDataType, dataHolder, decimal);
      default:
        return compressNonDecimalMaxMin(convertedDataType, dataHolder, decimal, maxValue);
    }
  }

  /**
   *
   * @param convertedDataType
   * @param dataHolder
   * @param decimal
   * @param maxValue
   * @return compressed data
   */
  protected abstract Object compressNonDecimalMaxMin(DataType convertedDataType,
      CarbonWriteDataHolder dataHolder, int decimal, Object maxValue);

  /**
   *
   * @param convertedDataType
   * @param dataHolder
   * @param decimal
   * @return compressed data
   */
  protected abstract Object compressNonDecimal(DataType convertedDataType,
      CarbonWriteDataHolder dataHolder, int decimal);

  /**
   *
   * @param convertedDataType
   * @param dataHolder
   * @param maxValue
   * @return compressed data
   */
  protected abstract Object compressMaxMin(DataType convertedDataType,
      CarbonWriteDataHolder dataHolder, Object maxValue);

  /**
   *
   * @param convertedDataType
   * @param dataHolder
   * @return compressed data
   */
  protected abstract Object compressAdaptive(DataType convertedDataType,
      CarbonWriteDataHolder dataHolder);
}
