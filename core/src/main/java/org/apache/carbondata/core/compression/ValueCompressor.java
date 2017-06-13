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

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.COMPRESSION_TYPE;
/**
 * Measure compressor
 */
public abstract class ValueCompressor {

  public Object getCompressedValues(CompressionFinder compressionFinder,
      ColumnPage columnPage, Object maxValue, int decimal) {
    COMPRESSION_TYPE compType = compressionFinder.getCompType();
    DataType convertedDataType = compressionFinder.getConvertedDataType();
    switch (compType) {
      case ADAPTIVE:
        return compressAdaptive(convertedDataType, columnPage);
      case DELTA_DOUBLE:
        return compressMaxMin(convertedDataType, columnPage, maxValue);
      case BIGINT:
        return compressNonDecimal(convertedDataType, columnPage, decimal);
      default:
        return compressNonDecimalMaxMin(convertedDataType, columnPage, decimal, maxValue);
    }
  }

  abstract Object compressNonDecimalMaxMin(DataType convertedDataType,
      ColumnPage columnPage, int decimal, Object maxValue);

  abstract Object compressNonDecimal(DataType convertedDataType,
      ColumnPage columnPage, int decimal);

  abstract Object compressMaxMin(DataType convertedDataType,
      ColumnPage columnPage, Object maxValue);

  abstract Object compressAdaptive(DataType convertedDataType,
      ColumnPage columnPage);
}
