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
import org.apache.carbondata.core.util.BigDecimalCompressionFinder;
import org.apache.carbondata.core.util.CompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Big decimal data type compressor
 *
 */
public class BigDecimalCompressor extends BigIntCompressor {

  private boolean readLeft = true;

  @Override
  public Object getCompressedValues(CompressionFinder compressionFinder,
      CarbonWriteDataHolder dataHolder, Object maxValue, int decimal) {
    BigDecimalCompressionFinder bigdCompressionFinder =
        (BigDecimalCompressionFinder) compressionFinder;
    Long[] maxValues = (Long[]) maxValue;
    Object leftCompressedValue = getCompressedValues(
        bigdCompressionFinder.getLeftCompType(), dataHolder,
        bigdCompressionFinder.getLeftConvertedDataType(), maxValues[0], 0);
    readLeft = false;
    Object rightCompressedValue = getCompressedValues(
        bigdCompressionFinder.getRightCompType(), dataHolder,
        bigdCompressionFinder.getRightConvertedDataType(), maxValues[1], 0);
    return new Object[] { leftCompressedValue, rightCompressedValue };
  }

  @Override
  protected Object compressMaxMin(DataType convertedDataType,
      CarbonWriteDataHolder dataHolder, Object max) {
    long maxValue = (long) max;
    long[][] writableBigDValues = dataHolder.getWritableBigDecimalValues();
    long[] value = null;
    if (readLeft) {
      value = writableBigDValues[0];
    } else {
      value = writableBigDValues[1];
    }
    return compressValue(convertedDataType, value, maxValue, true);
  }

  @Override
  protected Object compressAdaptive(DataType convertedDataType,
      CarbonWriteDataHolder dataHolder) {
    long[][] writableBigDValues = dataHolder.getWritableBigDecimalValues();
    long[] value = null;
    if (readLeft) {
      value = writableBigDValues[0];
    } else {
      value = writableBigDValues[1];
    }
    return compressValue(convertedDataType, value, 0, false);
  }
}
