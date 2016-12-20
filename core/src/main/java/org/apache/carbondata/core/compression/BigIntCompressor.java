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
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * It compresses big int data
 */
public class BigIntCompressor extends ValueCompressor {

  @Override protected Object compressNonDecimalMaxMin(DataType changedDataType,
      CarbonWriteDataHolder dataHolder, int decimal, Object max) {
    // in case if bigint, decimal will be 0
    return compressMaxMin(changedDataType, dataHolder, max);
  }

  @Override
  protected Object compressNonDecimal(DataType changedDataType, CarbonWriteDataHolder dataHolder,
      int decimal) {
    // in case if bigint, decimal will be 0
    return compressNone(changedDataType, dataHolder);
  }

  @Override
  protected Object compressMaxMin(DataType changedDataType, CarbonWriteDataHolder dataHolder,
      Object max) {
    long maxValue = (long) max;
    long[] value = dataHolder.getWritableLongValues();
    return compressMaxMin(changedDataType, maxValue, value);
  }

  /**
   * 1. It gets delta value i.e difference of maximum value and actual value
   * 2. Convert the delta value computed above to changedDatatype
   * @param changedDataType
   * @param maxValue
   * @param value
   * @return
   */
  protected Object compressMaxMin(DataType changedDataType, long maxValue,
      long[] value) {
    int i = 0;
    switch (changedDataType) {
      case DATA_BYTE:
        byte[] result = new byte[value.length];
        for (int j = 0; j < value.length; j++) {
          result[i] = (byte) (maxValue - value[j]);
          i++;
        }
        return result;
      case DATA_SHORT:
        short[] shortResult = new short[value.length];
        for (int j = 0; j < value.length; j++) {
          shortResult[i] = (short) (maxValue - value[j]);
          i++;
        }
        return shortResult;
      case DATA_INT:
        int[] intResult = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          intResult[i] = (int) (maxValue - value[j]);
          i++;
        }
        return intResult;
      default:
        long[] defaultResult = new long[value.length];
        for (int j = 0; j < value.length; j++) {
          defaultResult[i] = (long) (maxValue - value[j]);
          i++;
        }
        return defaultResult;
    }
  }

  @Override
  protected Object compressNone(DataType changedDataType, CarbonWriteDataHolder dataHolder) {
    long[] value = dataHolder.getWritableLongValues();
    return compressNone(changedDataType, value);
  }

  /**
   * It convert the value to changed datatype.
   * Changed datatype is computed based list of values it has.
   * for instance if value is 2,10,12,45
   * these value can be easily fit in byte and hence below method convert to byte and store it.
   * @param changedDataType
   * @param value
   * @return
   */
  protected Object compressNone(DataType changedDataType, long[] value) {
    int i = 0;
    switch (changedDataType) {
      case DATA_BYTE:
        byte[] result = new byte[value.length];
        for (int j = 0; j < value.length; j++)  {
          result[i] = (byte) value[j];
          i++;
        }
        return result;
      case DATA_SHORT:
        short[] shortResult = new short[value.length];
        for (int j = 0; j < value.length; j++) {
          shortResult[i] = (short) value[j];
          i++;
        }
        return shortResult;
      case DATA_INT:
        int[] intResult = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          intResult[i] = (int) value[j];
          i++;
        }
        return intResult;
      default:
        return value;
    }
  }
}
