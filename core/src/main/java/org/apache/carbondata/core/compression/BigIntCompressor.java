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

/**
 * It compresses big int data
 */
public class BigIntCompressor extends ValueCompressor {

  @Override
  protected Object compressNonDecimalMaxMin(DataType convertedDataType,
      ColumnPage columnPage, int decimal, Object max) {
    // in case if bigint, decimal will be 0
    return compressMaxMin(convertedDataType, columnPage, max);
  }

  @Override
  protected Object compressNonDecimal(DataType convertedDataType, ColumnPage columnPage,
      int decimal) {
    // in case if bigint, decimal will be 0
    return compressAdaptive(convertedDataType, columnPage);
  }

  /**
   * 1. It gets delta value i.e difference of maximum value and actual value
   * 2. Convert the delta value computed above to convertedDatatype if it can
   *    be stored with less byte
   */
  @Override
  protected Object compressMaxMin(DataType convertedDataType, ColumnPage columnPage,
      Object max) {
    long maxValue = (long) max;
    long[] value = columnPage.getLongPage();
    return compressValue(convertedDataType, value, maxValue, true);
  }

  /**
   * It converts actual value to converted data type if it can be stored with less bytes.
   */
  @Override
  protected Object compressAdaptive(DataType convertedDataType, ColumnPage columnPage) {
    long[] value = columnPage.getLongPage();
    return compressValue(convertedDataType, value, 0, false);
  }

  /**
   * It convert the value or delta value (max - actual) to converted data type.
   * Converted data type is computed based list of values it has.
   * for instance if value is 2,10,12,45
   * these value can be easily fitted in byte value,
   * hence it will be converted into byte to store.
   * @param convertedDataType
   * @param value
   * @param maxValue
   * @param isMinMax
   * @return
   */
  private Object compressValue(DataType convertedDataType, long[] value, long maxValue,
      boolean isMinMax) {
    switch (convertedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];
        if (isMinMax) {
          for (int j = 0; j < value.length; j++) {
            result[j] = (byte) (maxValue - value[j]);
          }
        } else {
          for (int j = 0; j < value.length; j++) {
            result[j] = (byte) value[j];
          }
        }
        return result;
      case SHORT:
        short[] shortResult = new short[value.length];
        if (isMinMax) {
          for (int j = 0; j < value.length; j++) {
            shortResult[j] = (short) (maxValue - value[j]);
          }
        } else {
          for (int j = 0; j < value.length; j++) {
            shortResult[j] = (short) value[j];
          }
        }
        return shortResult;
      case INT:
        int[] intResult = new int[value.length];
        if (isMinMax) {
          for (int j = 0; j < value.length; j++) {
            intResult[j] = (int) (maxValue - value[j]);
          }
        } else {
          for (int j = 0; j < value.length; j++) {
            intResult[j] = (int) value[j];
          }
        }
        return intResult;
      default:
        if (isMinMax) {
          for (int j = 0; j < value.length; j++) {
            value[j] = (maxValue - value[j]);
          }
        }
        return value;
    }
  }
}
