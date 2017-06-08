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

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Double compressor
 */
public class DoubleCompressor extends ValueCompressor {

  @Override
  protected Object compressNonDecimalMaxMin(DataType convertedDataType,
      ColumnPage columnPage, int decimal, Object maxValue) {
    int i = 0;
    BigDecimal max = BigDecimal.valueOf((double)maxValue);
    double[] value = columnPage.getDoublePage();
    switch (convertedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];
        for (int j = 0; j < value.length; j++) {
          BigDecimal val = BigDecimal.valueOf(value[j]);
          double diff = max.subtract(val).doubleValue();
          result[i] = (byte) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return result;
      case SHORT:
        short[] shortResult = new short[value.length];
        for (int j = 0; j < value.length; j++) {
          BigDecimal val = BigDecimal.valueOf(value[j]);
          double diff = max.subtract(val).doubleValue();
          shortResult[i] = (short) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return shortResult;
      case INT:
        int[] intResult = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          BigDecimal val = BigDecimal.valueOf(value[j]);
          double diff = max.subtract(val).doubleValue();
          intResult[i] = (int) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return intResult;
      case LONG:
        long[] longResult = new long[value.length];
        for (int j = 0; j < value.length; j++) {
          BigDecimal val = BigDecimal.valueOf(value[j]);
          double diff = max.subtract(val).doubleValue();
          longResult[i] = Math.round(diff * Math.pow(10, decimal));
          i++;
        }
        return longResult;
      case FLOAT:
        float[] floatResult = new float[value.length];
        for (int j = 0; j < value.length; j++) {
          BigDecimal val = BigDecimal.valueOf(value[j]);
          double diff = max.subtract(val).doubleValue();
          floatResult[i] = (float) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return floatResult;
      default:
        double[] defaultResult = new double[value.length];
        for (int j = 0; j < value.length; j++) {
          BigDecimal val = BigDecimal.valueOf(value[j]);
          double diff = max.subtract(val).doubleValue();
          defaultResult[i] =  (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return defaultResult;
    }
  }

  @Override
  protected Object compressNonDecimal(DataType convertedDataType, ColumnPage columnPage,
      int decimal) {
    int i = 0;
    double[] value = columnPage.getDoublePage();
    switch (convertedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];
        for (int j = 0; j < value.length; j++) {
          result[i] = (byte) (Math.round(Math.pow(10, decimal) * value[j]));
          i++;
        }
        return result;
      case SHORT:
        short[] shortResult = new short[value.length];
        for (int j = 0; j < value.length; j++) {
          shortResult[i] = (short) (Math.round(Math.pow(10, decimal) * value[j]));
          i++;
        }
        return shortResult;
      case INT:
        int[] intResult = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          intResult[i] = (int) (Math.round(Math.pow(10, decimal) * value[j]));
          i++;
        }
        return intResult;
      case LONG:
        long[] longResult = new long[value.length];
        for (int j = 0; j < value.length; j++) {
          longResult[i] = Math.round(Math.pow(10, decimal) * value[j]);
          i++;
        }
        return longResult;
      case FLOAT:
        float[] floatResult = new float[value.length];
        for (int j = 0; j < value.length; j++) {
          floatResult[i] = (float) (Math.round(Math.pow(10, decimal) * value[j]));
          i++;
        }
        return floatResult;
      default:
        double[] defaultResult = new double[value.length];
        for (int j = 0; j < value.length; j++) {
          defaultResult[i] = (double) (Math.round(Math.pow(10, decimal) * value[j]));
          i++;
        }
        return defaultResult;
    }
  }

  @Override
  protected Object compressMaxMin(DataType convertedDataType, ColumnPage columnPage,
      Object max) {
    double maxValue = (double) max;
    double[] value = columnPage.getDoublePage();
    int i = 0;
    switch (convertedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];
        for (int j = 0; j < value.length; j++) {
          result[i] = (byte) (maxValue - value[j]);
          i++;
        }
        return result;
      case SHORT:
        short[] shortResult = new short[value.length];
        for (int j = 0; j < value.length; j++) {
          shortResult[i] = (short) (maxValue - value[j]);
          i++;
        }
        return shortResult;
      case INT:
        int[] intResult = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          intResult[i] = (int) (maxValue - value[j]);
          i++;
        }
        return intResult;
      case LONG:
        long[] longResult = new long[value.length];
        for (int j = 0; j < value.length; j++) {
          longResult[i] = (long) (maxValue - value[j]);
          i++;
        }
        return longResult;
      case FLOAT:
        float[] floatResult = new float[value.length];
        for (int j = 0; j < value.length; j++) {
          floatResult[i] = (float) (maxValue - value[j]);
          i++;
        }
        return floatResult;
      default:
        double[] defaultResult = new double[value.length];
        for (int j = 0; j < value.length; j++) {
          defaultResult[i] = maxValue - value[j];
          i++;
        }
        return defaultResult;
    }
  }

  @Override
  protected Object compressAdaptive(DataType changedDataType, ColumnPage columnPage) {
    double[] value = columnPage.getDoublePage();
    int i = 0;
    switch (changedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];
        for (int j = 0; j < value.length; j++) {
          result[i] = (byte) value[j];
          i++;
        }
        return result;
      case SHORT:
        short[] shortResult = new short[value.length];
        for (int j = 0; j < value.length; j++) {
          shortResult[i] = (short) value[j];
          i++;
        }
        return shortResult;
      case INT:
        int[] intResult = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          intResult[i] = (int) value[j];
          i++;
        }
        return intResult;
      case LONG:
        long[] longResult = new long[value.length];
        for (int j = 0; j < value.length; j++) {
          longResult[i] = (long) value[j];
          i++;
        }
        return longResult;
      case FLOAT:
        float[] floatResult = new float[value.length];
        for (int j = 0; j < value.length; j++) {
          floatResult[i] = (float) value[j];
          i++;
        }
        return floatResult;
      default:
        return value;
    }
  }
}
