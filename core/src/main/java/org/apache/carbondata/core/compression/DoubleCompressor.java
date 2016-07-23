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

import java.math.BigDecimal;

import org.apache.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Double compressor
 */
public class DoubleCompressor extends ValueCompressor {

  @Override protected Object compressNonDecimalMaxMin(DataType changedDataType,
      CarbonWriteDataHolder dataHolder, int decimal, Object maxValue) {
    int i = 0;
    BigDecimal max = BigDecimal.valueOf((double)maxValue);
    double[] value = dataHolder.getWritableDoubleValues();
    switch (changedDataType) {
      case DATA_BYTE:
        byte[] result = new byte[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          result[i] = (byte) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          shortResult[i] = (short) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          intResult[i] = (int) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          longResult[i] = (long) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          floatResult[i] = (float) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return floatResult;

      default:

        double[] defaultResult = new double[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          defaultResult[i] =  (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return defaultResult;

    }
  }

  @Override
  protected Object compressNonDecimal(DataType changedDataType, CarbonWriteDataHolder dataHolder,
      int decimal) {
    int i = 0;
    double[] value = dataHolder.getWritableDoubleValues();
    switch (changedDataType) {
      case DATA_BYTE:
        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return result;
      case DATA_SHORT:
        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return shortResult;
      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return floatResult;

      default:
        double[] defaultResult = new double[value.length];

        for (double a : value) {
          defaultResult[i] = (double) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return defaultResult;
    }
  }

  @Override
  protected Object compressMaxMin(DataType changedDataType, CarbonWriteDataHolder dataHolder,
      Object max) {
    double maxValue = (double) max;
    double[] value = dataHolder.getWritableDoubleValues();
    int i = 0;
    switch (changedDataType) {
      case DATA_BYTE:

        byte[] result = new byte[value.length];
        for (double a : value) {
          result[i] = (byte) (maxValue - a);
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (maxValue - a);
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (maxValue - a);
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) (maxValue - a);
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) (maxValue - a);
          i++;
        }
        return floatResult;

      default:

        double[] defaultResult = new double[value.length];

        for (double a : value) {
          defaultResult[i] = (double) (maxValue - a);
          i++;
        }
        return defaultResult;

    }
  }

  @Override
  protected Object compressNone(DataType changedDataType, CarbonWriteDataHolder dataHolder) {
    double[] value = dataHolder.getWritableDoubleValues();
    int i = 0;
    switch (changedDataType) {

      case DATA_BYTE:

        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) a;
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) a;
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) a;
          i++;
        }
        return intResult;

      case DATA_LONG:
      case DATA_BIGINT:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) a;
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) a;
          i++;
        }
        return floatResult;

      default:

        return value;

    }
  }

}
