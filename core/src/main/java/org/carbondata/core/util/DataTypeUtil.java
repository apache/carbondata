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

package org.carbondata.core.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;

import org.apache.commons.lang.NumberUtils;
import org.apache.spark.unsafe.types.UTF8String;

public final class DataTypeUtil {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataTypeUtil.class.getName());

  /**
   * This method will convert a given value to its specific type
   *
   * @param msrValue
   * @param dataType
   * @param carbonMeasure
   * @return
   */
  public static Object getMeasureValueBasedOnDataType(String msrValue, DataType dataType,
      CarbonMeasure carbonMeasure) {
    switch (dataType) {
      case DECIMAL:
        BigDecimal bigDecimal =
            new BigDecimal(msrValue).setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
        return normalizeDecimalValue(bigDecimal, carbonMeasure.getPrecision());
      case LONG:
        return Long.valueOf(msrValue);
      default:
        return Double.valueOf(msrValue);
    }
  }

  /**
   * This method will check the digits before dot with the max precision allowed
   *
   * @param bigDecimal
   * @param allowedPrecision precision configured by the user
   * @return
   */
  private static BigDecimal normalizeDecimalValue(BigDecimal bigDecimal, int allowedPrecision) {
    if (bigDecimal.precision() > allowedPrecision) {
      return null;
    }
    return bigDecimal;
  }

  /**
   * This method will return the type of measure based on its data type
   *
   * @param dataType
   * @return
   */
  public static char getAggType(DataType dataType) {
    switch (dataType) {
      case DECIMAL:
        return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
      case LONG:
        return CarbonCommonConstants.BIG_INT_MEASURE;
      default:
        return CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE;
    }
  }

  /**
   * This method will convert a big decimal value to bytes
   *
   * @param num
   * @return
   */
  public static byte[] bigDecimalToByte(BigDecimal num) {
    BigInteger sig = new BigInteger(num.unscaledValue().toString());
    int scale = num.scale();
    byte[] bscale = new byte[] { (byte) (scale) };
    byte[] buff = sig.toByteArray();
    byte[] completeArr = new byte[buff.length + bscale.length];
    System.arraycopy(bscale, 0, completeArr, 0, bscale.length);
    System.arraycopy(buff, 0, completeArr, bscale.length, buff.length);
    return completeArr;
  }

  /**
   * This method will convert a byte value back to big decimal value
   *
   * @param raw
   * @return
   */
  public static BigDecimal byteToBigDecimal(byte[] raw) {
    int scale = (raw[0] & 0xFF);
    byte[] unscale = new byte[raw.length - 1];
    System.arraycopy(raw, 1, unscale, 0, unscale.length);
    BigInteger sig = new BigInteger(unscale);
    return new BigDecimal(sig, scale);
  }

  /**
   * returns the SqlStatement.Type of corresponding string value
   *
   * @param dataTypeStr
   * @return return the SqlStatement.Type
   */
  public static DataType getDataType(String dataTypeStr) {
    DataType dataType = null;
    switch (dataTypeStr) {
      case "TIMESTAMP":
        dataType = DataType.TIMESTAMP;
        break;
      case "STRING":
        dataType = DataType.STRING;
        break;
      case "INT":
        dataType = DataType.INT;
        break;
      case "SHORT":
        dataType = DataType.SHORT;
        break;
      case "LONG":
        dataType = DataType.LONG;
        break;
      case "DOUBLE":
        dataType = DataType.DOUBLE;
        break;
      case "DECIMAL":
        dataType = DataType.DECIMAL;
        break;
      case "ARRAY":
        dataType = DataType.ARRAY;
        break;
      case "STRUCT":
        dataType = DataType.STRUCT;
        break;
      case "MAP":
      default:
        dataType = DataType.STRING;
    }
    return dataType;
  }

  /**
   * Below method will be used to basically to know whether the input data is valid string of
   * giving data type. If there is any non parseable string is present return false.
   */
  public static boolean isValidData(String data, DataType actualDataType) {
    if (null == data) {
      return false;
    }
    switch (actualDataType) {
      case SHORT:
      case INT:
      case LONG:
      case DOUBLE:
      case DECIMAL:
        return NumberUtils.isDigits(data);
      case TIMESTAMP:
        if (data.isEmpty()) {
          return false;
        }
        SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
        try {
          parser.parse(data);
          return true;
        } catch (ParseException e) {
          return false;
        }
      default:
        return false;
    }
  }

  /**
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param data           data
   * @param actualDataType actual data type
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataType(String data, DataType actualDataType) {

    if (null == data || data.isEmpty() || CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(data)) {
      return null;
    }
    try {
      switch (actualDataType) {
        case INT:
          return Integer.parseInt(data);
        case SHORT:
          return Short.parseShort(data);
        case DOUBLE:
          return Double.parseDouble(data);
        case LONG:
          return Long.parseLong(data);
        case TIMESTAMP:
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr = null;
          try {
            dateToStr = parser.parse(data);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error("Cannot convert" + data + " to Time/Long type value" + e.getMessage());
            return null;
          }
        case DECIMAL:
          java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data);
          scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
          org.apache.spark.sql.types.Decimal decConverter =
              new org.apache.spark.sql.types.Decimal();
          return decConverter.set(scalaDecVal);
        default:
          return UTF8String.fromString(data);
      }
    } catch (NumberFormatException ex) {
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }

  }

  public static Object getMeasureDataBasedOnDataType(Object data, DataType dataType) {

    if (null == data) {
      return null;
    }
    try {
      switch (dataType) {
        case DOUBLE:
          return data;
        case LONG:
          return data;
        case DECIMAL:
          java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data.toString());
          scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
          org.apache.spark.sql.types.Decimal decConverter =
              new org.apache.spark.sql.types.Decimal();
          return decConverter.set(scalaDecVal);
        default:
          return data;
      }
    } catch (NumberFormatException ex) {
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }

  }
}
