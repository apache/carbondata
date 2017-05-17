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

package org.apache.carbondata.core.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.spark.unsafe.types.UTF8String;

public final class DataTypeUtil {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataTypeUtil.class.getName());
  private static final Map<String, String> dataTypeDisplayNames;

  private static final ThreadLocal<DateFormat> timeStampformatter = new ThreadLocal<DateFormat>() {
    @Override protected DateFormat initialValue() {
      return new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
    }
  };

  private static final ThreadLocal<DateFormat> dateformatter = new ThreadLocal<DateFormat>() {
    @Override protected DateFormat initialValue() {
      return new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
              CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));
    }
  };

  static {
    dataTypeDisplayNames = new HashMap<String, String>(16);
    dataTypeDisplayNames.put(DataType.DATE.toString(), DataType.DATE.getName());
    dataTypeDisplayNames.put(DataType.LONG.toString(), DataType.LONG.getName());
    dataTypeDisplayNames.put(DataType.INT.toString(), DataType.INT.getName());
    dataTypeDisplayNames.put(DataType.FLOAT.toString(), DataType.FLOAT.getName());
    dataTypeDisplayNames.put(DataType.BOOLEAN.toString(), DataType.BOOLEAN.getName());
    dataTypeDisplayNames.put(DataType.NULL.toString(), DataType.NULL.getName());
    dataTypeDisplayNames.put(DataType.DECIMAL.toString(), DataType.DECIMAL.getName());
    dataTypeDisplayNames.put(DataType.ARRAY.toString(), DataType.ARRAY.getName());
    dataTypeDisplayNames.put(DataType.STRUCT.toString(), DataType.STRUCT.getName());
    dataTypeDisplayNames.put(DataType.TIMESTAMP.toString(), DataType.TIMESTAMP.getName());
    dataTypeDisplayNames.put(DataType.DATE.toString(), DataType.DATE.getName());
    dataTypeDisplayNames.put(DataType.SHORT.toString(), DataType.SHORT.getName());
    dataTypeDisplayNames.put(DataType.STRING.toString(), DataType.STRING.getName());
  }

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
      case SHORT:
        return Short.parseShort(msrValue);
      case INT:
        return Integer.parseInt(msrValue);
      case LONG:
        return Long.valueOf(msrValue);
      default:
        Double parsedValue = Double.valueOf(msrValue);
        if (Double.isInfinite(parsedValue) || Double.isNaN(parsedValue)) {
          return null;
        }
        return parsedValue;
    }
  }

  /**
   * @param dataType
   * @return
   */
  public static String getColumnDataTypeDisplayName(String dataType) {
    return dataTypeDisplayNames.get(dataType);
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
      case SHORT:
      case INT:
      case LONG:
        return CarbonCommonConstants.BIG_INT_MEASURE;
      default:
        return CarbonCommonConstants.DOUBLE_MEASURE;
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
    byte[] bscale = { (byte) (scale) };
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
   * This method will convert a byte value back to big decimal value
   *
   * @param raw
   * @return
   */
  public static BigDecimal byteToBigDecimal(byte[] raw, int offset, int length) {
    int scale = (raw[offset] & 0xFF);
    byte[] unscale = new byte[length - 1];
    System.arraycopy(raw, offset + 1, unscale, 0, unscale.length);
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
      case "DATE":
        dataType = DataType.DATE;
        break;
      case "TIMESTAMP":
        dataType = DataType.TIMESTAMP;
        break;
      case "STRING":
        dataType = DataType.STRING;
        break;
      case "INT":
        dataType = DataType.INT;
        break;
      case "SMALLINT":
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
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param data           data
   * @param actualDataType actual data type
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataType(String data, DataType actualDataType) {
    if (null == data || CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(data)) {
      return null;
    }
    try {
      switch (actualDataType) {
        case INT:
          if (data.isEmpty()) {
            return null;
          }
          return Integer.parseInt(data);
        case SHORT:
          if (data.isEmpty()) {
            return null;
          }
          return Short.parseShort(data);
        case DOUBLE:
          if (data.isEmpty()) {
            return null;
          }
          return Double.parseDouble(data);
        case LONG:
          if (data.isEmpty()) {
            return null;
          }
          return Long.parseLong(data);
        case DATE:
          if (data.isEmpty()) {
            return null;
          }
          try {
            Date dateToStr = dateformatter.get().parse(data);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error("Cannot convert" + data + " to Time/Long type value" + e.getMessage());
            return null;
          }

        case TIMESTAMP:
          if (data.isEmpty()) {
            return null;
          }
          try {
            Date dateToStr = timeStampformatter.get().parse(data);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error("Cannot convert" + data + " to Time/Long type value" + e.getMessage());
            return null;
          }
        case DECIMAL:
          if (data.isEmpty()) {
            return null;
          }
          java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data);
          return org.apache.spark.sql.types.Decimal.apply(javaDecVal);
        default:
          return UTF8String.fromString(data);
      }
    } catch (NumberFormatException ex) {
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }

  }

  public static byte[] getBytesBasedOnDataTypeForNoDictionaryColumn(String dimensionValue,
      DataType actualDataType) {
    switch (actualDataType) {
      case STRING:
        return ByteUtil.toBytes(dimensionValue);
      case BOOLEAN:
        return ByteUtil.toBytes(Boolean.parseBoolean(dimensionValue));
      case SHORT:
        return ByteUtil.toBytes(Short.parseShort(dimensionValue));
      case INT:
        return ByteUtil.toBytes(Integer.parseInt(dimensionValue));
      case FLOAT:
        return ByteUtil.toBytes(Float.parseFloat(dimensionValue));
      case LONG:
        return ByteUtil.toBytes(Long.parseLong(dimensionValue));
      case DOUBLE:
        return ByteUtil.toBytes(Double.parseDouble(dimensionValue));
      case DECIMAL:
        return ByteUtil.toBytes(new BigDecimal(dimensionValue));
      default:
        return ByteUtil.toBytes(dimensionValue);
    }
  }


  /**
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param dataInBytes    data
   * @param actualDataType actual data type
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataTypeForNoDictionaryColumn(byte[] dataInBytes,
      DataType actualDataType) {
    if (null == dataInBytes || Arrays
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, dataInBytes)) {
      return null;
    }
    try {
      switch (actualDataType) {
        case STRING:
          return UTF8String.fromBytes(dataInBytes);
        case BOOLEAN:
          return ByteUtil.toBoolean(dataInBytes);
        case SHORT:
          return ByteUtil.toShort(dataInBytes, 0, dataInBytes.length);
        case INT:
          return ByteUtil.toInt(dataInBytes, 0, dataInBytes.length);
        case FLOAT:
          return ByteUtil.toFloat(dataInBytes, 0);
        case LONG:
          return ByteUtil.toLong(dataInBytes, 0, dataInBytes.length);
        case DOUBLE:
          return ByteUtil.toDouble(dataInBytes, 0);
        case DECIMAL:
          return ByteUtil.toBigDecimal(dataInBytes, 0, dataInBytes.length);
        default:
          return ByteUtil.toString(dataInBytes, 0, dataInBytes.length);
      }
    } catch (Throwable ex) {
      String data = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
      LOGGER.error("Cannot convert" + data + " to " + actualDataType.getName() + " type value" + ex
          .getMessage());
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }
  }


  /**
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param dataInBytes data
   * @param dimension
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataType(byte[] dataInBytes, CarbonDimension dimension) {
    if (null == dataInBytes || Arrays
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, dataInBytes)) {
      return null;
    }
    try {
      switch (dimension.getDataType()) {
        case INT:
          String data1 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data1.isEmpty()) {
            return null;
          }
          return Integer.parseInt(data1);
        case SHORT:
          String data2 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data2.isEmpty()) {
            return null;
          }
          return Short.parseShort(data2);
        case DOUBLE:
          String data3 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data3.isEmpty()) {
            return null;
          }
          return Double.parseDouble(data3);
        case LONG:
          String data4 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data4.isEmpty()) {
            return null;
          }
          return Long.parseLong(data4);
        case DATE:
          String data5 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data5.isEmpty()) {
            return null;
          }
          try {
            Date dateToStr = dateformatter.get().parse(data5);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error("Cannot convert" + data5 + " to Time/Long type value" + e.getMessage());
            return null;
          }

        case TIMESTAMP:
          String data6 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data6.isEmpty()) {
            return null;
          }
          try {
            Date dateToStr = timeStampformatter.get().parse(data6);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error("Cannot convert" + data6 + " to Time/Long type value" + e.getMessage());
            return null;
          }
        case DECIMAL:
          String data7 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
          if (data7.isEmpty()) {
            return null;
          }
          java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data7);
          if (dimension.getColumnSchema().getScale() > javaDecVal.scale()) {
            javaDecVal = javaDecVal.setScale(dimension.getColumnSchema().getScale());
          }
          return org.apache.spark.sql.types.Decimal.apply(javaDecVal);
        default:
          return UTF8String.fromBytes(dataInBytes);
      }
    } catch (NumberFormatException ex) {
      String data = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
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
          return org.apache.spark.sql.types.Decimal.apply((java.math.BigDecimal) data);
        default:
          return data;
      }
    } catch (NumberFormatException ex) {
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }

  }

  /**
   * Below method will be used to basically to know whether any non parseable
   * data is present or not. if present then return null so that system can
   * process to default null member value.
   *
   * @param data           data
   * @param actualDataType actual data type
   * @return actual data after conversion
   */
  public static Object normalizeIntAndLongValues(String data, DataType actualDataType) {
    if (null == data) {
      return null;
    }
    try {
      Object parsedValue = null;
      switch (actualDataType) {
        case SHORT:
          parsedValue = Short.parseShort(data);
          break;
        case INT:
          parsedValue = Integer.parseInt(data);
          break;
        case LONG:
          parsedValue = Long.parseLong(data);
          break;
        default:
          return data;
      }
      if (null != parsedValue) {
        return data;
      }
      return null;
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  /**
   * This method will parse a given string value corresponding to its data type
   *
   * @param value     value to parse
   * @param dimension dimension to get data type and precision and scale in case of decimal
   *                  data type
   * @return
   */
  public static String normalizeColumnValueForItsDataType(String value, CarbonDimension dimension) {
    try {
      Object parsedValue = null;
      // validation will not be done for timestamp datatype as for timestamp direct dictionary
      // is generated. No dictionary file is created for timestamp datatype column
      switch (dimension.getDataType()) {
        case DECIMAL:
          return parseStringToBigDecimal(value, dimension);
        case SHORT:
        case INT:
        case LONG:
          parsedValue = normalizeIntAndLongValues(value, dimension.getDataType());
          break;
        case DOUBLE:
          parsedValue = Double.parseDouble(value);
          break;
        default:
          return value;
      }
      if (null != parsedValue) {
        return value;
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * This method will parse a value to its datatype if datatype is decimal else will return
   * the value passed
   *
   * @param value     value to be parsed
   * @param dimension
   * @return
   */
  public static String parseValue(String value, CarbonDimension dimension) {
    if (null == value) {
      return null;
    }
    try {
      switch (dimension.getDataType()) {
        case DECIMAL:
          return parseStringToBigDecimal(value, dimension);
        case INT:
          Integer.parseInt(value);
          break;
        case DOUBLE:
          Double.parseDouble(value);
          break;
        case LONG:
          Long.parseLong(value);
          break;
        case FLOAT:
          Float.parseFloat(value);
          break;
        default:
          // do nothing
      }
    } catch (NumberFormatException e) {
      return null;
    }
    return value;
  }

  private static String parseStringToBigDecimal(String value, CarbonDimension dimension) {
    BigDecimal bigDecimal = new BigDecimal(value)
        .setScale(dimension.getColumnSchema().getScale(), RoundingMode.HALF_UP);
    BigDecimal normalizedValue =
        normalizeDecimalValue(bigDecimal, dimension.getColumnSchema().getPrecision());
    if (null != normalizedValue) {
      return normalizedValue.toString();
    }
    return null;
  }

  /**
   * This method will compare double values it will preserve
   * the -0.0 and 0.0 equality as per == ,also preserve NaN equality check as per
   * java.lang.Double.equals()
   *
   * @param d1 double value for equality check
   * @param d2 double value for equality check
   * @return boolean after comparing two double values.
   */
  public static int compareDoubleWithNan(Double d1, Double d2) {
    if ((d1.doubleValue() == d2.doubleValue()) || (Double.isNaN(d1) && Double.isNaN(d2))) {
      return 0;
    } else if (d1 < d2) {
      return -1;
    }
    return 1;
  }

  /**
   * Below method will be used to convert the data into byte[]
   *
   * @param data
   * @param columnSchema
   * @return actual data in byte[]
   */
  public static byte[] convertDataToBytesBasedOnDataType(String data, ColumnSchema columnSchema) {
    if (null == data) {
      return null;
    } else if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(data)) {
      LOGGER.error("Default value should not be carbon specific null value : " + data);
      return null;
    }
    try {
      long parsedIntVal = 0;
      switch (columnSchema.getDataType()) {
        case INT:
          parsedIntVal = (long) Integer.parseInt(data);
          return String.valueOf(parsedIntVal)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        case SHORT:
          parsedIntVal = (long) Short.parseShort(data);
          return String.valueOf(parsedIntVal)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        case DOUBLE:
          return String.valueOf(Double.parseDouble(data))
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        case LONG:
          return String.valueOf(Long.parseLong(data))
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        case DATE:
        case TIMESTAMP:
          DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(columnSchema.getDataType());
          int value = directDictionaryGenerator.generateDirectSurrogateKey(data);
          return String.valueOf(value)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        case DECIMAL:
          String parsedValue = parseStringToBigDecimal(data, columnSchema);
          if (null == parsedValue) {
            return null;
          }
          java.math.BigDecimal javaDecVal = new java.math.BigDecimal(parsedValue);
          return bigDecimalToByte(javaDecVal);
        default:
          return UTF8String.fromString(data).getBytes();
      }
    } catch (NumberFormatException ex) {
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }
  }

  /**
   * This method will parse a given string value corresponding to its data type
   *
   * @param value        value to parse
   * @param columnSchema dimension to get data type and precision and scale in case of decimal
   *                     data type
   * @return
   */
  public static String normalizeColumnValueForItsDataType(String value, ColumnSchema columnSchema) {
    try {
      Object parsedValue = null;
      switch (columnSchema.getDataType()) {
        case DECIMAL:
          return parseStringToBigDecimal(value, columnSchema);
        case SHORT:
        case INT:
        case LONG:
          parsedValue = normalizeIntAndLongValues(value, columnSchema.getDataType());
          break;
        case DOUBLE:
          parsedValue = Double.parseDouble(value);
          break;
        default:
          return value;
      }
      if (null != parsedValue) {
        return value;
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private static String parseStringToBigDecimal(String value, ColumnSchema columnSchema) {
    BigDecimal bigDecimal =
        new BigDecimal(value).setScale(columnSchema.getScale(), RoundingMode.HALF_UP);
    BigDecimal normalizedValue = normalizeDecimalValue(bigDecimal, columnSchema.getPrecision());
    if (null != normalizedValue) {
      return normalizedValue.toString();
    }
    return null;
  }
}