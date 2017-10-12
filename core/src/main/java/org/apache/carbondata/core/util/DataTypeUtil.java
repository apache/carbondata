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
import java.nio.ByteBuffer;
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
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

public final class DataTypeUtil {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataTypeUtil.class.getName());
  private static final Map<String, String> dataTypeDisplayNames;

  private static final ThreadLocal<DateFormat> timeStampformatter = new ThreadLocal<DateFormat>() {
    @Override protected DateFormat initialValue() {
      DateFormat dateFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
      dateFormat.setLenient(false);
      return dateFormat;
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
    dataTypeDisplayNames.put(DataTypes.DATE.toString(), DataTypes.DATE.getName());
    dataTypeDisplayNames.put(DataTypes.LONG.toString(), DataTypes.LONG.getName());
    dataTypeDisplayNames.put(DataTypes.INT.toString(), DataTypes.INT.getName());
    dataTypeDisplayNames.put(DataTypes.FLOAT.toString(), DataTypes.FLOAT.getName());
    dataTypeDisplayNames.put(DataTypes.BOOLEAN.toString(), DataTypes.BOOLEAN.getName());
    dataTypeDisplayNames.put(DataTypes.NULL.toString(), DataTypes.NULL.getName());
    dataTypeDisplayNames.put(DataTypes.DECIMAL.toString(), DataTypes.DECIMAL.getName());
    dataTypeDisplayNames.put(DataTypes.ARRAY.toString(), DataTypes.ARRAY.getName());
    dataTypeDisplayNames.put(DataTypes.STRUCT.toString(), DataTypes.STRUCT.getName());
    dataTypeDisplayNames.put(DataTypes.TIMESTAMP.toString(), DataTypes.TIMESTAMP.getName());
    dataTypeDisplayNames.put(DataTypes.DATE.toString(), DataTypes.DATE.getName());
    dataTypeDisplayNames.put(DataTypes.SHORT.toString(), DataTypes.SHORT.getName());
    dataTypeDisplayNames.put(DataTypes.STRING.toString(), DataTypes.STRING.getName());
  }

  /**
   * DataType converter for different computing engines
   */
  private static DataTypeConverter converter;

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
    if (dataType == DataTypes.DECIMAL) {
      BigDecimal bigDecimal =
          new BigDecimal(msrValue).setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
      return normalizeDecimalValue(bigDecimal, carbonMeasure.getPrecision());
    } else if (dataType == DataTypes.SHORT) {
      return Short.parseShort(msrValue);
    } else if (dataType == DataTypes.INT) {
      return Integer.parseInt(msrValue);
    } else if (dataType == DataTypes.LONG) {
      return Long.valueOf(msrValue);
    } else {
      Double parsedValue = Double.valueOf(msrValue);
      if (Double.isInfinite(parsedValue) || Double.isNaN(parsedValue)) {
        return null;
      }
      return parsedValue;
    }
  }

  public static Object getMeasureObjectFromDataType(byte[] data, DataType dataType) {
    if (data == null || data.length == 0) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.wrap(data);
    if (dataType == DataTypes.SHORT) {
      return (short) bb.getLong();
    } else if (dataType == DataTypes.INT) {
      return (int) bb.getLong();
    } else if (dataType == DataTypes.LONG) {
      return bb.getLong();
    } else if (dataType == DataTypes.DECIMAL) {
      return byteToBigDecimal(data);
    } else {
      return bb.getDouble();
    }
  }

  public static Object getMeasureObjectBasedOnDataType(ColumnPage measurePage, int index,
      DataType dataType, CarbonMeasure carbonMeasure) {
    if (dataType == DataTypes.SHORT) {
      return (short) measurePage.getLong(index);
    } else if (dataType == DataTypes.INT) {
      return (int) measurePage.getLong(index);
    } else if (dataType == DataTypes.LONG) {
      return measurePage.getLong(index);
    } else if (dataType == DataTypes.DECIMAL) {
      BigDecimal bigDecimalMsrValue = measurePage.getDecimal(index);
      if (null != bigDecimalMsrValue && carbonMeasure.getScale() > bigDecimalMsrValue.scale()) {
        bigDecimalMsrValue =
            bigDecimalMsrValue.setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
      }
      if (null != bigDecimalMsrValue) {
        return normalizeDecimalValue(bigDecimalMsrValue, carbonMeasure.getPrecision());
      } else {
        return bigDecimalMsrValue;
      }
    } else {
      return measurePage.getDouble(index);
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
        dataType = DataTypes.DATE;
        break;
      case "TIMESTAMP":
        dataType = DataTypes.TIMESTAMP;
        break;
      case "STRING":
        dataType = DataTypes.STRING;
        break;
      case "INT":
        dataType = DataTypes.INT;
        break;
      case "SMALLINT":
        dataType = DataTypes.SHORT;
        break;
      case "LONG":
        dataType = DataTypes.LONG;
        break;
      case "DOUBLE":
        dataType = DataTypes.DOUBLE;
        break;
      case "DECIMAL":
        dataType = DataTypes.DECIMAL;
        break;
      case "ARRAY":
        dataType = DataTypes.ARRAY;
        break;
      case "STRUCT":
        dataType = DataTypes.STRUCT;
        break;
      case "MAP":
      default:
        dataType = DataTypes.STRING;
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
    return getDataBasedOnDataType(data, actualDataType, getDataTypeConverter());
  }

  /**
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param data           data
   * @param actualDataType actual data type
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataType(String data, DataType actualDataType,
      DataTypeConverter converter) {
    if (null == data || CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(data)) {
      return null;
    }
    try {
      if (actualDataType == DataTypes.INT) {
        if (data.isEmpty()) {
          return null;
        }
        return Integer.parseInt(data);
      } else if (actualDataType == DataTypes.SHORT) {
        if (data.isEmpty()) {
          return null;
        }
        return Short.parseShort(data);
      } else if (actualDataType == DataTypes.FLOAT) {
        if (data.isEmpty()) {
          return null;
        }
        return Float.parseFloat(data);
      } else if (actualDataType == DataTypes.DOUBLE) {
        if (data.isEmpty()) {
          return null;
        }
        return Double.parseDouble(data);
      } else if (actualDataType == DataTypes.LONG) {
        if (data.isEmpty()) {
          return null;
        }
        return Long.parseLong(data);
      } else if (actualDataType == DataTypes.DATE) {
        if (data.isEmpty()) {
          return null;
        }
        try {
          Date dateToStr = dateformatter.get().parse(data);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage());
          return null;
        }
      } else if (actualDataType == DataTypes.TIMESTAMP) {
        if (data.isEmpty()) {
          return null;
        }
        try {
          Date dateToStr = timeStampformatter.get().parse(data);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage());
          return null;
        }
      } else if (actualDataType == DataTypes.DECIMAL) {
        if (data.isEmpty()) {
          return null;
        }
        return converter.convertToDecimal(data);
      } else {
        return converter.convertFromStringToUTF8String(data);
      }
    } catch (NumberFormatException ex) {
      LOGGER.error("Problem while converting data type" + data);
      return null;
    }
  }

  public static byte[] getBytesBasedOnDataTypeForNoDictionaryColumn(String dimensionValue,
      DataType actualDataType, String dateFormat) {
    if (actualDataType == DataTypes.STRING) {
      return ByteUtil.toBytes(dimensionValue);
    } else if (actualDataType == DataTypes.BOOLEAN) {
      return ByteUtil.toBytes(Boolean.parseBoolean(dimensionValue));
    } else if (actualDataType == DataTypes.SHORT) {
      return ByteUtil.toBytes(Short.parseShort(dimensionValue));
    } else if (actualDataType == DataTypes.INT) {
      return ByteUtil.toBytes(Integer.parseInt(dimensionValue));
    } else if (actualDataType == DataTypes.LONG) {
      return ByteUtil.toBytes(Long.parseLong(dimensionValue));
    } else if (actualDataType == DataTypes.TIMESTAMP) {
      Date dateToStr = null;
      DateFormat dateFormatter = null;
      try {
        if (null != dateFormat) {
          dateFormatter = new SimpleDateFormat(dateFormat);
        } else {
          dateFormatter = timeStampformatter.get();
        }
        dateToStr = dateFormatter.parse(dimensionValue);
        return ByteUtil.toBytes(dateToStr.getTime());
      } catch (ParseException e) {
        throw new NumberFormatException(e.getMessage());
      }
    } else {
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
      if (actualDataType == DataTypes.STRING) {
        return getDataTypeConverter().convertFromByteToUTF8String(dataInBytes);
      } else if (actualDataType == DataTypes.BOOLEAN) {
        return ByteUtil.toBoolean(dataInBytes);
      } else if (actualDataType == DataTypes.SHORT) {
        return ByteUtil.toShort(dataInBytes, 0, dataInBytes.length);
      } else if (actualDataType == DataTypes.INT) {
        return ByteUtil.toInt(dataInBytes, 0, dataInBytes.length);
      } else if (actualDataType == DataTypes.LONG) {
        return ByteUtil.toLong(dataInBytes, 0, dataInBytes.length);
      } else if (actualDataType == DataTypes.TIMESTAMP) {
        return ByteUtil.toLong(dataInBytes, 0, dataInBytes.length) * 1000L;
      } else {
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
      DataType dataType = dimension.getDataType();
      if (dataType == DataTypes.INT) {
        String data1 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data1.isEmpty()) {
          return null;
        }
        return Integer.parseInt(data1);
      } else if (dataType == DataTypes.SHORT) {
        String data2 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data2.isEmpty()) {
          return null;
        }
        return Short.parseShort(data2);
      } else if (dataType == DataTypes.DOUBLE) {
        String data3 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data3.isEmpty()) {
          return null;
        }
        return Double.parseDouble(data3);
      } else if (dataType == DataTypes.LONG) {
        String data4 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data4.isEmpty()) {
          return null;
        }
        return Long.parseLong(data4);
      } else if (dataType == DataTypes.DATE) {
        String data5 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data5.isEmpty()) {
          return null;
        }
        try {
          Date dateToStr = dateformatter.get().parse(data5);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage());
          return null;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        String data6 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data6.isEmpty()) {
          return null;
        }
        try {
          Date dateToStr = timeStampformatter.get().parse(data6);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage());
          return null;
        }
      } else if (dataType == DataTypes.DECIMAL) {
        String data7 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data7.isEmpty()) {
          return null;
        }
        java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data7);
        if (dimension.getColumnSchema().getScale() > javaDecVal.scale()) {
          javaDecVal = javaDecVal.setScale(dimension.getColumnSchema().getScale());
        }
        return getDataTypeConverter().convertToDecimal(javaDecVal);
      } else {
        return getDataTypeConverter().convertFromByteToUTF8String(dataInBytes);
      }
    } catch (NumberFormatException ex) {
      String data = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
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
      if (actualDataType == DataTypes.SHORT) {
        parsedValue = Short.parseShort(data);
      } else if (actualDataType == DataTypes.INT) {
        parsedValue = Integer.parseInt(data);
      } else if (actualDataType == DataTypes.LONG) {
        parsedValue = Long.parseLong(data);
      } else {
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
      DataType dataType = dimension.getDataType();
      if (dataType == DataTypes.DECIMAL) {
        return parseStringToBigDecimal(value, dimension);
      } else if (dataType == DataTypes.SHORT || dataType == DataTypes.INT ||
          dataType == DataTypes.LONG) {
        parsedValue = normalizeIntAndLongValues(value, dimension.getDataType());
      } else if (dataType == DataTypes.DOUBLE) {
        parsedValue = Double.parseDouble(value);
      } else {
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
      DataType dataType = dimension.getDataType();
      if (dataType == DataTypes.DECIMAL) {
        return parseStringToBigDecimal(value, dimension);
      } else if (dataType == DataTypes.INT) {
        Integer.parseInt(value);
      } else if (dataType == DataTypes.DOUBLE) {
        Double.parseDouble(value);
      } else if (dataType == DataTypes.LONG) {
        Long.parseLong(value);
      } else if (dataType == DataTypes.FLOAT) {
        Float.parseFloat(value);
      } else {
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
      DataType dataType = columnSchema.getDataType();
      if (dataType == DataTypes.INT) {
        parsedIntVal = (long) Integer.parseInt(data);
        return String.valueOf(parsedIntVal)
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (dataType == DataTypes.SHORT) {
        parsedIntVal = (long) Short.parseShort(data);
        return String.valueOf(parsedIntVal)
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (dataType == DataTypes.DOUBLE) {
        return String.valueOf(Double.parseDouble(data))
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (dataType == DataTypes.LONG) {
        return String.valueOf(Long.parseLong(data))
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (dataType == DataTypes.DATE) {
        DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(columnSchema.getDataType());
        int value = directDictionaryGenerator.generateDirectSurrogateKey(data);
        return String.valueOf(value)
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (columnSchema.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          DirectDictionaryGenerator directDictionaryGenerator1 = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(columnSchema.getDataType());
          int value1 = directDictionaryGenerator1.generateDirectSurrogateKey(data);
          return String.valueOf(value1)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        } else {
          try {
            Date dateToStr = timeStampformatter.get().parse(data);
            return ByteUtil.toBytes(dateToStr.getTime());
          } catch (ParseException e) {
            LOGGER.error(
                "Cannot convert value to Time/Long type value. Value is considered as null" + e
                    .getMessage());
            return null;
          }
        }
      } else if (dataType == DataTypes.DECIMAL) {
        String parsedValue = parseStringToBigDecimal(data, columnSchema);
        if (null == parsedValue) {
          return null;
        }
        java.math.BigDecimal javaDecVal = new java.math.BigDecimal(parsedValue);
        return bigDecimalToByte(javaDecVal);
      } else {
        return getDataTypeConverter().convertFromStringToByte(data);
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
      DataType dataType = columnSchema.getDataType();
      if (dataType == DataTypes.DECIMAL) {
        return parseStringToBigDecimal(value, columnSchema);
      } else if (dataType == DataTypes.SHORT || dataType == DataTypes.INT ||
          dataType == DataTypes.LONG) {
        parsedValue = normalizeIntAndLongValues(value, columnSchema.getDataType());
      } else if (dataType == DataTypes.DOUBLE) {
        parsedValue = Double.parseDouble(value);
      } else {
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

  /**
   * set the data type converter as per computing engine
   * @param converterLocal
   */
  public static void setDataTypeConverter(DataTypeConverter converterLocal) {
    converter = converterLocal;
  }

  public static DataTypeConverter getDataTypeConverter() {
    if (converter == null) {
      converter = new DataTypeConverterImpl();
    }
    return converter;
  }

}