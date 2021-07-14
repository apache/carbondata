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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.log4j.Logger;

public final class DataTypeUtil {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataTypeUtil.class.getName());

  private static final ThreadLocal<DateFormat> timestampFormatter = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      DateFormat dateFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
      dateFormat.setLenient(false);
      return dateFormat;
    }
  };

  private static final ThreadLocal<DateFormat> dateFormatter = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      return new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
              CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));
    }
  };

  /**
   * DataType converter for different computing engines
   */
  private static DataTypeConverter converter;

  /**
   * This method will convert a given value to its specific type
   *
   * @param msrValue
   * @param dataType
   * @return
   */
  public static Object getMeasureValueBasedOnDataType(String msrValue, DataType dataType, int scale,
      int precision) {
    return getMeasureValueBasedOnDataType(msrValue, dataType, scale, precision, false);
  }

  /**
   * This method will convert a given value to its specific type
   *
   * @param msrValue
   * @param dataType
   * @return
   */
  public static Object getMeasureValueBasedOnDataType(String msrValue, DataType dataType,
      int scale, int precision, boolean useConverter) {
    if (dataType == DataTypes.BOOLEAN) {
      return BooleanConvert.parseBoolean(msrValue);
    } else if (DataTypes.isDecimal(dataType)) {
      BigDecimal bigDecimal =
          new BigDecimal(msrValue).setScale(scale, RoundingMode.HALF_UP);
      BigDecimal decimal = normalizeDecimalValue(bigDecimal, precision);
      if (useConverter) {
        return converter.convertFromBigDecimalToDecimal(decimal);
      } else {
        return decimal;
      }
    } else if (dataType == DataTypes.SHORT) {
      return Short.parseShort(msrValue);
    } else if (dataType == DataTypes.INT) {
      return Integer.parseInt(msrValue);
    } else if (dataType == DataTypes.LONG) {
      return Long.valueOf(msrValue);
    } else if (dataType == DataTypes.FLOAT) {
      return Float.parseFloat(msrValue);
    } else if (dataType == DataTypes.BYTE) {
      return Byte.parseByte(msrValue);
    } else {
      double parsedValue = Double.parseDouble(msrValue);
      if (Double.isInfinite(parsedValue) || Double.isNaN(parsedValue)) {
        return null;
      }
      return parsedValue;
    }
  }

  /**
   * This method will convert a given value to its specific type
   *
   * @param dimValue
   * @param dataType
   * @return
   */
  public static Object getNoDictionaryValueBasedOnDataType(String dimValue, DataType dataType,
      int scale, int precision, boolean useConverter, String timeStampFormat) {
    if (dataType == DataTypes.BOOLEAN) {
      return BooleanConvert.parseBoolean(dimValue);
    } else if (DataTypes.isDecimal(dataType)) {
      BigDecimal bigDecimal = new BigDecimal(dimValue).setScale(scale, RoundingMode.HALF_UP);
      BigDecimal decimal = normalizeDecimalValue(bigDecimal, precision);
      if (useConverter) {
        return converter.convertFromBigDecimalToDecimal(decimal);
      } else {
        return decimal;
      }
    } else if (dataType == DataTypes.SHORT) {
      return Short.parseShort(dimValue);
    } else if (dataType == DataTypes.INT) {
      return Integer.parseInt(dimValue);
    } else if (dataType == DataTypes.LONG) {
      return Long.valueOf(dimValue);
    } else if (dataType == DataTypes.FLOAT) {
      return Float.parseFloat(dimValue);
    } else if (dataType == DataTypes.BYTE) {
      return Byte.parseByte(dimValue);
    } else if (dataType == DataTypes.TIMESTAMP) {
      return parseTimestamp(dimValue, timeStampFormat);
    } else {
      double parsedValue = Double.parseDouble(dimValue);
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
    if (dataType == DataTypes.BOOLEAN) {
      return BooleanConvert.byte2Boolean(bb.get());
    } else if (dataType == DataTypes.SHORT) {
      return (short) bb.getLong();
    } else if (dataType == DataTypes.INT) {
      return (int) bb.getLong();
    } else if (dataType == DataTypes.LONG) {
      return bb.getLong();
    } else if (dataType == DataTypes.FLOAT) {
      return bb.getFloat();
    } else if (dataType == DataTypes.BYTE) {
      return bb.get();
    } else if (DataTypes.isDecimal(dataType)) {
      return byteToBigDecimal(data);
    } else {
      return bb.getDouble();
    }
  }

  public static Object getMeasureObjectBasedOnDataType(ColumnPage measurePage, int index,
      DataType dataType, CarbonMeasure carbonMeasure) {
    if (dataType == DataTypes.BOOLEAN) {
      return measurePage.getBoolean(index);
    } else if (dataType == DataTypes.SHORT) {
      return (short) measurePage.getLong(index);
    } else if (dataType == DataTypes.INT) {
      return (int) measurePage.getLong(index);
    } else if (dataType == DataTypes.LONG) {
      return measurePage.getLong(index);
    } else if (dataType == DataTypes.FLOAT) {
      return measurePage.getFloat(index);
    } else if (dataType == DataTypes.BYTE) {
      return measurePage.getByte(index);
    } else if (DataTypes.isDecimal(dataType)) {
      BigDecimal bigDecimalMsrValue = measurePage.getDecimal(index);
      if (null != bigDecimalMsrValue && carbonMeasure.getScale() > bigDecimalMsrValue.scale()) {
        bigDecimalMsrValue =
            bigDecimalMsrValue.setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
      }
      if (null != bigDecimalMsrValue) {
        return normalizeDecimalValue(bigDecimalMsrValue, carbonMeasure.getPrecision());
      } else {
        return null;
      }
    } else {
      return measurePage.getDouble(index);
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
   * This method will convert a big decimal value to bytes
   *
   * @param num
   * @return
   */
  public static byte[] bigDecimalToByte(BigDecimal num) {
    BigInteger sig = new BigInteger(num.unscaledValue().toString());
    int scale = num.scale();
    byte[] scaleBytes = { (byte) (scale) };
    byte[] buff = sig.toByteArray();
    byte[] completeArr = new byte[buff.length + scaleBytes.length];
    System.arraycopy(scaleBytes, 0, completeArr, 0, scaleBytes.length);
    System.arraycopy(buff, 0, completeArr, scaleBytes.length, buff.length);
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
    byte[] value = new byte[raw.length - 1];
    System.arraycopy(raw, 1, value, 0, value.length);
    BigInteger sig = new BigInteger(value);
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
    byte[] value = new byte[length - 1];
    System.arraycopy(raw, offset + 1, value, 0, value.length);
    BigInteger sig = new BigInteger(value);
    return new BigDecimal(sig, scale);
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
      if (actualDataType == DataTypes.BOOLEAN) {
        if (data.isEmpty()) {
          return null;
        }
        return BooleanConvert.parseBoolean(data);
      } else if (actualDataType == DataTypes.INT) {
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
      } else if (actualDataType == DataTypes.BYTE) {
        if (data.isEmpty()) {
          return null;
        }
        return Byte.parseByte(data);
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
          Date dateToStr = dateFormatter.get().parse(data);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage(), e);
          return null;
        }
      } else if (actualDataType == DataTypes.TIMESTAMP) {
        if (data.isEmpty()) {
          return null;
        }
        try {
          Date dateToStr = timestampFormatter.get().parse(data);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage(), e);
          return null;
        }
      } else if (DataTypes.isDecimal(actualDataType)) {
        if (data.isEmpty()) {
          return null;
        }
        return converter.convertFromStringToDecimal(data);
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
    if (actualDataType == DataTypes.BOOLEAN) {
      return ByteUtil.toBytes(BooleanConvert.parseBoolean(dimensionValue));
    } else if (actualDataType == DataTypes.SHORT) {
      return ByteUtil.toXorBytes(Short.parseShort(dimensionValue));
    } else if (actualDataType == DataTypes.INT) {
      return ByteUtil.toXorBytes(Integer.parseInt(dimensionValue));
    } else if (actualDataType == DataTypes.LONG) {
      return ByteUtil.toXorBytes(Long.parseLong(dimensionValue));
    } else if (actualDataType == DataTypes.DOUBLE) {
      return ByteUtil.toXorBytes(Double.parseDouble(dimensionValue));
    } else if (actualDataType == DataTypes.FLOAT) {
      return ByteUtil.toXorBytes(Float.parseFloat(dimensionValue));
    } else if (actualDataType == DataTypes.BYTE) {
      return new byte[] { Byte.parseByte(dimensionValue) };
    } else if (DataTypes.isDecimal(actualDataType)) {
      return bigDecimalToByte(new BigDecimal(dimensionValue));
    } else if (actualDataType == DataTypes.TIMESTAMP) {
      Date dateToStr = null;
      DateFormat dateFormatter = null;
      try {
        if (null != dateFormat && !dateFormat.trim().isEmpty()) {
          dateFormatter = new SimpleDateFormat(dateFormat);
          dateFormatter.setLenient(false);
        } else {
          dateFormatter = timestampFormatter.get();
        }
        dateToStr = dateFormatter.parse(dimensionValue);
        return ByteUtil.toXorBytes(dateToStr.getTime());
      } catch (ParseException e) {
        throw new NumberFormatException(e.getMessage());
      }
    } else {
      // Default action for String/Varchar
      return ByteUtil.toBytes(dimensionValue);
    }
  }

  public static Object getDataDataTypeForNoDictionaryColumn(String dimensionValue,
      DataType actualDataType, String dateFormat) {
    if (actualDataType == DataTypes.BOOLEAN) {
      return BooleanConvert.parseBoolean(dimensionValue);
    } else if (actualDataType == DataTypes.SHORT) {
      return Short.parseShort(dimensionValue);
    } else if (actualDataType == DataTypes.INT) {
      return Integer.parseInt(dimensionValue);
    } else if (actualDataType == DataTypes.LONG) {
      return Long.parseLong(dimensionValue);
    } else if (actualDataType == DataTypes.DOUBLE) {
      return Double.parseDouble(dimensionValue);
    } else if (DataTypes.isDecimal(actualDataType)) {
      return new BigDecimal(dimensionValue);
    } else if (actualDataType == DataTypes.TIMESTAMP) {
      return parseTimestamp(dimensionValue, dateFormat);
    } else {
      // Default action for String/Varchar
      return converter.convertFromStringToUTF8String(dimensionValue);
    }
  }

  private static long createTimeInstant(String dimensionValue, String dateFormat) {
    // dateFormat is null, use default carbon timestamp format
    if (null == dateFormat || dateFormat.trim().isEmpty()) {
      dateFormat = CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT;
    } else if (!dateFormat.trim().contains(" ")) {
      // if format doesn't contain time segment for eg., dateFormat = yyyy/MM/dd, then append the
      // default time segment with format (yyyy/MM/dd HH:mm:ss), else LocalDateTime.parse will fail
      dateFormat += CarbonCommonConstants.CARBON_TIME_SEGMENT_DEFAULT_FORMAT;
    }
    String updatedDim = dimensionValue;
    // if value doesn't contain time segment data for eg., value = 2018/08/01, then append the
    // default time segment with dimValue(2018/08/01 00:00:00), else LocalDateTime.parse will fail
    if (!dimensionValue.trim().contains(" ")) {
      updatedDim += CarbonCommonConstants.CARBON_TIME_SEGMENT_DATA_DEFAULT_FORMAT;
    }
    // If format is yyyy-MM-dd HH:mm:ss and data is 2017-9-02 1:01:01, then parsing will fail,
    // because the month segment and hour segment data has single digit. Hence, add 0's to data,
    // if data doesn't matches the format length
    List<String> dateFormatPattern = new ArrayList<>();
    List<String> dimensionData = new ArrayList<>();
    StringBuilder format = new StringBuilder();
    // convert input data to proper format
    // separate year, month, day,... format segment's to a list
    for (int i = 0; i < dateFormat.length(); i++) {
      char c = dateFormat.charAt(i);
      if ((c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z')) {
        format.append(c);
      } else {
        String value = format.toString();
        if (value.equals("hh")) {
          value = "HH";
        }
        dateFormatPattern.add(value);
        dateFormatPattern.add(Character.toString(c));
        format = new StringBuilder();
      }
      if (i + 1 == dateFormat.length()) {
        dateFormatPattern.add(format.toString());
      }
    }
    format = new StringBuilder();
    // separate data year, month, day,.. to a list
    for (int i = 0; i < updatedDim.length(); i++) {
      char c = updatedDim.charAt(i);
      if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
        // bad record
        break;
      }
      if (c >= '0' && c <= '9') {
        format.append(c);
      } else {
        dimensionData.add(format.toString());
        dimensionData.add(Character.toString(c));
        format = new StringBuilder();
      }
      if (i + 1 == updatedDim.length()) {
        dimensionData.add(format.toString());
      }
    }
    // add 0's to year/month/day.. if the data format size doesn't match format size
    if (!dimensionData.isEmpty() && !(dimensionData.size() < dateFormatPattern.size())) {
      int i;
      for (i = 0; i < dateFormatPattern.size(); i++) {
        String currentTimestampFormat = dateFormatPattern.get(i);
        String currentDimData = dimensionData.get(i);
        if (currentTimestampFormat.length() != currentDimData.length()) {
          if (currentDimData.length() < currentTimestampFormat.length()) {
            dimensionData.set(i, "0" + currentDimData);
          }
        }
      }
      // if format is yyyy/MM/dd HH:mm:ss, and data is 2018/01/01 01:01:01.001, then parsing will
      // fail. In that case, remove the unnecessary data segment from the list
      if (dimensionData.size() > dateFormatPattern.size()) {
        dimensionData.subList(i, dimensionData.size()).clear();
      }
      // prepare the final format and data
      updatedDim = String.join("", dimensionData);
      dateFormat = String.join("", dateFormatPattern);
    }
    // create java instant
    Instant instant = Instant.from(ZonedDateTime
        .of(LocalDateTime.parse(updatedDim, DateTimeFormatter.ofPattern(dateFormat)),
            ZoneId.systemDefault()));
    validateTimeStampRange(instant.getEpochSecond());
    long us = Math.multiplyExact(instant.getEpochSecond(), 1000L);
    // get nanoseconds from instant
    int nano = instant.getNano();
    if (nano != 0) {
      while (nano % 10 == 0) {
        nano /= 10;
      }
    }
    return Math.addExact(us, nano);
  }

  private static Object parseTimestamp(String dimensionValue, String dateFormat) {
    Date dateToStr;
    DateFormat dateFormatter = null;
    long timeValue;
    try {
      if (Boolean.parseBoolean(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_SPARK_VERSION_SPARK3,
              CarbonCommonConstants.CARBON_SPARK_VERSION_SPARK3_DEFAULT))) {
        try {
          return createTimeInstant(dimensionValue, dateFormat.trim());
        } catch (DateTimeParseException e) {
          throw new NumberFormatException(e.getMessage());
        }
      }
      if (null != dateFormat && !dateFormat.trim().isEmpty()) {
        dateFormatter = new SimpleDateFormat(dateFormat);
        dateFormatter.setLenient(false);
      } else {
        dateFormatter = timestampFormatter.get();
      }
      dateToStr = dateFormatter.parse(dimensionValue);
      timeValue = dateToStr.getTime();
      validateTimeStampRange(timeValue);
      return timeValue;
    } catch (ParseException e) {
      // If the parsing fails, try to parse again with setLenient to true if the property is set
      // (example: 1941-03-15 00:00:00 is invalid data and will fail to parse in Asia/Shanghai zone
      // as DST is observed and clocks were turned forward 1 hour to 1941-03-15 01:00:00)
      if (CarbonProperties.getInstance().isSetLenientEnabled()) {
        try {
          dateFormatter.setLenient(true);
          dateToStr = dateFormatter.parse(dimensionValue);
          timeValue = dateToStr.getTime();
          validateTimeStampRange(timeValue);
          LOGGER.info("Parsed data with lenience as true, setting back to default mode");
          return timeValue;
        } catch (ParseException ex) {
          LOGGER.info("Failed to parse data with lenience as true, setting back to default mode");
          throw new NumberFormatException(ex.getMessage());
        } finally {
          dateFormatter.setLenient(false);
        }
      } else {
        throw new NumberFormatException(e.getMessage());
      }
    }
  }

  private static void validateTimeStampRange(Long timeValue) {
    if (timeValue < DateDirectDictionaryGenerator.MIN_VALUE
        || timeValue > DateDirectDictionaryGenerator.MAX_VALUE) {
      throw new NumberFormatException(
          "timestamp column data value: " + timeValue + "is not in valid range of: "
              + DateDirectDictionaryGenerator.MIN_VALUE + " and "
              + DateDirectDictionaryGenerator.MAX_VALUE);
    }
  }

  public static byte[] getBytesDataDataTypeForNoDictionaryColumn(Object dimensionValue,
      DataType actualDataType) {
    if (dimensionValue == null) {
      if (actualDataType == DataTypes.STRING) {
        return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      } else {
        return new byte[0];
      }
    }
    if (actualDataType == DataTypes.BOOLEAN) {
      return ByteUtil.toBytes((Boolean) dimensionValue);
    } else if (actualDataType == DataTypes.SHORT) {
      return ByteUtil.toXorBytes((Short) dimensionValue);
    } else if (actualDataType == DataTypes.INT) {
      return ByteUtil.toXorBytes((Integer) dimensionValue);
    } else if (actualDataType == DataTypes.LONG) {
      return ByteUtil.toXorBytes((Long) dimensionValue);
    } else if (actualDataType == DataTypes.DOUBLE) {
      return ByteUtil.toXorBytes((double) dimensionValue);
    } else if (actualDataType == DataTypes.FLOAT) {
      return ByteUtil.toXorBytes((float) dimensionValue);
    } else if (DataTypes.isDecimal(actualDataType)) {
      // Need to make BigDecimal object, else ByteUtil.toBytes will have precision loss
      return bigDecimalToByte(new BigDecimal(dimensionValue.toString()));
    } else if (actualDataType == DataTypes.BYTE) {
      return ByteUtil.toXorBytes((byte) dimensionValue);
    } else if (actualDataType == DataTypes.TIMESTAMP) {
      return ByteUtil.toXorBytes((Long) dimensionValue);
    } else if (actualDataType == DataTypes.BINARY) {
      if (dimensionValue instanceof String) {
        return ((String) dimensionValue).getBytes(
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else {
        return (byte[]) dimensionValue;
      }
    } else {
      // Default action for String/Varchar
      return ByteUtil.toBytes(dimensionValue.toString());
    }
  }

  /**
   * Convert the min/max values to bytes for no dictionary column
   *
   * @param dimensionValue
   * @param actualDataType
   * @return
   */
  public static byte[] getMinMaxBytesBasedOnDataTypeForNoDictionaryColumn(Object dimensionValue,
      DataType actualDataType) {
    if (dimensionValue == null) {
      if (actualDataType == DataTypes.STRING) {
        return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      } else {
        return new byte[0];
      }
    }
    if (actualDataType == DataTypes.BOOLEAN) {
      return ByteUtil.toBytes(ByteUtil.toBoolean((byte) dimensionValue));
    } else if (actualDataType == DataTypes.SHORT) {
      return ByteUtil.toXorBytes((Short) dimensionValue);
    } else if (actualDataType == DataTypes.INT) {
      return ByteUtil.toXorBytes((Integer) dimensionValue);
    } else if (actualDataType == DataTypes.LONG) {
      return ByteUtil.toXorBytes((Long) dimensionValue);
    } else if (actualDataType == DataTypes.TIMESTAMP) {
      return ByteUtil.toXorBytes((Long) dimensionValue);
    } else {
      // Default action for String/Varchar
      return ByteUtil.toBytes(dimensionValue.toString());
    }
  }

  /**
   * Returns true for fixed length DataTypes.
   * @param dataType
   * @return
   */
  public static boolean isFixedSizeDataType(DataType dataType) {
    return dataType != DataTypes.STRING
        && dataType != DataTypes.VARCHAR
        && dataType != DataTypes.BINARY
        && !DataTypes.isDecimal(dataType);
  }

  public static Object getDataBasedOnDataTypeForNoDictionaryColumn(byte[] dataInBytes,
      DataType actualDataType, boolean isTimeStampConversion) {
    return getDataBasedOnDataTypeForNoDictionaryColumn(dataInBytes, actualDataType,
        isTimeStampConversion, false);
  }

  /**
   * Wrapper for actual getDataBasedOnDataTypeForNoDictionaryColumn.
   *
   * @param dataInBytes
   * @param actualDataType
   * @return
   */
  public static Object getDataBasedOnDataTypeForNoDictionaryColumn(byte[] dataInBytes,
      DataType actualDataType) {
    return getDataBasedOnDataTypeForNoDictionaryColumn(dataInBytes, actualDataType, true, false);
  }

  /**
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param dataInBytes           data
   * @param actualDataType        actual data type
   * @param isTimeStampConversion
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataTypeForNoDictionaryColumn(byte[] dataInBytes,
      DataType actualDataType, boolean isTimeStampConversion, boolean getBytesData) {
    if (null == dataInBytes || Arrays
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, dataInBytes)) {
      return null;
    }
    try {
      if (actualDataType == DataTypes.BOOLEAN) {
        return ByteUtil.toBoolean(dataInBytes);
      } else if (actualDataType == DataTypes.BYTE) {
        return dataInBytes[0];
      } else if (actualDataType == DataTypes.SHORT) {
        // for non string type no dictionary column empty byte array is empty value
        // so no need to parse
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        return ByteUtil.toXorShort(dataInBytes, 0, dataInBytes.length);
      } else if (actualDataType == DataTypes.INT) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        return ByteUtil.toXorInt(dataInBytes, 0, dataInBytes.length);
      } else if (actualDataType == DataTypes.LONG) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        DataType blockDatatype;
        if (dataInBytes.length == DataTypes.INT.getSizeInBytes()) {
          blockDatatype = DataTypes.INT;
        } else {
          blockDatatype = DataTypes.LONG;
        }
        return getDataBasedOnRestructuredDataType(dataInBytes, blockDatatype, 0,
            dataInBytes.length);
      } else if (actualDataType == DataTypes.TIMESTAMP) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        if (isTimeStampConversion) {
          return ByteUtil.toXorLong(dataInBytes, 0, dataInBytes.length) * 1000L;
        } else {
          return ByteUtil.toXorLong(dataInBytes, 0, dataInBytes.length);
        }
      } else if (actualDataType == DataTypes.DOUBLE) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        return ByteUtil.toXorDouble(dataInBytes, 0, dataInBytes.length);
      } else if (actualDataType == DataTypes.FLOAT) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        return ByteUtil.toXorFloat(dataInBytes, 0, dataInBytes.length);
      } else if (DataTypes.isDecimal(actualDataType)) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        return getDataTypeConverter().convertFromBigDecimalToDecimal(byteToBigDecimal(dataInBytes));
      } else if (actualDataType == DataTypes.BINARY) {
        if (isEmptyByteArray(dataInBytes)) {
          return null;
        }
        return dataInBytes;
      } else {
        if (getBytesData) {
          return dataInBytes;
        }
        // Default action for String/Varchar
        return getDataTypeConverter().convertFromByteToUTF8String(dataInBytes);
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
   * Method to check if byte array is empty
   *
   * @param dataInBytes
   * @return
   */
  private static boolean isEmptyByteArray(byte[] dataInBytes) {
    return dataInBytes.length == 0;
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
          Date dateToStr = dateFormatter.get().parse(data5);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage(), e);
          return null;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        String data6 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data6.isEmpty()) {
          return null;
        }
        try {
          Date dateToStr = timestampFormatter.get().parse(data6);
          return dateToStr.getTime() * 1000;
        } catch (ParseException e) {
          LOGGER.error("Cannot convert value to Time/Long type value" + e.getMessage(), e);
          return null;
        }
      } else if (DataTypes.isDecimal(dataType)) {
        String data7 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data7.isEmpty()) {
          return null;
        }
        java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data7);
        if (dimension.getColumnSchema().getScale() > javaDecVal.scale()) {
          javaDecVal = javaDecVal.setScale(dimension.getColumnSchema().getScale());
        }
        return getDataTypeConverter().convertFromBigDecimalToDecimal(javaDecVal);
      } else if (dataType == DataTypes.BOOLEAN) {
        String data8 = new String(dataInBytes, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        if (data8.isEmpty()) {
          return null;
        }
        return BooleanConvert.parseBoolean(data8);
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
   * Below method will be used to basically to know whether any non parsable
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
      if (actualDataType == DataTypes.SHORT) {
        Short.parseShort(data);
      } else if (actualDataType == DataTypes.INT) {
        Integer.parseInt(data);
      } else if (actualDataType == DataTypes.LONG) {
        Long.parseLong(data);
      } else {
        return data;
      }
      return data;
    } catch (NumberFormatException ex) {
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
      if (DataTypes.isDecimal(dataType)) {
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

  private static String parseStringToBigDecimal(String value, CarbonColumn dimension) {
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
        parsedIntVal = Integer.parseInt(data);
        return String.valueOf(parsedIntVal)
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (dataType == DataTypes.SHORT) {
        parsedIntVal = Short.parseShort(data);
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
        if (columnSchema.getDataType() == DataTypes.DATE) {
          DirectDictionaryGenerator directDictionaryGenerator1 = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(columnSchema.getDataType());
          int value1 = directDictionaryGenerator1.generateDirectSurrogateKey(data);
          return String.valueOf(value1)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        } else {
          try {
            timestampFormatter.remove();
            Date dateToStr = timestampFormatter.get().parse(data);
            return ByteUtil.toXorBytes(dateToStr.getTime());
          } catch (ParseException e) {
            LOGGER.error(
                "Cannot convert value to Time/Long type value. Value is considered as null" + e
                    .getMessage());
            return null;
          }
        }
      } else if (DataTypes.isDecimal(dataType)) {
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
   *
   * @param converterLocal
   */
  public static void setDataTypeConverter(DataTypeConverter converterLocal) {
    if (converterLocal != null) {
      converter = converterLocal;
      timestampFormatter.remove();
      dateFormatter.remove();
    }
  }

  /**
   * As each load can have it's own time format. Reset the thread local for each load.
   */
  public static void clearFormatter() {
    timestampFormatter.remove();
    dateFormatter.remove();
  }

  public static DataTypeConverter getDataTypeConverter() {
    if (converter == null) {
      converter = new DataTypeConverterImpl();
    }
    return converter;
  }

  public static DataType valueOf(String name) {
    if (DataTypes.STRING.getName().equalsIgnoreCase(name)) {
      return DataTypes.STRING;
    } else if (DataTypes.DATE.getName().equalsIgnoreCase(name)) {
      return DataTypes.DATE;
    } else if (DataTypes.TIMESTAMP.getName().equalsIgnoreCase(name)) {
      return DataTypes.TIMESTAMP;
    } else if (DataTypes.BOOLEAN.getName().equalsIgnoreCase(name)) {
      return DataTypes.BOOLEAN;
    } else if (DataTypes.BYTE.getName().equalsIgnoreCase(name)) {
      return DataTypes.BYTE;
    } else if (DataTypes.SHORT.getName().equalsIgnoreCase(name)) {
      return DataTypes.SHORT;
    } else if (DataTypes.SHORT_INT.getName().equalsIgnoreCase(name)) {
      return DataTypes.SHORT_INT;
    } else if (DataTypes.INT.getName().equalsIgnoreCase(name)) {
      return DataTypes.INT;
    } else if (DataTypes.LONG.getName().equalsIgnoreCase(name)) {
      return DataTypes.LONG;
    } else if (DataTypes.FLOAT.getName().equalsIgnoreCase(name)) {
      return DataTypes.FLOAT;
    } else if (DataTypes.DOUBLE.getName().equalsIgnoreCase(name)) {
      return DataTypes.DOUBLE;
    } else if (DataTypes.VARCHAR.getName().equalsIgnoreCase(name)) {
      return DataTypes.VARCHAR;
    } else if (DataTypes.NULL.getName().equalsIgnoreCase(name)) {
      return DataTypes.NULL;
    } else if (DataTypes.BYTE_ARRAY.getName().equalsIgnoreCase(name)) {
      return DataTypes.BYTE_ARRAY;
    } else if (DataTypes.BINARY.getName().equalsIgnoreCase(name)) {
      return DataTypes.BINARY;
    } else if (name.equalsIgnoreCase("decimal")) {
      return DataTypes.createDefaultDecimalType();
    } else if (name.equalsIgnoreCase("array")) {
      return DataTypes.createDefaultArrayType();
    } else if (name.equalsIgnoreCase("struct")) {
      return DataTypes.createDefaultStructType();
    } else if (name.equalsIgnoreCase("map")) {
      return DataTypes.createDefaultMapType();
    } else {
      throw new RuntimeException("create DataType with invalid name: " + name);
    }
  }

  /**
   * @param dataType extracted from the json data
   * @return returns the datatype based on the input string from json to deserialize the tableInfo
   */
  public static DataType valueOf(DataType dataType, int precision, int scale) {
    if (DataTypes.isDecimal(dataType)) {
      return DataTypes.createDecimalType(precision, scale);
    } else {
      return valueOf(dataType.getName());
    }
  }

  /**
   * Method to type case the data based on modified data type. This method will used for
   * retrieving the data after change in data type restructure operation
   *
   * @param data
   * @param restructuredDataType
   * @param currentDataOffset
   * @param length
   * @return
   */
  public static long getDataBasedOnRestructuredDataType(byte[] data, DataType restructuredDataType,
      int currentDataOffset, int length) {
    long value = 0L;
    if (restructuredDataType == DataTypes.INT) {
      value = ByteUtil.toXorInt(data, currentDataOffset, length);
    } else if (restructuredDataType == DataTypes.LONG) {
      value = ByteUtil.toXorLong(data, currentDataOffset, length);
    }
    return value;
  }

  /**
   * Check if the column is a no dictionary primitive column
   *
   * @param dataType
   * @return
   */
  public static boolean isPrimitiveColumn(DataType dataType) {
    return dataType == DataTypes.BOOLEAN
        || dataType == DataTypes.BYTE
        || dataType == DataTypes.SHORT
        || dataType == DataTypes.INT
        || dataType == DataTypes.LONG
        || dataType == DataTypes.TIMESTAMP
        || DataTypes.isDecimal(dataType)
        || dataType == DataTypes.FLOAT
        || dataType == DataTypes.DOUBLE;
  }

  /**
   * utility function to check complex column child columns that can exceed 32000 length
   *
   * @param dataType
   * @return
   */
  public static boolean isByteArrayComplexChildColumn(DataType dataType) {
    return ((dataType == DataTypes.STRING) ||
        (dataType == DataTypes.VARCHAR) ||
        (dataType == DataTypes.BINARY) ||
        (dataType == DataTypes.DATE) ||
        DataTypes.isDecimal(dataType) ||
        (dataType == DataTypes.BYTE_ARRAY));
  }
}
