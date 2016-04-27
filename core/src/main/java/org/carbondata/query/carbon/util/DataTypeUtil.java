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
package org.carbondata.query.carbon.util;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Utility for data type
 */
public class DataTypeUtil {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataTypeUtil.class.getName());

  /**
   * Below method will be used to convert the data passed to its actual data
   * type
   *
   * @param data           data
   * @param actualDataType actual data type
   * @return actual data after conversion
   */
  public static Object getDataBasedOnDataType(String data, DataType actualDataType) {

    if (null == data) {
      return null;
    }
    try {
      switch (actualDataType) {
        case INT:
          if (data.isEmpty()) {
            return null;
          }
          return Integer.parseInt(data);
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
        case TIMESTAMP:
          if (data.isEmpty()) {
            return null;
          }
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr = null;
          try {
            dateToStr = parser.parse(data);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Cannot convert" + data.toString() + " to Time/Long type value" + e.getMessage());
            return null;
          }
        case DECIMAL:
          if (data.isEmpty()) {
            return null;
          }
          java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data);
          scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
          org.apache.spark.sql.types.Decimal decConverter =
              new org.apache.spark.sql.types.Decimal();
          return decConverter.set(scalaDecVal);
        default:
          return data;
      }
    } catch (NumberFormatException ex) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Problem while converting data type" + data);
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

          return (Double) data;
        case LONG:

          return (Long) data;

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
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Problem while converting data type" + data);
      return null;
    }

  }

  public static int compareBasedOnDatatYpe(Object data1, Object data2, DataType dataType) {
    switch (dataType) {
      case INT:
        return ((Integer) data1).compareTo((Integer) data2);
      case LONG:
      case TIMESTAMP:
        return ((Long) data1).compareTo((Long) data2);
      case DOUBLE:
        return ((Double) data1).compareTo((Double) data2);
      case DECIMAL:
        return ((BigDecimal) data1).compareTo((BigDecimal) data2);
      default:
        return ((String) data1).compareTo((String) data2);
    }
  }

  /**
   * below method is to check whether data type is of numeric type or not
   *
   * @param dataType data type
   * @return true if numeric data type
   */
  public boolean isNumericDatatype(DataType dataType) {
    switch (dataType) {
      case INT:
      case LONG:
      case DOUBLE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

}
