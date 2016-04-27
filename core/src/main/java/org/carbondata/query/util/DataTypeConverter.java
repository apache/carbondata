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

package org.carbondata.query.util;

//import java.sql.Timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;

import org.apache.spark.sql.columnar.TIMESTAMP;

public final class DataTypeConverter {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataTypeConverter.class.getName());

  private DataTypeConverter() {

  }

  public static Object getDataBasedOnDataType(String data, DataType dataType) {

    if (null == data) {
      return null;
    }
    try {
      switch (dataType) {
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
        case BOOLEAN:
          if (data.isEmpty()) {
            return null;
          }
          return Boolean.parseBoolean(data);
        case TIMESTAMP:
          if (data.isEmpty()) {
            return null;
          }
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr;
          try {
            dateToStr = parser.parse(data);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Cannot convert" + TIMESTAMP.toString() + " to Time/Long type value" + e
                    .getMessage());
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
      //            if(data.isEmpty())
      //            {
      //                return null;
      //            }
      //            else
      //            {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Problem while converting data type" + data);
      return null;
      //            }
    }

  }

  public static Object getDataBasedOnDataType(String data, SqlStatement.Type dataType) {

    if (null == data) {
      return null;
    }
    try {
      switch (dataType) {
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
        case BOOLEAN:
          if (data.isEmpty()) {
            return null;
          }
          return Boolean.parseBoolean(data);
        case TIMESTAMP:
          if (data.isEmpty()) {
            return null;
          }
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr;
          try {
            dateToStr = parser.parse(data);
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Cannot convert" + TIMESTAMP.toString() + " to Time/Long type value" + e
                    .getMessage());
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
      //            if(data.isEmpty())
      //            {
      //                return null;
      //            }
      //            else
      //            {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Problem while converting data type" + data);
      return null;
      //            }
    }

  }

  public static Object getMeasureDataBasedOnDataType(Object data, SqlStatement.Type dataType) {

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

}
