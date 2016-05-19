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

package org.carbondata.spark.partition.reader;
/**
 * Copyright 2005 Bytecode Pty Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * helper class for processing JDBC ResultSet objects.
 */
public class ResultSetHelperService implements ResultSetHelper {
  public static final int CLOBBUFFERSIZE = 2048;

  // note: we want to maintain compatibility with Java 5 VM's
  // These types don't exist in Java 5
  static final int NVARCHAR = -9;
  static final int NCHAR = -15;
  static final int LONGNVARCHAR = -16;
  static final int NCLOB = 2011;

  static final String DEFAULT_DATE_FORMAT = "dd-MMM-yyyy";
  static final String DEFAULT_TIMESTAMP_FORMAT = "dd-MMM-yyyy HH:mm:ss";

  /**
   * Default Constructor.
   */
  public ResultSetHelperService() {
  }

  private static String read(Clob c) throws SQLException, IOException {
    StringBuilder sb = new StringBuilder((int) c.length());
    Reader r = c.getCharacterStream();
    try {
      char[] cbuf = new char[CLOBBUFFERSIZE];
      int n;
      while ((n = r.read(cbuf, 0, cbuf.length)) != -1) {
        sb.append(cbuf, 0, n);
      }
    } finally {
      r.close();
    }
    return sb.toString();

  }

  /**
   * Returns the column names from the result set.
   *
   * @param rs - ResultSet
   * @return - a string array containing the column names.
   * @throws SQLException - thrown by the result set.
   */
  public String[] getColumnNames(ResultSet rs) throws SQLException {
    List<String> names = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_010
    ResultSetMetaData metadata = rs.getMetaData();
    //CHECKSTYLE:ON
    for (int i = 0; i < metadata.getColumnCount(); i++) {
      names.add(metadata.getColumnName(i + 1));
    }

    String[] nameArray = new String[names.size()];
    return names.toArray(nameArray);
  }

  /**
   * Get all the column values from the result set.
   *
   * @param rs - the ResultSet containing the values.
   * @return - String array containing all the column values.
   * @throws SQLException - thrown by the result set.
   * @throws IOException  - thrown by the result set.
   */
  public String[] getColumnValues(ResultSet rs) throws SQLException, IOException {
    return this.getColumnValues(rs, false, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
  }

  /**
   * Get all the column values from the result set.
   *
   * @param rs   - the ResultSet containing the values.
   * @param trim - values should have white spaces trimmed.
   * @return - String array containing all the column values.
   * @throws SQLException - thrown by the result set.
   * @throws IOException  - thrown by the result set.
   */
  public String[] getColumnValues(ResultSet rs, boolean trim) throws SQLException, IOException {
    return this.getColumnValues(rs, trim, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
  }

  /**
   * Get all the column values from the result set.
   *
   * @param rs               - the ResultSet containing the values.
   * @param trim             - values should have white spaces trimmed.
   * @param dateFormatString - format String for dates.
   * @param timeFormatString - format String for timestamps.
   * @return - String array containing all the column values.
   * @throws SQLException - thrown by the result set.
   * @throws IOException  - thrown by the result set.
   */
  public String[] getColumnValues(ResultSet rs, boolean trim, String dateFormatString,
      String timeFormatString) throws SQLException, IOException {
    List<String> values = new ArrayList<>();
    ResultSetMetaData metadata = rs.getMetaData();

    for (int i = 0; i < metadata.getColumnCount(); i++) {
      values.add(getColumnValue(rs, metadata.getColumnType(i + 1), i + 1, trim, dateFormatString,
          timeFormatString));
    }

    String[] valueArray = new String[values.size()];
    return values.toArray(valueArray);
  }

  /**
   * changes an object to a String.
   *
   * @param obj - Object to format.
   * @return - String value of an object or empty string if the object is null.
   */
  protected String handleObject(Object obj) {
    return obj == null ? "" : String.valueOf(obj);
  }

  /**
   * changes a BigDecimal to String.
   *
   * @param decimal - BigDecimal to format
   * @return String representation of a BigDecimal or empty string if null
   */
  protected String handleBigDecimal(BigDecimal decimal) {
    return decimal == null ? "" : decimal.toString();
  }

  /**
   * Retrieves the string representation of an Long value from the result set.
   *
   * @param rs          - Result set containing the data.
   * @param columnIndex - index to the column of the long.
   * @return - the string representation of the long
   * @throws SQLException - thrown by the result set on error.
   */
  protected String handleLong(ResultSet rs, int columnIndex) throws SQLException {
    long lv = rs.getLong(columnIndex);
    return rs.wasNull() ? "" : Long.toString(lv);
  }

  /**
   * Retrieves the string representation of an Integer value from the result set.
   *
   * @param rs          - Result set containing the data.
   * @param columnIndex - index to the column of the integer.
   * @return - string representation of the Integer.
   * @throws SQLException - returned from the result set on error.
   */
  protected String handleInteger(ResultSet rs, int columnIndex) throws SQLException {
    int i = rs.getInt(columnIndex);
    return rs.wasNull() ? "" : Integer.toString(i);
  }

  /**
   * Retrieves a date from the result set.
   *
   * @param rs               - Result set containing the data
   * @param columnIndex      - index to the column of the date
   * @param dateFormatString - format for the date
   * @return - formatted date.
   * @throws SQLException - returned from the result set on error.
   */
  protected String handleDate(ResultSet rs, int columnIndex, String dateFormatString)
      throws SQLException {
    java.sql.Date date = rs.getDate(columnIndex);
    String value = null;
    if (date != null) {
      SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
      value = dateFormat.format(date);
    }
    return value;
  }

  /**
   * Return time read from ResultSet.
   *
   * @param time time read from ResultSet
   * @return String version of time or null if time is null.
   */
  protected String handleTime(Time time) {
    return time == null ? null : time.toString();
  }

  /**
   * The formatted timestamp.
   *
   * @param timestamp             - timestamp read from resultset
   * @param timestampFormatString - format string
   * @return - formatted time stamp.
   */
  protected String handleTimestamp(Timestamp timestamp, String timestampFormatString) {
    SimpleDateFormat timeFormat = new SimpleDateFormat(timestampFormatString);
    return timestamp == null ? null : timeFormat.format(timestamp);
  }

  private String getColumnValue(ResultSet rs, int colType, int colIndex, boolean trim,
      String dateFormatString, String timestampFormatString) throws SQLException, IOException {

    String value = "";

    switch (colType) {
      case Types.BIT:
      case Types.JAVA_OBJECT:
        value = handleObject(rs.getObject(colIndex));
        break;
      case Types.BOOLEAN:
        boolean b = rs.getBoolean(colIndex);
        value = Boolean.valueOf(b).toString();
        break;
      case NCLOB: // todo : use rs.getNClob
      case Types.CLOB:
        Clob c = rs.getClob(colIndex);
        if (c != null) {
          value = read(c);
        }
        break;
      case Types.BIGINT:
        value = handleLong(rs, colIndex);
        break;
      case Types.DECIMAL:
      case Types.DOUBLE:
      case Types.FLOAT:
      case Types.REAL:
      case Types.NUMERIC:
        value = handleBigDecimal(rs.getBigDecimal(colIndex));
        break;
      case Types.INTEGER:
      case Types.TINYINT:
      case Types.SMALLINT:
        value = handleInteger(rs, colIndex);
        break;
      case Types.DATE:
        value = handleDate(rs, colIndex, dateFormatString);
        break;
      case Types.TIME:
        value = handleTime(rs.getTime(colIndex));
        break;
      case Types.TIMESTAMP:
        value = handleTimestamp(rs.getTimestamp(colIndex), timestampFormatString);
        break;
      case NVARCHAR: // todo : use rs.getNString
      case NCHAR: // todo : use rs.getNString
      case LONGNVARCHAR: // todo : use rs.getNString
      case Types.LONGVARCHAR:
      case Types.VARCHAR:
      case Types.CHAR:
        value = getColumnValue(rs, colIndex, trim);
        break;
      default:
        value = "";
    }

    if (value == null) {
      value = "";
    }

    return value;
  }

  /**
   * @param rs
   * @param colIndex
   * @param trim
   * @return
   * @throws SQLException
   */
  public String getColumnValue(ResultSet rs, int colIndex, boolean trim) throws SQLException {
    String value;
    String columnValue = rs.getString(colIndex);
    if (trim && columnValue != null) {
      value = columnValue.trim();
    } else {
      value = columnValue;
    }
    return value;
  }
}
