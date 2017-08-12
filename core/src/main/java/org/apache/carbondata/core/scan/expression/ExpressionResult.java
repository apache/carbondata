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

package org.apache.carbondata.core.scan.expression;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.util.CarbonUtil;

public class ExpressionResult implements Comparable<ExpressionResult> {

  protected DataType dataType;

  protected Object value;

  private List<ExpressionResult> expressionResults;

  private boolean isLiteral = false;

  public ExpressionResult(DataType dataType, Object value) {
    this.dataType = dataType;
    this.value = value;
  }

  public ExpressionResult(DataType dataType, Object value, boolean isLiteral) {
    this(dataType, value);
    this.isLiteral = isLiteral;
  }

  public ExpressionResult(List<ExpressionResult> expressionResults) {
    this.expressionResults = expressionResults;
  }

  public void set(DataType dataType, Object value) {
    this.dataType = dataType;
    this.value = value;
    this.expressionResults = null;
  }

  public DataType getDataType() {
    return dataType;
  }

  public Integer getInt() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Integer.parseInt(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterIllegalMemberException(e);
          }
        case SHORT:
          return ((Short) value).intValue();
        case INT:
        case DOUBLE:
          if (value instanceof Double) {
            return ((Double) value).intValue();
          }
          if (value instanceof Long) {
            return ((Long) value).intValue();
          }
          return (Integer) value;
        case DATE:
          if (value instanceof java.sql.Date) {
            return (int) (((java.sql.Date) value).getTime());
          } else {
            return (Integer) value;
          }
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return (int) (((Timestamp) value).getTime());
          } else {
            if (isLiteral) {
              Long l = (Long) value / 1000;
              return l.intValue();
            }
            return (Integer) value;
          }
        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to integer type value");
      }

    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to Integer type value");
    }
  }

  public Short getShort() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Short.parseShort(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterIllegalMemberException(e);
          }
        case SHORT:
        case INT:
        case DOUBLE:

          if (value instanceof Double) {
            return ((Double) value).shortValue();
          } else if (value instanceof Integer) {
            return ((Integer) value).shortValue();
          }
          return (Short) value;

        case DATE:

          if (value instanceof java.sql.Date) {
            return (short) (((java.sql.Date) value).getTime());
          } else {
            return (Short) value;
          }
        case TIMESTAMP:

          if (value instanceof Timestamp) {
            return (short) (((Timestamp) value).getTime());
          } else {
            if (isLiteral) {
              Long l = ((long) value / 1000);
              return l.shortValue();
            }
            return (Short) value;
          }

        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to integer type value");
      }

    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to Integer type value");
    }
  }

  public String getString() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case DATE:
        case TIMESTAMP:
          String format = CarbonUtil.getFormatFromProperty(this.getDataType());
          SimpleDateFormat parser = new SimpleDateFormat(format);
          if (this.getDataType() == DataType.DATE) {
            parser.setTimeZone(TimeZone.getTimeZone("GMT"));
          }
          if (value instanceof Timestamp) {
            return parser.format((Timestamp) value);
          } else if (value instanceof java.sql.Date) {
            return parser.format((java.sql.Date) value);
          } else if (value instanceof Long) {
            if (isLiteral) {
              return parser.format(new Timestamp((long) value / 1000));
            }
            return parser.format(new Timestamp((long) value));
          } else if (value instanceof Integer) {
            long date = ((int) value) * DateDirectDictionaryGenerator.MILLIS_PER_DAY;
            return parser.format(new java.sql.Date(date));
          }
          return value.toString();
        default:
          return value.toString();
      }
    } catch (Exception e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to String type value");
    }
  }

  public Double getDouble() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Double.parseDouble(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterIllegalMemberException(e);
          }
        case SHORT:
          return ((Short) value).doubleValue();
        case INT:
          return ((Integer) value).doubleValue();
        case LONG:
          return ((Long) value).doubleValue();
        case DOUBLE:
          return (Double) value;
        case DATE:
          if (value instanceof java.sql.Date) {
            return (double) ((java.sql.Date) value).getTime();
          } else {
            return (Double) (value);
          }
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return (double) ((Timestamp) value).getTime();
          } else {
            if (isLiteral) {
              Long l = (Long) value / 1000;
              return l.doubleValue();
            }
            return (Double) (value);
          }
        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to double type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to Double type value");
    }
  }

  public Long getLong() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Long.parseLong(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterIllegalMemberException(e);
          }
        case SHORT:
          return ((Short) value).longValue();
        case INT:
          return (Long) value;
        case LONG:
          return (Long) value;
        case DOUBLE:
          return (Long) value;
        case DATE:
          if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).getTime();
          } else {
            return (Long) value;
          }
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
          } else {
            return (Long) value;
          }
        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(e.getMessage() + " " +
          "Cannot convert" + this.getDataType().name() + " to Long type value");
    }

  }

  //Add to judge for BigDecimal
  public BigDecimal getDecimal() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return new BigDecimal(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterIllegalMemberException(e);
          }
        case SHORT:
          return new BigDecimal((short) value);
        case INT:
          return new BigDecimal((int) value);
        case LONG:
          return new BigDecimal((long) value);
        case DOUBLE:
        case DECIMAL:
          return new BigDecimal(value.toString());
        case DATE:
          if (value instanceof java.sql.Date) {
            return new BigDecimal(((java.sql.Date) value).getTime());
          } else {
            return new BigDecimal((long) value);
          }
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return new BigDecimal(((Timestamp) value).getTime());
          } else {
            if (isLiteral) {
              return new BigDecimal((long) value / 1000);
            }
            return new BigDecimal((long) value);
          }
        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to Long type value");
    }

  }

  public Long getTime() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          // Currently the query engine layer only supports yyyy-MM-dd HH:mm:ss date format
          // no matter in which format the data is been stored, so while retrieving the direct
          // surrogate value for filter member first it should be converted in date form as per
          // above format and needs to retrieve time stamp.
          SimpleDateFormat parser =
              new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
          Date dateToStr;
          try {
            dateToStr = parser.parse(value.toString());
            return dateToStr.getTime();
          } catch (ParseException e) {
            throw new FilterIllegalMemberException(
                "Cannot convert" + this.getDataType().name() + " to Time/Long type value");
          }
        case SHORT:
          return ((Short) value).longValue();
        case INT:
        case LONG:
          return (Long) value;
        case DOUBLE:
          return (Long) value;
        case DATE:
          if (value instanceof java.sql.Date) {
            return ((Date) value).getTime();
          } else {
            return (Long) value;
          }
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
          } else {
            if (isLiteral) {
              return (Long) value / 1000;
            }
            return (Long) value;
          }
        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to Time/Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to Time/Long type value");
    }

  }

  public Boolean getBoolean() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Boolean.parseBoolean(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterIllegalMemberException(e);
          }

        case BOOLEAN:
          return Boolean.parseBoolean(value.toString());

        default:
          throw new FilterIllegalMemberException(
              "Cannot convert" + this.getDataType().name() + " to boolean type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().name() + " to Boolean type value");
    }
  }

  public List<ExpressionResult> getList() {
    if (null == expressionResults) {
      List<ExpressionResult> a = new ArrayList<ExpressionResult>(20);
      a.add(new ExpressionResult(dataType, value, isLiteral));
      return a;
    } else {
      return expressionResults;
    }
  }

  public List<String> getListAsString() throws FilterIllegalMemberException {
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    List<ExpressionResult> evaluateResultList = getList();
    for (ExpressionResult result : evaluateResultList) {
      String resultString = result.getString();
      if (resultString == null) {
        evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        continue;
      }
      evaluateResultListFinal.add(resultString);
    }
    return evaluateResultListFinal;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    if (null != expressionResults) {
      result = prime * result + expressionResults.hashCode();
    } else if (null != value) {
      result = prime * result + value.toString().hashCode();
    } else {
      result = prime * result + "".hashCode();
    }

    return result;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof ExpressionResult)) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ExpressionResult objToCompare = (ExpressionResult) obj;
    boolean result = false;
    if (this.value == objToCompare.value) {
      return true;
    }

    if (this.isNull() || objToCompare.isNull()) {
      return false;
    }

    // make the comparison against the data type whose precedence is higher like
    // LONG precedence is higher than INT, so from int value we should get the long value
    // and then compare both the values. If done vice versa exception will be thrown
    // and comparison will fail
    DataType dataType = null;
    if (objToCompare.getDataType().getPrecedenceOrder() < this.getDataType().getPrecedenceOrder()) {
      dataType = this.getDataType();
    } else {
      dataType = objToCompare.getDataType();
    }
    try {
      switch (dataType) {
        case STRING:
          result = this.getString().equals(objToCompare.getString());
          break;
        case SHORT:
          result = this.getShort().equals(objToCompare.getShort());
          break;
        case INT:
          result = this.getInt().equals(objToCompare.getInt());
          break;
        case LONG:
        case DATE:
        case TIMESTAMP:
          result = this.getLong().equals(objToCompare.getLong());
          break;
        case DOUBLE:
          result = this.getDouble().equals(objToCompare.getDouble());
          break;
        case DECIMAL:
          result = this.getDecimal().equals(objToCompare.getDecimal());
          break;
        default:
          break;
      }
    } catch (FilterIllegalMemberException ex) {
      return false;
    }

    return result;
  }

  public boolean isNull() {
    return value == null;
  }

  @Override public int compareTo(ExpressionResult o) {
    try {
      switch (o.dataType) {
        case SHORT:
        case INT:
        case LONG:
        case DOUBLE:
          Double d1 = this.getDouble();
          Double d2 = o.getDouble();
          return d1.compareTo(d2);
        case DECIMAL:
          java.math.BigDecimal val1 = this.getDecimal();
          java.math.BigDecimal val2 = o.getDecimal();
          return val1.compareTo(val2);
        case DATE:
        case TIMESTAMP:
          String format = CarbonUtil.getFormatFromProperty(o.dataType);
          SimpleDateFormat parser = new SimpleDateFormat(format);
          Date date1 = parser.parse(this.getString());
          Date date2 = parser.parse(o.getString());
          return date1.compareTo(date2);
        case STRING:
        default:
          return this.getString().compareTo(o.getString());
      }
    } catch (ParseException e) {
      return -1;
    } catch (FilterIllegalMemberException e) {
      return -1;
    }
  }

}
