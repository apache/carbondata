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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
        try {
          return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
          throw new FilterIllegalMemberException(e);
        }
      } else if (dataType == DataTypes.SHORT) {
        return ((Short) value).intValue();
      } else if (dataType == DataTypes.INT ||
          dataType == DataTypes.DOUBLE) {
        if (value instanceof Double) {
          return ((Double) value).intValue();
        }
        if (value instanceof Long) {
          return ((Long) value).intValue();
        }
        return (Integer) value;
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return (int) (((java.sql.Date) value).getTime());
        } else {
          return (Integer) value;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Timestamp) {
          return (int) (((Timestamp) value).getTime());
        } else {
          if (isLiteral) {
            Long l = (Long) value / 1000;
            return l.intValue();
          }
          return (Integer) value;
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to integer type value");
      }

    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to Integer type value");
    }
  }

  public Short getShort() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
        try {
          return Short.parseShort(value.toString());
        } catch (NumberFormatException e) {
          throw new FilterIllegalMemberException(e);
        }
      } else if (dataType == DataTypes.SHORT ||
          dataType == DataTypes.INT ||
          dataType == DataTypes.DOUBLE) {
        if (value instanceof Double) {
          return ((Double) value).shortValue();
        } else if (value instanceof Integer) {
          return ((Integer) value).shortValue();
        }
        return (Short) value;
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return (short) (((java.sql.Date) value).getTime());
        } else {
          return (Short) value;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Timestamp) {
          return (short) (((Timestamp) value).getTime());
        } else {
          if (isLiteral) {
            Long l = ((long) value / 1000);
            return l.shortValue();
          }
          return (Short) value;
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to integer type value");
      }

    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to Integer type value");
    }
  }

  public String getString() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
        return CarbonUtil
            .getFormattedDateOrTimestamp(CarbonUtil.getFormatFromProperty(dataType), dataType,
                value, isLiteral);
      } else {
        return value.toString();
      }
    } catch (Exception e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to String type value");
    }
  }

  public Double getDouble() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
        try {
          return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
          throw new FilterIllegalMemberException(e);
        }
      } else if (dataType == DataTypes.SHORT) {
        return ((Short) value).doubleValue();
      } else if (dataType == DataTypes.INT) {
        return ((Integer) value).doubleValue();
      } else if (dataType == DataTypes.LONG) {
        return ((Long) value).doubleValue();
      } else if (dataType == DataTypes.DOUBLE) {
        return (Double) value;
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return (double) ((java.sql.Date) value).getTime();
        } else {
          return (Double) (value);
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Timestamp) {
          return (double) ((Timestamp) value).getTime();
        } else {
          if (isLiteral) {
            Long l = (Long) value / 1000;
            return l.doubleValue();
          }
          return (Double) (value);
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to double type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to Double type value");
    }
  }

  public Long getLong() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
        try {
          return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
          throw new FilterIllegalMemberException(e);
        }
      } else if (dataType == DataTypes.SHORT) {
        return ((Short) value).longValue();
      } else if (dataType == DataTypes.INT) {
        return (Long) value;
      } else if (dataType == DataTypes.LONG) {
        return (Long) value;
      } else if (dataType == DataTypes.DOUBLE) {
        return (Long) value;
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return ((java.sql.Date) value).getTime();
        } else {
          return (Long) value;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Timestamp) {
          return ((Timestamp) value).getTime();
        } else {
          return (Long) value;
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(e.getMessage() + " " +
          "Cannot convert" + this.getDataType().getName() + " to Long type value");
    }

  }

  //Add to judge for BigDecimal
  public BigDecimal getDecimal() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
        try {
          return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
          throw new FilterIllegalMemberException(e);
        }
      } else if (dataType == DataTypes.SHORT) {
        return new BigDecimal((short) value);
      } else if (dataType == DataTypes.INT) {
        return new BigDecimal((int) value);
      } else if (dataType == DataTypes.LONG) {
        return new BigDecimal((long) value);
      } else if (dataType == DataTypes.DOUBLE || DataTypes.isDecimal(dataType)) {
        return new BigDecimal(value.toString());
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return new BigDecimal(((java.sql.Date) value).getTime());
        } else {
          return new BigDecimal((long) value);
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Timestamp) {
          return new BigDecimal(((Timestamp) value).getTime());
        } else {
          if (isLiteral) {
            return new BigDecimal((long) value / 1000);
          }
          return new BigDecimal((long) value);
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to Decimal type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to Decimal type value");
    }

  }

  public Long getTime() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
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
              "Cannot convert" + this.getDataType().getName() + " to Time type value");
        }
      } else if (dataType == DataTypes.SHORT) {
        return ((Short) value).longValue();
      } else if (dataType == DataTypes.INT || dataType == DataTypes.LONG) {
        return (Long) value;
      } else if (dataType == DataTypes.DOUBLE) {
        return (Long) value;
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return ((Date) value).getTime();
        } else {
          return (Long) value;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Timestamp) {
          return ((Timestamp) value).getTime();
        } else {
          if (isLiteral) {
            return (Long) value / 1000;
          }
          return (Long) value;
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to Time type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to Time type value");
    }

  }

  public Long getTimeAsMillisecond() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
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
              "Cannot convert" + this.getDataType().getName() + " to Time type value");
        }
      } else if (dataType == DataTypes.SHORT) {
        return ((Short) value).longValue();
      } else if (dataType == DataTypes.INT || dataType == DataTypes.LONG) {
        return (Long) value;
      } else if (dataType == DataTypes.DOUBLE) {
        return (Long) value;
      } else if (dataType == DataTypes.DATE) {
        if (value instanceof java.sql.Date) {
          return ((Date) value).getTime();
        } else {
          return (Long) value;
        }
      } else if (dataType == DataTypes.TIMESTAMP) {
        if (value instanceof Date) {
          return ((Date) value).getTime();
        } else if (value instanceof Timestamp) {
          return ((Timestamp) value).getTime();
        } else {
          return (Long) value;
        }
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert " + this.getDataType().getName() + " to Time type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert " + this.getDataType().getName() + " to Time type value");
    }
  }

  public Boolean getBoolean() throws FilterIllegalMemberException {
    if (value == null) {
      return null;
    }
    try {
      DataType dataType = this.getDataType();
      if (dataType == DataTypes.STRING) {
        try {
          return Boolean.parseBoolean(value.toString());
        } catch (NumberFormatException e) {
          throw new FilterIllegalMemberException(e);
        }
      } else if (dataType == DataTypes.BOOLEAN) {
        return Boolean.parseBoolean(value.toString());
      } else {
        throw new FilterIllegalMemberException(
            "Cannot convert" + this.getDataType().getName() + " to boolean type value");
      }
    } catch (ClassCastException e) {
      throw new FilterIllegalMemberException(
          "Cannot convert" + this.getDataType().getName() + " to Boolean type value");
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

  @Override
  public int hashCode() {
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

  @Override
  public boolean equals(Object obj) {
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
      if (dataType == DataTypes.STRING) {
        result = this.getString().equals(objToCompare.getString());
      } else if (dataType == DataTypes.SHORT) {
        result = this.getShort().equals(objToCompare.getShort());
      } else if (dataType == DataTypes.INT) {
        result = this.getInt().equals(objToCompare.getInt());
      } else if (dataType == DataTypes.LONG ||
          dataType == DataTypes.DATE ||
          dataType == DataTypes.TIMESTAMP) {
        result = this.getLong().equals(objToCompare.getLong());
      } else if (dataType == DataTypes.DOUBLE) {
        result = this.getDouble().equals(objToCompare.getDouble());
      } else if (DataTypes.isDecimal(dataType)) {
        result = this.getDecimal().equals(objToCompare.getDecimal());
      }
    } catch (FilterIllegalMemberException ex) {
      return false;
    }

    return result;
  }

  public boolean isNull() {
    return value == null;
  }

  @Override
  public int compareTo(ExpressionResult o) {
    try {
      DataType type = o.dataType;
      if (type == DataTypes.SHORT ||
          type == DataTypes.INT ||
          type == DataTypes.LONG ||
          type == DataTypes.DOUBLE) {
        Double d1 = this.getDouble();
        Double d2 = o.getDouble();
        return d1.compareTo(d2);
      } else if (DataTypes.isDecimal(type)) {
        java.math.BigDecimal val1 = this.getDecimal();
        java.math.BigDecimal val2 = o.getDecimal();
        return val1.compareTo(val2);
      } else if (type == DataTypes.DATE || type == DataTypes.TIMESTAMP) {
        String format = CarbonUtil.getFormatFromProperty(o.dataType);
        SimpleDateFormat parser = new SimpleDateFormat(format);
        Date date1 = parser.parse(this.getString());
        Date date2 = parser.parse(o.getString());
        return date1.compareTo(date2);
      } else {
        return this.getString().compareTo(o.getString());
      }
    } catch (ParseException | FilterIllegalMemberException e) {
      return -1;
    }
  }

}
