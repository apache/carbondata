/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additiona   l information
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

package org.carbondata.scan.expression;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.scan.expression.exception.FilterUnsupportedException;

public class ExpressionResult implements Comparable<ExpressionResult> {

  private static final long serialVersionUID = 1L;
  protected DataType dataType;

  protected Object value;

  private List<ExpressionResult> expressionResults;

  public ExpressionResult(DataType dataType, Object value) {
    this.dataType = dataType;
    this.value = value;
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

  //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_009
  public Integer getInt() throws FilterUnsupportedException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Integer.parseInt(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterUnsupportedException(e);
          }

        case INT:
        case DOUBLE:

          if (value instanceof Double) {
            return ((Double) value).intValue();
          }
          return (Integer) value;

        case TIMESTAMP:

          if (value instanceof Timestamp) {
            return (int) (((Timestamp) value).getTime() % 1000);
          } else {
            return (Integer) value;
          }

        default:
          throw new FilterUnsupportedException(
              "Cannot convert" + this.getDataType().name() + " to integer type value");
      }

    } catch (ClassCastException e) {
      throw new FilterUnsupportedException(
          "Cannot convert" + this.getDataType().name() + " to Integer type value");
    }
  }

  public String getString() {
    if (value == null) {
      return null;
    }
    switch (this.getDataType()) {
      case TIMESTAMP:
        SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
        if (value instanceof Timestamp) {
          return parser.format((Timestamp) value);
        } else {
          return parser.format(new Timestamp((long) value / 1000));
        }

      default:
        return value.toString();
    }
  }

  public Double getDouble() throws FilterUnsupportedException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Double.parseDouble(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterUnsupportedException(e);
          }

        case INT:
          return ((Integer) value).doubleValue();
        case LONG:
          return ((Long) value).doubleValue();
        case DOUBLE:
          return (Double) value;
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return (double) ((Timestamp) value).getTime() * 1000;
          } else {
            return (Double) (value);
          }
        default:
          throw new FilterUnsupportedException(
              "Cannot convert" + this.getDataType().name() + " to double type value");
      }
    } catch (ClassCastException e) {
      throw new FilterUnsupportedException(
          "Cannot convert" + this.getDataType().name() + " to Double type value");
    }
  }
  //CHECKSTYLE:ON

  public Long getLong() throws FilterUnsupportedException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Long.parseLong(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterUnsupportedException(e);
          }

        case INT:
          return (Long) value;
        case LONG:
          return (Long) value;
        case DOUBLE:
          return (Long) value;
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return 1000 * ((Timestamp) value).getTime();
          } else {
            return (Long) value;
          }
        default:
          throw new FilterUnsupportedException(
              "Cannot convert" + this.getDataType().name() + " to Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterUnsupportedException(
          "Cannot convert" + this.getDataType().name() + " to Long type value");
    }

  }

  //Add to judge for BigDecimal
  public BigDecimal getDecimal() throws FilterUnsupportedException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return new BigDecimal(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterUnsupportedException(e);
          }

        case INT:
          return new BigDecimal((int) value);
        case LONG:
          return new BigDecimal((long) value);
        case DOUBLE:
          return new BigDecimal((double) value);
        case DECIMAL:
          return new BigDecimal(value.toString());
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return new BigDecimal(1000 * ((Timestamp) value).getTime());
          } else {
            return new BigDecimal((long) value);
          }
        default:
          throw new FilterUnsupportedException(
              "Cannot convert" + this.getDataType().name() + " to Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterUnsupportedException(
          "Cannot convert" + this.getDataType().name() + " to Long type value");
    }

  }

  public Long getTime() throws FilterUnsupportedException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr;
          try {
            dateToStr = parser.parse(value.toString());
            return dateToStr.getTime() * 1000;
          } catch (ParseException e) {
            throw new FilterUnsupportedException(
                "Cannot convert" + this.getDataType().name() + " to Time/Long type value");
          }
        case INT:
        case LONG:
          return (Long) value;
        case DOUBLE:
          return (Long) value;
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime() * 1000;
          } else {
            return (Long) value;
          }
        default:
          throw new FilterUnsupportedException(
              "Cannot convert" + this.getDataType().name() + " to Time/Long type value");
      }
    } catch (ClassCastException e) {
      throw new FilterUnsupportedException(
          "Cannot convert" + this.getDataType().name() + " to Time/Long type value");
    }

  }

  public Boolean getBoolean() throws FilterUnsupportedException {
    if (value == null) {
      return null;
    }
    try {
      switch (this.getDataType()) {
        case STRING:
          try {
            return Boolean.parseBoolean(value.toString());
          } catch (NumberFormatException e) {
            throw new FilterUnsupportedException(e);
          }

        case BOOLEAN:
          return Boolean.parseBoolean(value.toString());

        default:
          throw new FilterUnsupportedException(
              "Cannot convert" + this.getDataType().name() + " to boolean type value");
      }
    } catch (ClassCastException e) {
      throw new FilterUnsupportedException(
          "Cannot convert" + this.getDataType().name() + " to Boolean type value");
    }
  }

  public List<ExpressionResult> getList() {
    if (null == expressionResults) {
      List<ExpressionResult> a = new ArrayList<ExpressionResult>(20);
      a.add(new ExpressionResult(dataType, value));
      return a;
    } else {
      return expressionResults;
    }
  }

  public List<String> getListAsString() {
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    List<ExpressionResult> evaluateResultList = getList();
    for (ExpressionResult result : evaluateResultList) {
      if (result.getString() == null) {
        evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        continue;
      }
      evaluateResultListFinal.add(result.getString());
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
    try {
      switch (this.getDataType()) {
        case STRING:
          result = this.getString().equals(objToCompare.getString());
          break;
        case INT:
          result = this.getInt().equals(objToCompare.getInt());
          break;

        case DOUBLE:
          result = this.getDouble().equals(objToCompare.getDouble());
          break;
        case TIMESTAMP:
          result = this.getLong().equals(objToCompare.getLong());
          break;
        default:
          break;
      }
    } catch (FilterUnsupportedException ex) {
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
        case TIMESTAMP:
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date date1 = null;
          Date date2 = null;
          date1 = parser.parse(this.getString());
          date2 = parser.parse(o.getString());
          return date1.compareTo(date2);
        case STRING:
        default:
          return this.getString().compareTo(o.getString());
      }
    } catch (Exception e) {
      return -1;
    }
  }

}
