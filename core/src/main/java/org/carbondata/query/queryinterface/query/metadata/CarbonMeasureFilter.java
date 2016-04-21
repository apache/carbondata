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

package org.carbondata.query.queryinterface.query.metadata;

import java.io.Serializable;

/**
 * Measure filter
 */
public class CarbonMeasureFilter implements Serializable {
  private static final long serialVersionUID = -4253090536204072658L;

  /**
   * filterValue
   */
  private double filterValue;

  /**
   * filterType
   */
  private FilterType filterType;

  /**
   * afterTopN
   */
  private boolean afterTopN;

  /**
   * Constructor that takes filter information for measure filter.
   *
   * @param filterValue
   * @param filterType
   */
  public CarbonMeasureFilter(double filterValue, FilterType filterType) {
    this.filterValue = filterValue;
    this.filterType = filterType;
  }

  /**
   * Constructor that takes filter information for measure filter.
   *
   * @param filterType
   */
  public CarbonMeasureFilter(FilterType filterType) {
    this.filterType = filterType;
  }

  /**
   * get FilterValue
   *
   * @return the filterValue
   */
  public double getFilterValue() {
    return filterValue;
  }

  /**
   * FilterType
   *
   * @return the filterType
   */
  public FilterType getFilterType() {
    return filterType;
  }

  /**
   * @return the afterTopN
   */
  public boolean isAfterTopN() {
    return afterTopN;
  }

  /**
   * @param afterTopN the afterTopN to set
   */
  public void setAfterTopN(boolean afterTopN) {
    this.afterTopN = afterTopN;
  }

  public String toSQLConstruct(String levelName) {
    return levelName + filterType.symbol + filterValue;
  }

  /**
   * '
   * Enum for measure filter types.
   */
  public enum FilterType {
    /**
     * EQUAL_TO
     */
    EQUAL_TO(" = "),
    /**
     * NOT_EQUAL_TO
     */
    NOT_EQUAL_TO(" != "),
    /**
     * GREATER_THAN
     */
    GREATER_THAN(" > "),
    /**
     * LESS_THAN
     */
    LESS_THAN(" < "),
    /**
     * LESS_THAN_EQUAL
     */
    LESS_THAN_EQUAL(" <= "),
    /**
     * GREATER_THAN_EQUAL
     */
    GREATER_THAN_EQUAL(" >= "),

    /**
     * NOT_EMPTY
     */
    NOT_EMPTY(" IS NOT NULL ");

    String symbol;

    FilterType(String symbol) {
      this.symbol = symbol;
    }
  }
}
