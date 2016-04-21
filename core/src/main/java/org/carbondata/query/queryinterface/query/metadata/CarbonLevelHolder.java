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
import java.util.List;

import org.carbondata.query.queryinterface.query.CarbonQuery;

/**
 * It is holder class for a level
 */
public class CarbonLevelHolder implements Serializable {
  private static final long serialVersionUID = -6328136034161360231L;

  /**
   * Level
   */
  private CarbonLevel level;

  /**
   * sortType
   */
  private CarbonQuery.SortType sortType;

  /**
   * msrFilters
   */
  private List<CarbonMeasureFilter> msrFilters;

  /**
   * dimLevelFilter
   */
  private CarbonDimensionLevelFilter dimLevelFilter;

  /**
   * Constructor
   *
   * @param level
   * @param sortType
   */
  public CarbonLevelHolder(CarbonLevel level, CarbonQuery.SortType sortType) {
    super();
    this.level = level;
    this.sortType = sortType;
  }

  /**
   * @return the level
   */
  public CarbonLevel getLevel() {
    return level;
  }

  /**
   * @return the sortType
   */
  public CarbonQuery.SortType getSortType() {
    return sortType;
  }

  /**
   * @param sortType the sortType to set
   */
  public void setSortType(CarbonQuery.SortType sortType) {
    this.sortType = sortType;
  }

  /**
   * @return the msrFilter
   */
  public List<CarbonMeasureFilter> getMsrFilters() {
    return msrFilters;
  }

  /**
   * @param msrFilter the msrFilter to set
   */
  public void setMsrFilters(List<CarbonMeasureFilter> msrFilters) {
    this.msrFilters = msrFilters;
  }

  /**
   * @return the dimLevelFilter
   */
  public CarbonDimensionLevelFilter getDimLevelFilter() {
    return dimLevelFilter;
  }

  /**
   * @param dimLevelFilter the dimLevelFilter to set
   */
  public void setDimLevelFilter(CarbonDimensionLevelFilter dimLevelFilter) {
    this.dimLevelFilter = dimLevelFilter;
  }
}