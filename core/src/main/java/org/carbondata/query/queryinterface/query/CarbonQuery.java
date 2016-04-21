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

package org.carbondata.query.queryinterface.query;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.carbondata.query.queryinterface.query.metadata.CarbonDimensionLevel;
import org.carbondata.query.queryinterface.query.metadata.CarbonDimensionLevelFilter;
import org.carbondata.query.queryinterface.query.metadata.CarbonMeasure;
import org.carbondata.query.queryinterface.query.metadata.CarbonMeasureFilter;

public interface CarbonQuery extends Serializable {
  /**
   * This property can be set to the extra properties Map
   * It takes boolean(true or false). it enables/disables pagination.
   */
  String PAGINATION_REQUIRED = "PAGINATION_REQUIRED";

  /**
   * This property can be set to the extra properties Map
   * It takes string in the following format 0-100 or 1000-1100.
   * This property can be taken only if PAGINATION_REQUIRED set to true;
   */
  String PAGE_RANGE = "PAGE_RANGE";

  /**
   * This property can be set to the extra properties Map
   * It takes unique string and also this property can be taken only if PAGINATION_REQUIRED
   * set to true;.
   */
  String QUERY_ID = "QUERY_ID";

  /**
   * Property can be set to save the result as a Data Set
   */
  String DATA_SET_PATH = "DATA_SET_PATH";

  /**
   * Property can be set to configure the transformations in the query
   */
  String TRANSFORMATIONS = "TRANSFORMATIONS";

  /**
   * Add dimension levels to the query
   *
   * @param dimensionLevel
   * @param axis
   */
  void addDimensionLevel(CarbonDimensionLevel dimensionLevel, CarbonDimensionLevelFilter filter,
      SortType sortType, AxisType axis);

  /**
   * Add measure to the query
   *
   * @param measure
   * @param axis
   */
  void addMeasure(CarbonMeasure measure, List<CarbonMeasureFilter> filters, SortType sortType);

  /**
   * Add level filter to the query. If this dimension level is already added to any axis,then no
   * need to add again.
   *
   * @param dimensionLevel
   */
  void addSlice(CarbonDimensionLevel dimensionLevel, CarbonDimensionLevelFilter filter);

  /**
   * Add level filter to the query. If this measure is already added to any axis,then no need
   * to add again.
   *
   * @param CarbonMeasure measure
   */
  void addSlice(CarbonMeasure measure, List<CarbonMeasureFilter> filters);

  /**
   * Add top count to the query
   *
   * @param dimensionLevel
   * @param measure
   * @param count
   */
  void addTopCount(CarbonDimensionLevel dimensionLevel, CarbonMeasure measure, int count);

  /**
   * Add bottom count to the query.
   *
   * @param dimensionLevel
   * @param measure
   * @param count
   */
  void addBottomCount(CarbonDimensionLevel dimensionLevel, CarbonMeasure measure, int count);

  /**
   * Whether to show dimension properties or not.
   *
   * @param showProperties
   */
  void showLevelProperties(boolean showProperties);

  /**
   * When it set as true then user needs to provide the filters exactly with there parent members.
   * For example : To apply day level filter as 1 then he has to provide like [2000].[jan].[1].
   * Now it exactly fetches the data for that day
   * If it is false then he can provide just like [1].
   * But this will fetch the data for the day 1 of all months
   * and years.
   *
   * @param exactLevelsMatch. By default it is false.
   */
  void setExactHirarchyLevelsMatch(boolean exactLevelsMatch);

  /**
   * This is properties will available to the execution.
   * This is only used from Analyzer client purpose.
   *
   * @param extraProperties
   */
  void setExtraProperties(Map<String, Object> extraProperties);

  /**
   * When this property sets, it converts the data to the target time zone.
   * By default there is no time zone set. When this property sets, even the time filters
   * passed through this interface will be converted from this timezone.
   *
   * For example : timezone sets UTC-12 and filters passed are Jan 1 23:00 then it
   * converts filter to Jan 2 11:00.
   *
   * @param timeZone
   */
  void setTimeZone(String timeZone);

  /**
   * Axis
   */
  public enum AxisType {
    /**
     * Row axis
     */
    ROW(0),
    /**
     * Column axis
     */
    COLUMN(1),

    /**
     * SLICE
     */
    SLICE(2);

    /**
     * index
     */
    private int index;

    /**
     * Get axis type
     *
     * @param index
     */
    private AxisType(int index) {
      this.index = index;
    }

    /**
     * Get axis index
     *
     * @return index
     */
    public int getIndex() {
      return index;
    }
  }

  /**
   * Sort type
   */
  public enum SortType {
    /**
     * Ascending order
     */
    ASC(0),
    /**
     * Descending order
     */
    DESC(1),
    /**
     * Ascending order
     */
    BASC(2),
    /**
     * Descending order
     */
    BDESC(3),
    /**
     * None
     */
    NONE(-1);

    private int sortVal;

    SortType(int sortVal) {
      this.sortVal = sortVal;
    }

    /**
     * getSortValue
     *
     * @return
     */
    public int getSortValue() {
      return sortVal;
    }
  }
}
