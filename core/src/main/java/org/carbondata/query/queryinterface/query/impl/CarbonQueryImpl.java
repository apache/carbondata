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

package org.carbondata.query.queryinterface.query.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.query.queryinterface.query.CarbonQuery;
import org.carbondata.query.queryinterface.query.metadata.Axis;
import org.carbondata.query.queryinterface.query.metadata.CarbonDimensionLevel;
import org.carbondata.query.queryinterface.query.metadata.CarbonDimensionLevelFilter;
import org.carbondata.query.queryinterface.query.metadata.CarbonMeasure;
import org.carbondata.query.queryinterface.query.metadata.CarbonMeasureFilter;
import org.carbondata.query.queryinterface.query.metadata.TopCount;

/**
 * It is the implementation class for CarbonQuery interface.
 */
public class CarbonQueryImpl implements CarbonQuery {
  private static final long serialVersionUID = -1565369538375956018L;

  /**
   * Slice number
   */
  private static final int SLICE = 2;

  /**
   * axises
   */
  private Axis[] axises;

  /**
   * Top count list
   */
  private List<TopCount> topCounts = new ArrayList<TopCount>(10);

  /**
   * propertiesRequired
   */
  private boolean propertiesRequired;

  /**
   * When it set as true then user needs to provide the filters exactly with there parent members.
   */
  private boolean exactLevelsMatch;

  /**
   * breakHierarchyTopN
   */
  private boolean breakHierarchyTopN;

  /**
   * Time zone to covert the data.
   */
  private String timeZone;

  private Map<String, Object> extraProperties = new HashMap<String, Object>(16);

  /**
   * Default constructor
   */
  public CarbonQueryImpl() {
    axises = new Axis[3];
    for (int i = 0; i < axises.length; i++) {
      axises[i] = new Axis();
    }
  }

  /**
   * see interface comments.
   */
  @Override public void addDimensionLevel(CarbonDimensionLevel dimensionLevel,
      CarbonDimensionLevelFilter filter, SortType sortType, AxisType axis) {
    sortType = sortType == null ? SortType.NONE : sortType;
    axises[axis.getIndex()].add(dimensionLevel, sortType, null, filter);
  }

  /**
   * see interface comments.
   */
  @Override public void addMeasure(CarbonMeasure measure, List<CarbonMeasureFilter> filters,
      SortType sortType) {
    sortType = sortType == null ? SortType.NONE : sortType;
    axises[AxisType.COLUMN.getIndex()].add(measure, sortType, filters, null);

  }

  /**
   * see interface comments.
   */
  @Override public void addSlice(CarbonDimensionLevel dimensionLevel,
      CarbonDimensionLevelFilter filter) {
    axises[SLICE].add(dimensionLevel, null, null, filter);
  }

  /**
   * see interface comments.
   */
  @Override public void addSlice(CarbonMeasure measure, List<CarbonMeasureFilter> filters) {
    axises[SLICE].add(measure, null, filters, null);
  }

  /**
   * see interface comments.
   */
  @Override public void addTopCount(CarbonDimensionLevel dimensionLevel, CarbonMeasure measure,
      int count) {
    topCounts.add(new TopCount(dimensionLevel, measure, count, TopCount.TopNType.TOP));
  }

  /**
   * see interface comments.
   */
  @Override public void addBottomCount(CarbonDimensionLevel dimensionLevel, CarbonMeasure measure,
      int count) {
    topCounts.add(new TopCount(dimensionLevel, measure, count, TopCount.TopNType.BOTTOM));
  }

  /**
   * @return the axises
   */
  public Axis[] getAxises() {
    return axises;
  }

  /**
   * @return the topCounts
   */
  public List<TopCount> getTopCounts() {
    return topCounts;
  }

  /**
   * See interface comments
   */
  @Override public void showLevelProperties(boolean showProerties) {
    propertiesRequired = showProerties;
  }

  /**
   * Whether can show properties or not.
   *
   * @return
   */
  public boolean isShowLevelProperties() {
    return propertiesRequired;
  }

  /**
   * See interface comments
   */
  @Override public void setExactHirarchyLevelsMatch(boolean exactLevelsMatch) {
    this.exactLevelsMatch = exactLevelsMatch;
  }

  /**
   * @return the exactLevelsMatch
   */
  public boolean isExactLevelsMatch() {
    return exactLevelsMatch;
  }

  /**
   * @return the extraProperties
   */
  public Map<String, Object> getExtraProperties() {
    return extraProperties;
  }

  @Override public void setExtraProperties(Map<String, Object> extraProperties) {
    this.extraProperties = extraProperties;
  }

  /**
   * @return the breakHierarchyTopN
   */
  public boolean isBreakHierarchyTopN() {
    return breakHierarchyTopN;
  }

  /**
   * @param breakHierarchyTopN the breakHierarchyTopN to set
   */
  public void setBreakHierarchyTopN(boolean breakHierarchyTopN) {
    this.breakHierarchyTopN = breakHierarchyTopN;
  }

  /**
   * getTimeZone
   *
   * @return
   */
  public String getTimeZone() {
    return this.timeZone;
  }

  /**
   * setTimeZone
   */
  @Override public void setTimeZone(String timeZone) {
    this.timeZone = timeZone;
  }

}
