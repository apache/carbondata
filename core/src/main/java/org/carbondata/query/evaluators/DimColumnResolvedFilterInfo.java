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

package org.carbondata.query.evaluators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

public class DimColumnResolvedFilterInfo implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 3428115141211084114L;

  /**
   * column index in file
   */
  private int columnIndex = -1;

  /**
   * need compressed data from file
   */
  private boolean needCompressedData;

  /**
   * rowIndex
   */
  private int rowIndex = -1;

  private boolean isDimensionExistsInCurrentSilce = true;

  private int rsSurrogates;

  private String defaultValue;

  private Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex;

  private CarbonDimension dimension;

  /**
   * start index key of the block based on the keygenerator
   */
  private transient IndexKey starIndexKey;

  /**
   * end index key  which is been formed considering the max surrogate values
   * from dictionary cache
   */
  private transient IndexKey endIndexKey;

  /**
   * reolved filter object of a particlar filter Expression.
   */
  private DimColumnFilterInfo resolvedFilterValueObj;

  private Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionResolvedFilter;

  private Map<CarbonMeasure, List<DimColumnFilterInfo>> measureResolvedFilter;

  public DimColumnResolvedFilterInfo() {
    dimensionResolvedFilter = new HashMap<CarbonDimension, List<DimColumnFilterInfo>>(20);
    measureResolvedFilter = new HashMap<CarbonMeasure, List<DimColumnFilterInfo>>(20);
  }

  public IndexKey getStarIndexKey() {
    return starIndexKey;
  }

  public void setStarIndexKey(IndexKey starIndexKey) {
    this.starIndexKey = starIndexKey;
  }

  public IndexKey getEndIndexKey() {
    return endIndexKey;
  }

  public void setEndIndexKey(IndexKey endIndexKey) {
    this.endIndexKey = endIndexKey;
  }

  public void addDimensionResolvedFilterInstance(CarbonDimension dimension,
      DimColumnFilterInfo filterResolvedObj) {
    List<DimColumnFilterInfo> currentVals = dimensionResolvedFilter.get(dimension);
    if (null == currentVals) {
      currentVals = new ArrayList<DimColumnFilterInfo>(20);
      currentVals.add(filterResolvedObj);
      dimensionResolvedFilter.put(dimension, currentVals);
    } else {
      currentVals.add(filterResolvedObj);
    }
  }

  public Map<CarbonDimension, List<DimColumnFilterInfo>> getDimensionResolvedFilterInstance() {
    return dimensionResolvedFilter;
  }

  public Map<Integer, GenericQueryType> getComplexTypesWithBlockStartIndex() {
    return complexTypesWithBlockStartIndex;
  }

  public void setComplexTypesWithBlockStartIndex(
      Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex) {
    this.complexTypesWithBlockStartIndex = complexTypesWithBlockStartIndex;
  }

  public CarbonDimension getDimension() {
    return dimension;
  }

  public void setDimension(CarbonDimension dimension) {
    this.dimension = dimension;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

  public boolean isNeedCompressedData() {
    return needCompressedData;
  }

  public void setNeedCompressedData(boolean needCompressedData) {
    this.needCompressedData = needCompressedData;
  }

  public DimColumnFilterInfo getFilterValues() {
    return resolvedFilterValueObj;
  }

  public void setFilterValues(final DimColumnFilterInfo resolvedFilterValueObj) {
    this.resolvedFilterValueObj = resolvedFilterValueObj;
  }

  public int getRowIndex() {
    return rowIndex;
  }

  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }

  public boolean isDimensionExistsInCurrentSilce() {
    return isDimensionExistsInCurrentSilce;
  }

  public void setDimensionExistsInCurrentSilce(boolean isDimensionExistsInCurrentSilce) {
    this.isDimensionExistsInCurrentSilce = isDimensionExistsInCurrentSilce;
  }

  public int getRsSurrogates() {
    return rsSurrogates;
  }

  public void setRsSurrogates(int rsSurrogates) {
    this.rsSurrogates = rsSurrogates;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }
}
