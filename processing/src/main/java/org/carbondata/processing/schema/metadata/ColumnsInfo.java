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

package org.carbondata.processing.schema.metadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.processing.datatypes.GenericDataType;

public class ColumnsInfo {

  /**
   * Indices for dimensions in the record. Doesn't include any properties.
   */
  private int[] dims;

  /**
   * Map<HierTableName, KeyGenerator>
   */
  private Map<String, KeyGenerator> keyGenerators;

  /**
   * Cube name
   */
  private String cubeName;

  /**
   * Hierarchy table names (Same will be file names for file store or
   * incremental load)
   */
  private Set<String> hierTables;

  /**
   * Batch size configured in transformation
   */
  private int batchSize;

  /**
   * To decide it is data load for aggregate table or not.
   */
  private boolean isAggregateLoad;

  /**
   * Store type DB or file based ?
   */
  private String storeType;

  /**
   * column Names for dimensions. Which will be used as table name for store
   */
  private String[] dimColNames;

  /**
   * baseStoreLocation
   */
  private String baseStoreLocation;

  /**
   * Maximum possible surrogate key for dimension possible based on
   * cardinality value in schema definition
   */
  private int[] maxKeys;

  /**
   * Dimension Index, Properties indices in the tuple.
   * [0] - [2,3] - 2 Props at indices 2 & 3 [1] - [4,7,8] - 3 props at indices
   * 4,7, & 8 [2] - [] - No props
   */
  private int[][] propIndx;

  /**
   * Dimension Index, Property column names from cube.
   * [0] - [col2,col3] [1] - [col4,col7,col8] [2] - []
   */
  private List<String>[] propColumns;

  /**
   * timDimIndex
   */
  private int timDimIndex;

  /**
   * timDimIndexEnd
   */
  private int timDimIndexEnd;

  /**
   * timeOrdinalIndices
   */
  private int[] timeOrdinalIndices;

  /**
   * timeOrdinalCols
   */
  private String[] timeOrdinalCols;

  /**
   * propTypes
   */
  private List<String>[] propTypes;

  /**
   * dimHierRel
   */
  private String[] dimHierRel;

  /**
   * tableName
   */
  private String tableName;

  /**
   * Primary key Map
   */
  private Map<String, Boolean> primaryKeyMap;

  /**
   * measureColumns
   */
  private String[] measureColumns;

  private boolean[] dimsPresent;

  private String schemaName;

  private Map<String, GenericDataType> complexTypesMap;

  /**
   * column Ids of dimensions in a table
   */
  private String[] dimensionColumnIds;

  /**
   * wrapper object having the columnSchemaDetails
   */
  private ColumnSchemaDetailsWrapper columnSchemaDetailsWrapper;

  private Map<String, Map<String, String>> columnProperties;

  public Map<String, GenericDataType> getComplexTypesMap() {
    return complexTypesMap;
  }

  public void setComplexTypesMap(Map<String, GenericDataType> complexTypesMap) {
    this.complexTypesMap = complexTypesMap;
  }

  /**
   * @return Returns the dims.
   */
  public int[] getDims() {
    return dims;
  }

  /**
   * @param dims The dims to set.
   */
  public void setDims(int[] dims) {
    this.dims = dims;
  }

  /**
   * @return Returns the keyGenerators.
   */
  public Map<String, KeyGenerator> getKeyGenerators() {
    return keyGenerators;
  }

  /**
   * @param keyGenerators The keyGenerators to set.
   */
  public void setKeyGenerators(Map<String, KeyGenerator> keyGenerators) {
    this.keyGenerators = keyGenerators;
  }

  /**
   * @return Returns the cubeName.
   */
  public String getCubeName() {
    return cubeName;
  }

  /**
   * @param cubeName The cubeName to set.
   */
  public void setCubeName(String cubeName) {
    this.cubeName = cubeName;
  }

  /**
   * @return Returns the hierTables.
   */
  public Set<String> getHierTables() {
    return hierTables;
  }

  /**
   * @param hierTables The hierTables to set.
   */
  public void setHierTables(Set<String> hierTables) {
    this.hierTables = hierTables;
  }

  /**
   * @return Returns the batchSize.
   */
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize The batchSize to set.
   */
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * @return Returns the isInitialLoad.
   */
  public boolean isAggregateLoad() {
    return isAggregateLoad;
  }

  public void setAggregateLoad(boolean isAggregateLoad) {
    this.isAggregateLoad = isAggregateLoad;
  }

  /**
   * @return Returns the storeType.
   */
  public String getStoreType() {
    return storeType;
  }

  /**
   * @param storeType The storeType to set.
   */
  public void setStoreType(String storeType) {
    this.storeType = storeType;
  }

  /**
   * @return Returns the dimColNames.
   */
  public String[] getDimColNames() {
    return dimColNames;
  }

  /**
   * @param dimColNames The dimColNames to set.
   */
  public void setDimColNames(String[] dimColNames) {
    this.dimColNames = dimColNames;
  }

  /**
   * @return Returns the maxKeys.
   */
  public int[] getMaxKeys() {
    return maxKeys;
  }

  /**
   * @param maxKeys The maxKeys to set.
   */
  public void setMaxKeys(int[] maxKeys) {
    this.maxKeys = maxKeys;
  }

  /**
   * @return Returns the propIndx.
   */
  public int[][] getPropIndx() {
    return propIndx;
  }

  /**
   * @param propIndx The propIndx to set.
   */
  public void setPropIndx(int[][] propIndx) {
    this.propIndx = propIndx;
  }

  /**
   * @return Returns the propColumns.
   */
  public List<String>[] getPropColumns() {
    return propColumns;
  }

  /**
   * @param propColumns The propColumns to set.
   */
  public void setPropColumns(List<String>[] propColumns) {
    this.propColumns = propColumns;
  }

  /**
   * @return Returns the timDimIndex.
   */
  public int getTimDimIndex() {
    return timDimIndex;
  }

  /**
   * @param timDimIndex The timDimIndex to set.
   */
  public void setTimDimIndex(int timDimIndex) {
    this.timDimIndex = timDimIndex;
  }

  /**
   * @return Returns the timDimIndexEnd.
   */
  public int getTimDimIndexEnd() {
    return timDimIndexEnd;
  }

  /**
   * @param timDimIndexEnd The timDimIndexEnd to set.
   */
  public void setTimDimIndexEnd(int timDimIndexEnd) {
    this.timDimIndexEnd = timDimIndexEnd;
  }

  /**
   * @return Returns the timeOrdinalIndices.
   */
  public int[] getTimeOrdinalIndices() {
    return timeOrdinalIndices;
  }

  /**
   * @param timeOrdinalIndices The timeOrdinalIndices to set.
   */
  public void setTimeOrdinalIndices(int[] timeOrdinalIndices) {
    this.timeOrdinalIndices = timeOrdinalIndices;
  }

  /**
   * @return Returns the timeOrdinalCols.
   */
  public String[] getTimeOrdinalCols() {
    return timeOrdinalCols;
  }

  /**
   * @param timeOrdinalCols The timeOrdinalCols to set.
   */
  public void setTimeOrdinalCols(String[] timeOrdinalCols) {
    this.timeOrdinalCols = timeOrdinalCols;
  }

  /**
   * @return Returns the propTypes.
   */
  public List<String>[] getPropTypes() {
    return propTypes;
  }

  /**
   * @param propTypes The propTypes to set.
   */
  public void setPropTypes(List<String>[] propTypes) {
    this.propTypes = propTypes;
  }

  /**
   * @return Returns the baseStoreLocation.
   */
  public String getBaseStoreLocation() {
    return baseStoreLocation;
  }

  /**
   * @param baseStoreLocation The baseStoreLocation to set.
   */
  public void setBaseStoreLocation(String baseStoreLocation) {
    this.baseStoreLocation = baseStoreLocation;
  }

  /**
   * @return Returns the dimHierRel.
   */
  public String[] getDimHierRel() {
    return dimHierRel;
  }

  /**
   * @param dimHierRel The dimHierRel to set.
   */
  public void setDimHierRel(String[] dimHierRel) {
    this.dimHierRel = dimHierRel;
  }

  /**
   * @return Returns the tableName.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return Returns the primaryKeyMap.
   */
  public Map<String, Boolean> getPrimaryKeyMap() {
    return primaryKeyMap;
  }

  /**
   * @param primaryKeyMap The primaryKeyMap to set.
   */
  public void setPrimaryKeyMap(Map<String, Boolean> primaryKeyMap) {
    this.primaryKeyMap = primaryKeyMap;
  }

  /**
   * getDimsPresent
   *
   * @return boolean[]
   */
  public boolean[] getDimsPresent() {
    return dimsPresent;
  }

  /**
   * setDimsPresent
   *
   * @param dimsPresent
   */
  public void setDimsPresent(boolean[] dimsPresent) {
    this.dimsPresent = dimsPresent;
  }

  /**
   * @return Returns the measureColumns.
   */
  public String[] getMeasureColumns() {
    return measureColumns;
  }

  /**
   * @param measureColumns The measureColumns to set.
   */
  public void setMeasureColumns(String[] measureColumns) {
    this.measureColumns = measureColumns;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * @return column Ids
   */
  public String[] getDimensionColumnIds() {
    return dimensionColumnIds;
  }

  /**
   * @param dimensionColumnIds column Ids for dimensions in a table
   */
  public void setDimensionColumnIds(String[] dimensionColumnIds) {
    this.dimensionColumnIds = dimensionColumnIds;
  }

  /**
   * returns wrapper object having the columnSchemaDetails
   *
   * @return
   */
  public ColumnSchemaDetailsWrapper getColumnSchemaDetailsWrapper() {
    return columnSchemaDetailsWrapper;
  }

  /**
   * set the wrapper object having the columnSchemaDetails
   *
   * @param columnSchemaDetailsWrapper
   */
  public void setColumnSchemaDetailsWrapper(ColumnSchemaDetailsWrapper columnSchemaDetailsWrapper) {
    this.columnSchemaDetailsWrapper = columnSchemaDetailsWrapper;
  }

  public void setColumnProperties(Map<String, Map<String, String>> columnProperties) {
    this.columnProperties = columnProperties;
  }

  public Map<String, String> getColumnProperties(String columnName) {
    return this.columnProperties.get(columnName);
  }
}
