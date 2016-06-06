/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License; Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing;
 * software distributed under the License is distributed on an
 * "AS IS" BASIS; WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND; either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.store;

import java.util.Map;

import org.carbondata.core.vo.ColumnGroupModel;
import org.carbondata.processing.datatypes.GenericDataType;

/**
 * This class contains all the data required for processing and writing the carbon data
 */
public class CarbonFactDataHandlerModel {

  /**
   * dbName
   */
  private String databaseName;
  /**
   * table name
   */
  private String tableName;
  /**
   * flag to check whether to group the similar data
   */
  private boolean isGroupByEnabled;
  /**
   * total count of measures in table
   */
  private int measureCount;
  /**
   * length of mdKey
   */
  private int mdKeyLength;
  /**
   * mdKey index in one row object
   */
  private int mdKeyIndex;
  /**
   * aggregators (e,g min, amx, sum)
   */
  private String[] aggregators;
  /**
   * custom aggregator class which contains the logic of merging data
   */
  private String[] aggregatorClass;
  /**
   * local store location
   */
  private String storeLocation;
  /**
   * cardinality of all dimensions
   */
  private int[] factDimLens;
  /**
   * flag to check whether to merge data based on custom aggregator
   */
  private boolean isMergingRequestForCustomAgg;
  /**
   * flag to check whether the request is for updating a member
   */
  private boolean isUpdateMemberRequest;
  /**
   * dimension cardinality
   */
  private int[] dimLens;
  /**
   * array of fact table columns
   */
  private String[] factLevels;
  /**
   * array of aggregate levels
   */
  private String[] aggLevels;
  /**
   * flag for data writing request
   */
  private boolean isDataWritingRequest;
  /**
   * count of columns for which dictionary is not generated
   */
  private int noDictionaryCount;
  /**
   * total number of columns in table
   */
  private int dimensionCount;
  /**
   * map which maintains indexing of complex columns
   */
  private Map<Integer, GenericDataType> complexIndexMap;
  /**
   * primitive dimensions cardinality
   */
  private int[] primitiveDimLens;
  /**
   * column group model
   */
  private ColumnGroupModel colGrpModel;
  /**
   * array in which each character represents an aggregation type and
   * the array length will be equal to the number of measures in table
   */
  private char[] aggType;
  /**
   * carbon data file attributes like task id, file stamp
   */
  private CarbonDataFileAttributes carbonDataFileAttributes;
  /**
   * carbon data directory path
   */
  private String carbonDataDirectoryPath;

  private int[] colCardinality;

  public int[] getColCardinality() {
    return colCardinality;
  }

  public void setColCardinality(int[] colCardinality) {
    this.colCardinality = colCardinality;
  }
  public CarbonDataFileAttributes getCarbonDataFileAttributes() {
    return carbonDataFileAttributes;
  }

  public void setCarbonDataFileAttributes(CarbonDataFileAttributes carbonDataFileAttributes) {
    this.carbonDataFileAttributes = carbonDataFileAttributes;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean isGroupByEnabled() {
    return isGroupByEnabled;
  }

  public int getMeasureCount() {
    return measureCount;
  }

  public void setMeasureCount(int measureCount) {
    this.measureCount = measureCount;
  }

  public int getMdKeyLength() {
    return mdKeyLength;
  }

  public void setMdKeyLength(int mdKeyLength) {
    this.mdKeyLength = mdKeyLength;
  }

  public int getMdKeyIndex() {
    return mdKeyIndex;
  }

  public void setMdKeyIndex(int mdKeyIndex) {
    this.mdKeyIndex = mdKeyIndex;
  }

  public String[] getAggregators() {
    return aggregators;
  }

  public void setAggregators(String[] aggregators) {
    this.aggregators = aggregators;
  }

  public String[] getAggregatorClass() {
    return aggregatorClass;
  }

  public void setAggregatorClass(String[] aggregatorClass) {
    this.aggregatorClass = aggregatorClass;
  }

  public String getStoreLocation() {
    return storeLocation;
  }

  public void setStoreLocation(String storeLocation) {
    this.storeLocation = storeLocation;
  }

  public int[] getFactDimLens() {
    return factDimLens;
  }

  public void setFactDimLens(int[] factDimLens) {
    this.factDimLens = factDimLens;
  }

  public boolean isMergingRequestForCustomAgg() {
    return isMergingRequestForCustomAgg;
  }

  public void setMergingRequestForCustomAgg(boolean mergingRequestForCustomAgg) {
    isMergingRequestForCustomAgg = mergingRequestForCustomAgg;
  }

  public boolean isUpdateMemberRequest() {
    return isUpdateMemberRequest;
  }

  public int[] getDimLens() {
    return dimLens;
  }

  public void setDimLens(int[] dimLens) {
    this.dimLens = dimLens;
  }

  public String[] getFactLevels() {
    return factLevels;
  }

  public void setFactLevels(String[] factLevels) {
    this.factLevels = factLevels;
  }

  public String[] getAggLevels() {
    return aggLevels;
  }

  public void setAggLevels(String[] aggLevels) {
    this.aggLevels = aggLevels;
  }

  public boolean isDataWritingRequest() {
    return isDataWritingRequest;
  }

  public void setDataWritingRequest(boolean dataWritingRequest) {
    isDataWritingRequest = dataWritingRequest;
  }

  public int getNoDictionaryCount() {
    return noDictionaryCount;
  }

  public void setNoDictionaryCount(int noDictionaryCount) {
    this.noDictionaryCount = noDictionaryCount;
  }

  public int getDimensionCount() {
    return dimensionCount;
  }

  public void setDimensionCount(int dimensionCount) {
    this.dimensionCount = dimensionCount;
  }

  public Map<Integer, GenericDataType> getComplexIndexMap() {
    return complexIndexMap;
  }

  public void setComplexIndexMap(Map<Integer, GenericDataType> complexIndexMap) {
    this.complexIndexMap = complexIndexMap;
  }

  public int[] getPrimitiveDimLens() {
    return primitiveDimLens;
  }

  public void setPrimitiveDimLens(int[] primitiveDimLens) {
    this.primitiveDimLens = primitiveDimLens;
  }

  public ColumnGroupModel getColGrpModel() {
    return colGrpModel;
  }

  public void setColGrpModel(ColumnGroupModel colGrpModel) {
    this.colGrpModel = colGrpModel;
  }

  public char[] getAggType() {
    return aggType;
  }

  public void setAggType(char[] aggType) {
    this.aggType = aggType;
  }

  public String getCarbonDataDirectoryPath() {
    return carbonDataDirectoryPath;
  }

  public void setCarbonDataDirectoryPath(String carbonDataDirectoryPath) {
    this.carbonDataDirectoryPath = carbonDataDirectoryPath;
  }
}

