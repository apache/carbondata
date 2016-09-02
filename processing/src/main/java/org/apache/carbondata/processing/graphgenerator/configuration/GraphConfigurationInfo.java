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

package org.apache.carbondata.processing.graphgenerator.configuration;

import java.util.Map;

import org.apache.carbondata.processing.schema.metadata.ColumnSchemaDetailsWrapper;
import org.apache.carbondata.processing.schema.metadata.TableOptionWrapper;

public class GraphConfigurationInfo {
  private String connectionName;

  private String dbType;

  private String numberOfCores;

  private String storeLocation;

  private String tableName;

  private String blockletSize;

  private String maxBlockletInFile;

  private String batchSize;

  private Map<String, String> dimCardinalities;

  private String[] dimensions;

  private String noDictionaryDims;

  private String[] measures;

  private String dimensionString;

  private String hiersString;

  private String measuresString;

  private String propertiesString;

  private String timeHeirString;

  private String metaHeirString;

  private String metaHeirQueryString;

  private String jndiName;

  private Map<String, String> tableMeasuresAndDataTypeMap;

  private String tableInputSqlQuery;

  private String dimensionSqlQuery;

  private String sortSize;

  private boolean isAGG;

  private String driverclass;

  private String username;

  private String password;

  private String connectionUrl;

  private String[] actualDims;

  /**
   * Sets the dimension:hirearchy#levelnames1,levelName2
   */
  private String dimensionTableNames;

  /**
   * column Ids concatenated by a delimeter
   */
  private String dimensionColumnIds;

  /**
   * Agg type
   */
  private String[] aggType;

  /**
   * mdkeySize
   */
  private String mdkeySize;

  /**
   * complexTypeString
   */
  private String complexTypeString;

  /**
   * measureCount
   */

  private String measureCount;

  /**
   * heirAndKeySizeString
   */
  private String heirAndKeySizeString;

  /**
   * hier and containing columns string
   */
  private String hierColumnString;

  /**
   * forignKey
   */
  private String[] forignKey;

  /**
   * Foreign Key and respective hierarchy
   */
  private String foreignKeyHierarchyString;

  /**
   * Primary key String
   */
  private String primaryKeyString;

  /**
   * Measure Names
   */
  private String measureNamesString;

  /**
   * Measure Names
   */
  private String measureUniqueColumnNamesString;

  /**
   * actualDimensionColumns
   */
  private String actualDimensionColumns;

  /**
   * normHiers
   */
  private String normHiers;

  private String forgienKeyAndPrimaryKeyMapString;

  /**
   * heirAndDimLens
   */
  private String heirAndDimLens;

  /**
   * measureTypeInfo
   */
  private String measureDataTypeInfo;

  /**
   * columnAndTableName_ColumnMapForAgg
   */
  private String columnAndTableNameColumnMapForAgg;
  /**
   * denormColumns
   */
  private String denormColumns;
  private String[] aggClass;
  /**
   * type
   */
  private char[] type;
  private String levelAnddataType;

  private Boolean[] isNoDictionaryDimMapping;

  private Boolean[] isUseInvertedIndex;

  private String columnPropertiesString;

  /**
   * wrapper object holding the columnschemadetails
   */
  private ColumnSchemaDetailsWrapper columnSchemaDetailsWrapper;

  /**
   * wrapper object holding the table options details needed while dataload
   */
  private TableOptionWrapper tableOptionWrapper;

  /**
   * It is column groups in below format
   * 0,1~2~3,4,5,6~7~8,9
   * groups are
   * ,-> all ordinal with different group id
   * ~-> all ordinal with same group id
   */
  private String columnGroupsString;
  private String columnsDataTypeString;
  /**
   * @return isUseInvertedIndex
   */
  public Boolean[] getIsUseInvertedIndex() {
    return isUseInvertedIndex;
  }

  /**
   * @param isUseInvertedIndex the bool array whether use inverted index to set
   */
  public void setIsUseInvertedIndex(Boolean[] isUseInvertedIndex) {
    this.isUseInvertedIndex = isUseInvertedIndex;
  }

  /**
   * @return the connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName the connectionName to set
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * @return the dbType
   */
  public String getDbType() {
    return dbType;
  }

  /**
   * @param dbType the dbType to set
   */
  public void setDbType(String dbType) {
    this.dbType = dbType;
  }

  /**
   * @return the numberOfCores
   */
  public String getNumberOfCores() {
    return numberOfCores;
  }

  /**
   * @param numberOfCores the numberOfCores to set
   */
  public void setNumberOfCores(String numberOfCores) {
    this.numberOfCores = numberOfCores;
  }

  /**
   * @return the storeLocation
   */
  public String getStoreLocation() {
    return storeLocation;
  }

  /**
   * @param storeLocation the storeLocation to set
   */
  public void setStoreLocation(String storeLocation) {
    this.storeLocation = storeLocation;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getComplexTypeString() {
    return complexTypeString;
  }

  public void setComplexTypeString(String complexTypeString) {
    this.complexTypeString = complexTypeString;
  }

  /**
   * @return the blockletSize
   */
  public String getBlockletSize() {
    return blockletSize;
  }

  /**
   * @param blockletSize the blockletSize to set
   */
  public void setBlockletSize(String blockletSize) {
    this.blockletSize = blockletSize;
  }

  /**
   * @return the maxBlockletInFile
   */
  public String getMaxBlockletInFile() {
    return maxBlockletInFile;
  }

  /**
   * @param maxBlockletInFile the maxBlockletInFile to set
   */
  public void setMaxBlockletInFile(String maxBlockletInFile) {
    this.maxBlockletInFile = maxBlockletInFile;
  }

  /**
   * @return the batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize the batchSize to set
   */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * @return the dimCardinalities
   */
  public Map<String, String> getDimCardinalities() {
    return dimCardinalities;
  }

  /**
   * @param dimCardinalities the dimCardinalities to set
   */
  public void setDimCardinalities(Map<String, String> dimCardinalities) {
    this.dimCardinalities = dimCardinalities;
  }

  /**
   * @return the dimensions
   */
  public String[] getDimensions() {
    return dimensions;
  }

  /**
   * @param dimensions the dimensions to set
   */
  public void setDimensions(String[] dimensions) {
    this.dimensions = dimensions;
  }

  /**
   * @return the measures
   */
  public String[] getMeasures() {
    return measures;
  }

  /**
   * @param measures the measures to set
   */
  public void setMeasures(String[] measures) {
    this.measures = measures;
  }

  /**
   * @return the dimensionString
   */
  public String getDimensionString() {
    return dimensionString;
  }

  /**
   * @param dimensionString the dimensionString to set
   */
  public void setDimensionString(String dimensionString) {
    this.dimensionString = dimensionString;
  }

  /**
   * getNormHiers
   *
   * @return String
   */
  public String getNormHiers() {
    return normHiers;
  }

  /**
   * setNormHiers
   *
   * @param normHiers void
   */
  public void setNormHiers(String normHiers) {
    this.normHiers = normHiers;
  }

  /**
   * @return the hiersString
   */
  public String getHiersString() {
    return hiersString;
  }

  /**
   * @param hiersString the hiersString to set
   */
  public void setHiersString(String hiersString) {
    this.hiersString = hiersString;
  }

  /**
   * @return the measuresString
   */
  public String getMeasuresString() {
    return measuresString;
  }

  /**
   * @param measuresString the measuresString to set
   */
  public void setMeasuresString(String measuresString) {
    this.measuresString = measuresString;
  }

  /**
   * @return the propertiesString
   */
  public String getPropertiesString() {
    return propertiesString;
  }

  /**
   * @param propertiesString the propertiesString to set
   */
  public void setPropertiesString(String propertiesString) {
    this.propertiesString = propertiesString;
  }

  /**
   * @return the timeHeirString
   */
  public String getTimeHeirString() {
    return timeHeirString;
  }

  /**
   * @param timeHeirString the timeHeirString to set
   */
  public void setTimeHeirString(String timeHeirString) {
    this.timeHeirString = timeHeirString;
  }

  /**
   * @return the metaHeirString
   */
  public String getMetaHeirString() {
    return metaHeirString;
  }

  /**
   * @param metaHeirString the metaHeirString to set
   */
  public void setMetaHeirString(String metaHeirString) {
    this.metaHeirString = metaHeirString;
  }

  /**
   * @return the metaHeirQueryString
   */
  public String getMetaHeirQueryString() {
    return metaHeirQueryString;
  }

  /**
   * @param metaHeirQueryString the metaHeirQueryString to set
   */
  public void setMetaHeirQueryString(String metaHeirQueryString) {
    this.metaHeirQueryString = metaHeirQueryString;
  }

  /**
   * @return the jndiName
   */
  public String getJndiName() {
    return jndiName;
  }

  /**
   * @param jndiName the jndiName to set
   */
  public void setJndiName(String jndiName) {
    this.jndiName = jndiName;
  }

  /**
   * @return the tableMeasuresAndDataTypeMap
   */
  public Map<String, String> getTableMeasuresAndDataTypeMap() {
    return tableMeasuresAndDataTypeMap;
  }

  /**
   * @param tableMeasuresAndDataTypeMap the tableMeasuresAndDataTypeMap to set
   */
  public void setTableMeasuresAndDataTypeMap(Map<String, String> tableMeasuresAndDataTypeMap) {
    this.tableMeasuresAndDataTypeMap = tableMeasuresAndDataTypeMap;
  }

  /**
   * @return the tableInputSqlQuery
   */
  public String getTableInputSqlQuery() {
    return tableInputSqlQuery;
  }

  /**
   * @param tableInputSqlQuery the tableInputSqlQuery to set
   */
  public void setTableInputSqlQuery(String tableInputSqlQuery) {
    this.tableInputSqlQuery = tableInputSqlQuery;
  }

  /**
   * @return the dimensionSqlQuery
   */
  public String getDimensionSqlQuery() {
    return dimensionSqlQuery;
  }

  /**
   * @param dimensionSqlQuery the dimensionSqlQuery to set
   */
  public void setDimensionSqlQuery(String dimensionSqlQuery) {
    this.dimensionSqlQuery = dimensionSqlQuery;
  }

  /**
   * @return the sortSize
   */
  public String getSortSize() {
    return sortSize;
  }

  /**
   * @param sortSize the sortSize to set
   */
  public void setSortSize(String sortSize) {
    this.sortSize = sortSize;
  }

  /**
   * @return the isAGG
   */
  public boolean isAGG() {
    return isAGG;
  }

  /**
   * @param isAGG the isAGG to set
   */
  public void setAGG(boolean isAGG) {
    this.isAGG = isAGG;
  }

  /**
   * @return the driverclass
   */
  public String getDriverclass() {
    return driverclass;
  }

  /**
   * @param driverclass the driverclass to set
   */
  public void setDriverclass(String driverclass) {
    this.driverclass = driverclass;
  }

  /**
   * @return the username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username the username to set
   */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password the password to set
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return the connectionUrl
   */
  public String getConnectionUrl() {
    return connectionUrl;
  }

  /**
   * @param connectionUrl the connectionUrl to set
   */
  public void setConnectionUrl(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }

  /**
   * @return the actualDims
   */
  public String[] getActualDims() {
    return actualDims;
  }

  /**
   * @param actualDims the actualDims to set
   */
  public void setActualDims(String[] actualDims) {
    this.actualDims = actualDims;
  }

  /**
   * @return the dimensionTableNames
   */
  public String getDimensionTableNames() {
    return dimensionTableNames;
  }

  /**
   * @param dimensionTableNames the dimensionTableNames to set
   */
  public void setDimensionTableNames(String dimensionTableNames) {
    this.dimensionTableNames = dimensionTableNames;
  }

  /**
   * @return
   */
  public String getDimensionColumnIds() {
    return dimensionColumnIds;
  }

  /**
   * @param dimensionColumnIds column Ids for dimensions in a table
   */
  public void setDimensionColumnIds(String dimensionColumnIds) {
    this.dimensionColumnIds = dimensionColumnIds;
  }

  /**
   * getMdkeySize
   *
   * @return String
   */
  public String getMdkeySize() {
    return mdkeySize;
  }

  /**
   * setMdkeySize
   *
   * @param mdkeySize void
   */
  public void setMdkeySize(String mdkeySize) {
    this.mdkeySize = mdkeySize;
  }

  /**
   * getMeasureCount
   *
   * @return String
   */
  public String getMeasureCount() {
    return measureCount;
  }

  /**
   * setMeasureCount
   *
   * @param measureCount void
   */
  public void setMeasureCount(String measureCount) {
    this.measureCount = measureCount;
  }

  /**
   * getHeirAndKeySizeString
   *
   * @return String
   */
  public String getHeirAndKeySizeString() {
    return heirAndKeySizeString;
  }

  /**
   * setHeirAndKeySizeString
   *
   * @param heirAndKeySizeString void
   */
  public void setHeirAndKeySizeString(String heirAndKeySizeString) {
    this.heirAndKeySizeString = heirAndKeySizeString;
  }

  /**
   * @return Returns the hierColumnString.
   */
  public String getHierColumnString() {
    return hierColumnString;
  }

  /**
   * @param hierColumnString The hierColumnString to set.
   */
  public void setHierColumnString(String hierColumnString) {
    this.hierColumnString = hierColumnString;
  }

  /**
   * @return Returns the forignKey.
   */
  public String[] getForignKey() {
    return forignKey;
  }

  /**
   * @param forignKey The forignKey to set.
   */
  public void setForignKey(String[] forignKey) {
    this.forignKey = forignKey;
  }

  /**
   * @return Returns the foreignKeyHierarchyString.
   */
  public String getForeignKeyHierarchyString() {
    return foreignKeyHierarchyString;
  }

  /**
   * @param foreignKeyHierarchyString The foreignKeyHierarchyString to set.
   */
  public void setForeignKeyHierarchyString(String foreignKeyHierarchyString) {
    this.foreignKeyHierarchyString = foreignKeyHierarchyString;
  }

  /**
   * @return Returns the primaryKeyString.
   */
  public String getPrimaryKeyString() {
    return primaryKeyString;
  }

  /**
   * @param primaryKeyString The primaryKeyString to set.
   */
  public void setPrimaryKeyString(String primaryKeyString) {
    this.primaryKeyString = primaryKeyString;
  }

  /**
   * @return the measureNamesString
   */
  public String getMeasureNamesString() {
    return measureNamesString;
  }

  /**
   * @param measureNamesString the measureNamesString to set
   */
  public void setMeasureNamesString(String measureNamesString) {
    this.measureNamesString = measureNamesString;
  }

  /**
   * @return Returns the aggType.
   */
  public String[] getAggType() {
    return aggType;
  }

  /**
   * @param aggType The aggType to set.
   */
  public void setAggType(String[] aggType) {
    this.aggType = aggType;
  }

  /**
   * @return Returns the actualDimensionColumns.
   */
  public String getActualDimensionColumns() {
    return actualDimensionColumns;
  }

  /**
   * @param actualDimensionColumns The actualDimensionColumns to set.
   */
  public void setActualDimensionColumns(String actualDimensionColumns) {
    this.actualDimensionColumns = actualDimensionColumns;
  }

  /**
   * getForgienKeyAndPrimaryKeyMapString
   *
   * @return String
   */
  public String getForgienKeyAndPrimaryKeyMapString() {
    return forgienKeyAndPrimaryKeyMapString;
  }

  /**
   * setForgienKeyAndPrimaryKeyMapString
   *
   * @param forgienKeyAndPrimaryKeyMapString void
   */
  public void setForgienKeyAndPrimaryKeyMapString(String forgienKeyAndPrimaryKeyMapString) {
    this.forgienKeyAndPrimaryKeyMapString = forgienKeyAndPrimaryKeyMapString;
  }

  /**
   * @return Returns the heirAndDimLens.
   */
  public String getHeirAndDimLens() {
    return heirAndDimLens;
  }

  /**
   * @param heirAndDimLens The heirAndDimLens to set.
   */
  public void setHeirAndDimLens(String heirAndDimLens) {
    this.heirAndDimLens = heirAndDimLens;
  }

  /**
   * @return Returns the measureDataTypeInfo.
   */
  public String getMeasureDataTypeInfo() {
    return measureDataTypeInfo;
  }

  /**
   * @param measureDataTypeInfo The measureDataTypeInfo to set.
   */
  public void setMeasureDataTypeInfo(String measureDataTypeInfo) {
    this.measureDataTypeInfo = measureDataTypeInfo;
  }

  public String getColumnAndTableNameColumnMapForAgg() {
    return columnAndTableNameColumnMapForAgg;
  }

  public void setColumnAndTableNameColumnMapForAgg(String columnAndTableNameColumnMapForAgg) {
    this.columnAndTableNameColumnMapForAgg = columnAndTableNameColumnMapForAgg;
  }

  /**
   * @return Returns the denormColumns.
   */
  public String getDenormColumns() {
    return denormColumns;
  }

  /**
   * @param denormColumns The denormColumns to set.
   */
  public void setDenormColumns(String denormColumns) {
    this.denormColumns = denormColumns;
  }

  /**
   * @return the aggClass
   */
  public String[] getAggClass() {
    return aggClass;
  }

  /**
   * @param aggClass the aggClass to set
   */
  public void setAggClass(String[] aggClass) {
    this.aggClass = aggClass;
  }

  /**
   * @return the measureUniqueColumnNamesString
   */
  public String getMeasureUniqueColumnNamesString() {
    return measureUniqueColumnNamesString;
  }

  /**
   * @param measureUniqueColumnNamesString the measureUniqueColumnNamesString to set
   */
  public void setMeasureUniqueColumnNamesString(String measureUniqueColumnNamesString) {
    this.measureUniqueColumnNamesString = measureUniqueColumnNamesString;
  }

  /**
   * @return the type
   */
  public char[] getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(char[] type) {
    this.type = type;
  }

  public String getLevelAnddataType() {
    return levelAnddataType;
  }

  public void setLevelAnddataType(String levelAnddataType) {
    this.levelAnddataType = levelAnddataType;
  }

  /**
   * getNoDictionaryDims.
   *
   * @return
   */
  public String getNoDictionaryDims() {
    return noDictionaryDims;
  }

  /**
   * setNoDictionaryDims.
   *
   * @param noDictionaryDims
   */
  public void setNoDictionaryDims(String noDictionaryDims) {
    this.noDictionaryDims = noDictionaryDims;
  }

  /**
   * Set Wrapper Object having the columnschemadetails
   *
   * @param columnSchemaDetailsWrapper
   */
  public void setColumnSchemaDetails(ColumnSchemaDetailsWrapper columnSchemaDetailsWrapper) {
    this.columnSchemaDetailsWrapper = columnSchemaDetailsWrapper;
  }

  /**
   * return the Wrapper Object having the columnschemadetails
   * @return
   */
  public ColumnSchemaDetailsWrapper getColumnSchemaDetails() {
    return columnSchemaDetailsWrapper;
  }

  /**
   * set wraper object having table options needed while dataload
   *
   * @return
   */
  public void setTableOptionWrapper(TableOptionWrapper tableOptionWrapper) {
    this.tableOptionWrapper = tableOptionWrapper;
  }

  /**
   * method returns the table options detail wrapper instance.
   * @return
   */
  public TableOptionWrapper getTableOptionWrapper() {
    return tableOptionWrapper;
  }

  public void setColumnGroupsString(String columnGroups) {
    this.columnGroupsString = columnGroups;
  }

  /**
   * @return columngroups
   */
  public String getColumnGroupsString() {
    return columnGroupsString;
  }

  public Boolean[] getIsNoDictionaryDimMapping() {
    return isNoDictionaryDimMapping;
  }

  public void setIsNoDictionaryDimMapping(Boolean[] isNoDictionaryDimMapping) {
    this.isNoDictionaryDimMapping = isNoDictionaryDimMapping;
  }

  public void setColumnPropertiesString(String columnPropertiesString) {
    this.columnPropertiesString = columnPropertiesString;
  }

  public String getColumnPropertiesString() {
    return this.columnPropertiesString;
  }

  /**
   * @return columngroups
   */
  public String getDimensionColumnsDataType() {
    return columnsDataTypeString;
  }

  public void setDimensionColumnsDataType(String columnsDataTypeString) {
    this.columnsDataTypeString = columnsDataTypeString;
  }
}
