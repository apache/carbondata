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

package org.apache.carbondata.processing.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

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

  public int getBlockSizeInMB() {
    return blockSize;
  }

  public void setBlockSizeInMB(int blockSize) {
    this.blockSize = blockSize;
  }

  /**
   * table blocksize in MB
   */
  private int blockSize;
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
   * flag to check whether use inverted index
   */
  private boolean[] isUseInvertedIndex;
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
   * Segment properties
   */
  private SegmentProperties segmentProperties;

  /**
   * primitive dimensions cardinality
   */
  private int[] primitiveDimLens;

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

  /**
   * cardinality of dimension including no dictionary. no dictionary cardinality
   * is set to -1
   */
  private int[] colCardinality;

  /**
   * wrapper column schema
   */
  private List<ColumnSchema> wrapperColumnSchema;

  /**
   * This is the boolean which will determine whether the data handler call is from the compaction
   * or not.
   */
  private boolean isCompactionFlow;

  /**
   * To use kettle flow to load or not.
   */
  private boolean useKettle = true;

  /**
   * Create the model using @{@link CarbonDataLoadConfiguration}
   * @param configuration
   * @return CarbonFactDataHandlerModel
   */
  public static CarbonFactDataHandlerModel createCarbonFactDataHandlerModel(
      CarbonDataLoadConfiguration configuration, String storeLocation, String partitionId) {

    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonTableIdentifier tableIdentifier =
        identifier;
    boolean[] isUseInvertedIndex =
        CarbonDataProcessorUtil.getIsUseInvertedIndex(configuration.getDataFields());

    int[] dimLensWithComplex =
        (int[]) configuration.getDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS);
    List<Integer> dimsLenList = new ArrayList<Integer>();
    for (int eachDimLen : dimLensWithComplex) {
      if (eachDimLen != 0) dimsLenList.add(eachDimLen);
    }
    int[] dimLens = new int[dimsLenList.size()];
    for (int i = 0; i < dimsLenList.size(); i++) {
      dimLens[i] = dimsLenList.get(i);
    }

    int dimensionCount = configuration.getDimensionCount();
    int noDictionaryCount = configuration.getNoDictionaryCount();
    int complexDimensionCount = configuration.getComplexDimensionCount();
    int measureCount = configuration.getMeasureCount();

    int simpleDimsCount = dimensionCount - noDictionaryCount - complexDimensionCount;
    int[] simpleDimsLen = new int[simpleDimsCount];
    for (int i = 0; i < simpleDimsCount; i++) {
      simpleDimsLen[i] = dimLens[i];
    }

    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        tableIdentifier.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + tableIdentifier
            .getTableName());
    List<ColumnSchema> wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableIdentifier.getTableName()),
            carbonTable.getMeasureByTableName(tableIdentifier.getTableName()));
    int[] colCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchema);
    SegmentProperties segmentProperties =
        new SegmentProperties(wrapperColumnSchema, colCardinality);
    // Actual primitive dimension used to generate start & end key

    KeyGenerator keyGenerator = segmentProperties.getDimensionKeyGenerator();

    //To Set MDKey Index of each primitive type in complex type
    int surrIndex = simpleDimsCount;
    Iterator<Map.Entry<String, GenericDataType>> complexMap =
        CarbonDataProcessorUtil.getComplexTypesMap(configuration.getDataFields()).entrySet()
            .iterator();
    Map<Integer, GenericDataType> complexIndexMap = new HashMap<>(complexDimensionCount);
    while (complexMap.hasNext()) {
      Map.Entry<String, GenericDataType> complexDataType = complexMap.next();
      complexDataType.getValue().setOutputArrayIndex(0);
      complexIndexMap.put(simpleDimsCount, complexDataType.getValue());
      simpleDimsCount++;
      List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
      complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
      for (GenericDataType eachPrimitive : primitiveTypes) {
        eachPrimitive.setSurrogateIndex(surrIndex++);
      }
    }

    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(Integer.parseInt(configuration.getTaskNo()),
            (String) configuration.getDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP));
    String carbonDataDirectoryPath = getCarbonDataFolderLocation(configuration, partitionId);

    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setDatabaseName(
        identifier.getDatabaseName());
    carbonFactDataHandlerModel
        .setTableName(identifier.getTableName());
    carbonFactDataHandlerModel.setMeasureCount(measureCount);
    carbonFactDataHandlerModel.setMdKeyLength(keyGenerator.getKeySizeInBytes());
    carbonFactDataHandlerModel.setStoreLocation(storeLocation);
    carbonFactDataHandlerModel.setDimLens(dimLens);
    carbonFactDataHandlerModel.setNoDictionaryCount(noDictionaryCount);
    carbonFactDataHandlerModel
        .setDimensionCount(configuration.getDimensionCount() - noDictionaryCount);
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel.setColCardinality(colCardinality);
    carbonFactDataHandlerModel.setDataWritingRequest(true);
    carbonFactDataHandlerModel.setAggType(CarbonDataProcessorUtil
        .getAggType(measureCount, identifier.getDatabaseName(), identifier.getTableName()));
    carbonFactDataHandlerModel.setFactDimLens(dimLens);
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    carbonFactDataHandlerModel.setPrimitiveDimLens(simpleDimsLen);
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndex);
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    carbonFactDataHandlerModel.setUseKettle(false);
    if (noDictionaryCount > 0 || complexDimensionCount > 0) {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount + 1);
    } else {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount);
    }
    return carbonFactDataHandlerModel;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  private static String getCarbonDataFolderLocation(CarbonDataLoadConfiguration configuration,
      String partitionId) {
    String carbonStorePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        tableIdentifier.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + tableIdentifier
            .getTableName());
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath = carbonTablePath
        .getCarbonDataDirectoryPath(partitionId,
            configuration.getSegmentId() + "");
    return carbonDataDirectoryPath;
  }

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

  /**
   * To check whether the data handler is for compaction flow or not.
   * @return
   */
  public boolean isCompactionFlow() {
    return isCompactionFlow;
  }

  /**
   * If the handler is calling from the compaction flow set this to true.
   * @param compactionFlow
   */
  public void setCompactionFlow(boolean compactionFlow) {
    isCompactionFlow = compactionFlow;
  }

  public boolean[] getIsUseInvertedIndex() {
    return isUseInvertedIndex;
  }

  public void setIsUseInvertedIndex(boolean[] isUseInvertedIndex) {
    this.isUseInvertedIndex = isUseInvertedIndex;
  }
  /**
   *
   * @return segmentProperties
   */
  public SegmentProperties getSegmentProperties() {
    return segmentProperties;
  }

  /**
   *
   * @param segmentProperties
   */
  public void setSegmentProperties(SegmentProperties segmentProperties) {
    this.segmentProperties = segmentProperties;
  }

  /**
   * @return wrapperColumnSchema
   */
  public List<ColumnSchema> getWrapperColumnSchema() {
    return wrapperColumnSchema;
  }

  /**
   * @param wrapperColumnSchema
   */
  public void setWrapperColumnSchema(List<ColumnSchema> wrapperColumnSchema) {
    this.wrapperColumnSchema = wrapperColumnSchema;
  }

  public boolean isUseKettle() {
    return useKettle;
  }

  public void setUseKettle(boolean useKettle) {
    this.useKettle = useKettle;
  }
}

