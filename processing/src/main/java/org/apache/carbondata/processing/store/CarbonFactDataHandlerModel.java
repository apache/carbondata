/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datamap.DataMapWriterListener;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.sort.SortScopeOptions;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

// This class contains all the data required for processing and writing the carbon data
// TODO: we should try to minimize this class as refactorying loading process
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
   * total count of measures in table
   */
  private int measureCount;
  /**
   * local store location
   */
  private String[] storeLocation;
  /**
   * flag to check whether use inverted index
   */
  private boolean[] isUseInvertedIndex;

  /**
   * length of each dimension, including dictionary, nodictioncy, complex dimension
   */
  private int[] dimLens;

  /**
   * total number of no dictionary dimension in the table
   */
  private int noDictionaryCount;
  /**
   * total number of dictionary dimension and complex dimension columns in table
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
   * data type of all measures in the table
   */
  private DataType[] measureDataType;
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

  private int bucketId = 0;

  private String segmentId;

  /**
   * schema updated time stamp to be used for restructure scenarios
   */
  private long schemaUpdatedTimeStamp;

  private int taskExtension;

  // key generator for complex dimension
  private KeyGenerator[] complexDimensionKeyGenerator;

  private TableSpec tableSpec;

  private SortScopeOptions.SortScope sortScope;

  private DataMapWriterListener dataMapWriterlistener;

  private short writingCoresCount;

  /**
   * Create the model using @{@link CarbonDataLoadConfiguration}
   */
  public static CarbonFactDataHandlerModel createCarbonFactDataHandlerModel(
      CarbonDataLoadConfiguration configuration, String[] storeLocation, int bucketId,
      int taskExtension) {
    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    boolean[] isUseInvertedIndex =
        CarbonDataProcessorUtil.getIsUseInvertedIndex(configuration.getDataFields());

    int[] dimLensWithComplex = configuration.getCardinalityFinder().getCardinality();
    if (!configuration.isSortTable()) {
      for (int i = 0; i < dimLensWithComplex.length; i++) {
        if (dimLensWithComplex[i] != 0) {
          dimLensWithComplex[i] = Integer.MAX_VALUE;
        }
      }
    }
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        identifier.getDatabaseName(), identifier.getTableName());

    List<ColumnSchema> wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(identifier.getTableName()),
            carbonTable.getMeasureByTableName(identifier.getTableName()));
    int[] colCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchema);

    SegmentProperties segmentProperties =
        new SegmentProperties(wrapperColumnSchema, colCardinality);

    int[] dimLens = configuration.calcDimensionLengths();

    int dimensionCount = configuration.getDimensionCount();
    int noDictionaryCount = configuration.getNoDictionaryCount();
    int complexDimensionCount = configuration.getComplexColumnCount();
    int measureCount = configuration.getMeasureCount();

    int simpleDimsCount = dimensionCount - noDictionaryCount - complexDimensionCount;
    int[] simpleDimsLen = new int[simpleDimsCount];
    for (int i = 0; i < simpleDimsCount; i++) {
      simpleDimsLen[i] = dimLens[i];
    }
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
        new CarbonDataFileAttributes(Long.parseLong(configuration.getTaskNo()),
            (Long) configuration.getDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP));
    String carbonDataDirectoryPath = getCarbonDataFolderLocation(configuration);

    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setSchemaUpdatedTimeStamp(configuration.getSchemaUpdatedTimeStamp());
    carbonFactDataHandlerModel.setDatabaseName(
        identifier.getDatabaseName());
    carbonFactDataHandlerModel
        .setTableName(identifier.getTableName());
    carbonFactDataHandlerModel.setMeasureCount(measureCount);
    carbonFactDataHandlerModel.setStoreLocation(storeLocation);
    carbonFactDataHandlerModel.setDimLens(dimLens);
    carbonFactDataHandlerModel.setNoDictionaryCount(noDictionaryCount);
    carbonFactDataHandlerModel.setDimensionCount(
        configuration.getDimensionCount() - noDictionaryCount);
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel.setColCardinality(colCardinality);
    carbonFactDataHandlerModel.setMeasureDataType(configuration.getMeasureDataType());
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    carbonFactDataHandlerModel.setPrimitiveDimLens(simpleDimsLen);
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndex);
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    carbonFactDataHandlerModel.setComplexDimensionKeyGenerator(
        configuration.createKeyGeneratorForComplexDimension());
    carbonFactDataHandlerModel.bucketId = bucketId;
    carbonFactDataHandlerModel.segmentId = configuration.getSegmentId();
    carbonFactDataHandlerModel.taskExtension = taskExtension;
    carbonFactDataHandlerModel.tableSpec = configuration.getTableSpec();
    carbonFactDataHandlerModel.sortScope = CarbonDataProcessorUtil.getSortScope(configuration);

    DataMapWriterListener listener = new DataMapWriterListener();
    listener.registerAllWriter(configuration.getTableSpec().getCarbonTable(),
        configuration.getSegmentId(), storeLocation[new Random().nextInt(storeLocation.length)]);
    carbonFactDataHandlerModel.dataMapWriterlistener = listener;
    carbonFactDataHandlerModel.writingCoresCount = configuration.getWritingCoresCount();

    return carbonFactDataHandlerModel;
  }

  /**
   * This method will create a model object for carbon fact data handler
   *
   * @param loadModel
   * @return
   */
  public static CarbonFactDataHandlerModel getCarbonFactDataHandlerModel(CarbonLoadModel loadModel,
      CarbonTable carbonTable, SegmentProperties segmentProperties, String tableName,
      String[] tempStoreLocation) {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setSchemaUpdatedTimeStamp(carbonTable.getTableLastUpdatedTime());
    carbonFactDataHandlerModel.setDatabaseName(loadModel.getDatabaseName());
    carbonFactDataHandlerModel.setTableName(tableName);
    carbonFactDataHandlerModel.setMeasureCount(segmentProperties.getMeasures().size());
    carbonFactDataHandlerModel.setStoreLocation(tempStoreLocation);
    carbonFactDataHandlerModel.setDimLens(segmentProperties.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel
        .setNoDictionaryCount(segmentProperties.getNumberOfNoDictionaryDimension());
    carbonFactDataHandlerModel.setDimensionCount(
        segmentProperties.getDimensions().size() - carbonFactDataHandlerModel
            .getNoDictionaryCount());
    List<ColumnSchema> wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableName),
            carbonTable.getMeasureByTableName(tableName));
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    // get the cardinality for all all the columns including no dictionary columns
    int[] formattedCardinality = CarbonUtil
        .getFormattedCardinality(segmentProperties.getDimColumnsCardinality(), wrapperColumnSchema);
    carbonFactDataHandlerModel.setColCardinality(formattedCardinality);
    //TO-DO Need to handle complex types here .
    Map<Integer, GenericDataType> complexIndexMap =
        new HashMap<Integer, GenericDataType>(segmentProperties.getComplexDimensions().size());
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    DataType[] measureDataTypes = new DataType[segmentProperties.getMeasures().size()];
    int i = 0;
    for (CarbonMeasure msr : segmentProperties.getMeasures()) {
      measureDataTypes[i++] = msr.getDataType();
    }
    carbonFactDataHandlerModel.setMeasureDataType(measureDataTypes);
    String carbonDataDirectoryPath = CarbonDataProcessorUtil
        .checkAndCreateCarbonStoreLocation(carbonTable.getTablePath(), loadModel.getSegmentId());
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    List<CarbonDimension> dimensionByTableName = carbonTable.getDimensionByTableName(tableName);
    boolean[] isUseInvertedIndexes = new boolean[dimensionByTableName.size()];
    int index = 0;
    for (CarbonDimension dimension : dimensionByTableName) {
      isUseInvertedIndexes[index++] = dimension.isUseInvertedIndex();
    }
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndexes);
    carbonFactDataHandlerModel.setPrimitiveDimLens(segmentProperties.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());

    carbonFactDataHandlerModel.tableSpec =
        new TableSpec(loadModel.getCarbonDataLoadSchema().getCarbonTable());
    DataMapWriterListener listener = new DataMapWriterListener();
    listener.registerAllWriter(
        loadModel.getCarbonDataLoadSchema().getCarbonTable(),
        loadModel.getSegmentId(),
        tempStoreLocation[new Random().nextInt(tempStoreLocation.length)]);
    carbonFactDataHandlerModel.dataMapWriterlistener = listener;
    return carbonFactDataHandlerModel;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  private static String getCarbonDataFolderLocation(CarbonDataLoadConfiguration configuration) {
    AbsoluteTableIdentifier identifier = configuration.getTableIdentifier();
    String carbonDataDirectoryPath =
        CarbonTablePath.getSegmentPath(identifier.getTablePath(), configuration.getSegmentId());
    CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
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

  public int getMeasureCount() {
    return measureCount;
  }

  public void setMeasureCount(int measureCount) {
    this.measureCount = measureCount;
  }

  public String[] getStoreLocation() {
    return storeLocation;
  }

  public void setStoreLocation(String[] storeLocation) {
    this.storeLocation = storeLocation;
  }

  public int[] getDimLens() {
    return dimLens;
  }

  public void setDimLens(int[] dimLens) {
    this.dimLens = dimLens;
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

  public DataType[] getMeasureDataType() {
    return measureDataType;
  }

  public void setMeasureDataType(DataType[] measureDataType) {
    this.measureDataType = measureDataType;
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

  public int getBucketId() {
    return bucketId;
  }

  public void setBucketId(Integer bucketId) { this.bucketId = bucketId; }

  public long getSchemaUpdatedTimeStamp() {
    return schemaUpdatedTimeStamp;
  }

  public void setSchemaUpdatedTimeStamp(long schemaUpdatedTimeStamp) {
    this.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  public int getTaskExtension() {
    return taskExtension;
  }

  public KeyGenerator[] getComplexDimensionKeyGenerator() {
    return complexDimensionKeyGenerator;
  }

  public void setComplexDimensionKeyGenerator(KeyGenerator[] complexDimensionKeyGenerator) {
    this.complexDimensionKeyGenerator = complexDimensionKeyGenerator;
  }

  public KeyGenerator getMDKeyGenerator() {
    return segmentProperties.getDimensionKeyGenerator();
  }

  // return the number of complex columns
  public int getComplexColumnCount() {
    return complexIndexMap.size();
  }

  // return the number of complex column after complex columns are expanded
  public int getExpandedComplexColsCount() {
    int count = 0;
    int dictDimensionCount = getDimensionCount();
    for (int i = 0; i < dictDimensionCount; i++) {
      GenericDataType complexDataType = getComplexIndexMap().get(i);
      if (complexDataType != null) {
        count += complexDataType.getColsCount();
      }
    }
    return count;
  }

  public boolean isSortColumn(int columnIndex) {
    return columnIndex < segmentProperties.getNumberOfSortColumns();
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public SortScopeOptions.SortScope getSortScope() {
    return sortScope;
  }

  public short getWritingCoresCount() {
    return writingCoresCount;
  }

  public DataMapWriterListener getDataMapWriterlistener() {
    return dataMapWriterlistener;
  }
}

