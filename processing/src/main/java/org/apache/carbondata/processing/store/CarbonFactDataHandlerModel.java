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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.SortScopeOptions;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datamap.DataMapWriterListener;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

// This class contains all the data required for processing and writing the carbon data
// TODO: we should try to minimize this class as refactorying loading process
public class CarbonFactDataHandlerModel {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataHandlerModel.class.getName());

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
   * no dictionary and complex columns in the table
   */
  private CarbonColumn[] noDictAndComplexColumns;

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

  private Map<String, LocalDictionaryGenerator> columnLocalDictGenMap;

  private int numberOfCores;

  private List<Integer> varcharDimIdxInNoDict;

  private String columnCompressor;

  /**
   * Create the model using @{@link CarbonDataLoadConfiguration}
   */
  public static CarbonFactDataHandlerModel createCarbonFactDataHandlerModel(
      CarbonDataLoadConfiguration configuration, String[] storeLocation, int bucketId,
      int taskExtension, DataMapWriterListener listener) {
    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();

    int[] dimLensWithComplex = configuration.getCardinalityFinder().getCardinality();
    if (!configuration.isSortTable()) {
      for (int i = 0; i < dimLensWithComplex.length; i++) {
        if (dimLensWithComplex[i] != 0) {
          dimLensWithComplex[i] = Integer.MAX_VALUE;
        }
      }
    }
    CarbonTable carbonTable = configuration.getTableSpec().getCarbonTable();

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
    int complexDimensionCount = configuration.getComplexDictionaryColumnCount() + configuration
        .getComplexNonDictionaryColumnCount();
    int measureCount = configuration.getMeasureCount();

    int simpleDimsCount = dimensionCount - noDictionaryCount - complexDimensionCount;
    int[] simpleDimsLen = new int[simpleDimsCount];
    for (int i = 0; i < simpleDimsCount; i++) {
      simpleDimsLen[i] = dimLens[i];
    }

    int noDictionayDimensionIndex = 0;
    // for dynamic page size in write step if varchar columns exist
    List<Integer> varcharDimIdxInNoDict = new ArrayList<>();
    for (DataField dataField : configuration.getDataFields()) {
      CarbonColumn column = dataField.getColumn();
      if (!dataField.hasDictionaryEncoding()) {
        if (!column.isComplex() && column.getDataType() == DataTypes.VARCHAR) {
          varcharDimIdxInNoDict.add(noDictionayDimensionIndex);
        }
        noDictionayDimensionIndex++;
      }
    }

    //To Set MDKey Index of each primitive type in complex type
    int surrIndex = simpleDimsCount;
    Iterator<Map.Entry<String, GenericDataType>> complexMap =
        CarbonDataProcessorUtil.getComplexTypesMap(configuration.getDataFields(), configuration)
            .entrySet().iterator();
    Map<Integer, GenericDataType> complexIndexMap = new HashMap<>(complexDimensionCount);
    while (complexMap.hasNext()) {
      Map.Entry<String, GenericDataType> complexDataType = complexMap.next();
      complexDataType.getValue().setOutputArrayIndex(0);
      complexIndexMap.put(simpleDimsCount, complexDataType.getValue());
      simpleDimsCount++;
      List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
      complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
      for (GenericDataType eachPrimitive : primitiveTypes) {
        if (eachPrimitive.getIsColumnDictionary()) {
          eachPrimitive.setSurrogateIndex(surrIndex++);
        }
      }
    }

    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(Long.parseLong(configuration.getTaskNo()),
            (Long) configuration.getDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP));
    String carbonDataDirectoryPath = getCarbonDataFolderLocation(configuration);

    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setSchemaUpdatedTimeStamp(configuration.getSchemaUpdatedTimeStamp());
    carbonFactDataHandlerModel.setDatabaseName(identifier.getDatabaseName());
    carbonFactDataHandlerModel.setTableName(identifier.getTableName());
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
    carbonFactDataHandlerModel
        .setNoDictAndComplexColumns(configuration.getNoDictAndComplexDimensions());
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    carbonFactDataHandlerModel.setPrimitiveDimLens(simpleDimsLen);
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    carbonFactDataHandlerModel.setComplexDimensionKeyGenerator(
        configuration.createKeyGeneratorForComplexDimension());
    carbonFactDataHandlerModel.bucketId = bucketId;
    carbonFactDataHandlerModel.segmentId = configuration.getSegmentId();
    carbonFactDataHandlerModel.taskExtension = taskExtension;
    carbonFactDataHandlerModel.tableSpec = configuration.getTableSpec();
    carbonFactDataHandlerModel.sortScope = CarbonDataProcessorUtil.getSortScope(configuration);
    carbonFactDataHandlerModel.columnCompressor = configuration.getColumnCompressor();

    if (listener == null) {
      listener = new DataMapWriterListener();
      listener.registerAllWriter(
          configuration.getTableSpec().getCarbonTable(),
          configuration.getSegmentId(),
          CarbonTablePath.getShardName(
              carbonDataFileAttributes.getTaskId(),
              bucketId,
              0,
              String.valueOf(carbonDataFileAttributes.getFactTimeStamp()),
              configuration.getSegmentId()),
          segmentProperties);
    }
    carbonFactDataHandlerModel.dataMapWriterlistener = listener;
    carbonFactDataHandlerModel.writingCoresCount = configuration.getWritingCoresCount();
    carbonFactDataHandlerModel.initNumberOfCores();
    carbonFactDataHandlerModel.setVarcharDimIdxInNoDict(varcharDimIdxInNoDict);
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
      String[] tempStoreLocation, String carbonDataDirectoryPath) {

    // for dynamic page size in write step if varchar columns exist
    List<Integer> varcharDimIdxInNoDict = new ArrayList<>();
    List<CarbonDimension> allDimensions = carbonTable.getDimensions();
    int dictDimCount = allDimensions.size() - segmentProperties.getNumberOfNoDictionaryDimension()
            - segmentProperties.getComplexDimensions().size();
    CarbonColumn[] noDicAndComplexColumns =
        new CarbonColumn[segmentProperties.getNumberOfNoDictionaryDimension() + segmentProperties
            .getComplexDimensions().size()];
    int noDicAndComp = 0;
    for (CarbonDimension dim : allDimensions) {
      if (!dim.isComplex() && !dim.hasEncoding(Encoding.DICTIONARY) &&
          dim.getDataType() == DataTypes.VARCHAR) {
        // ordinal is set in CarbonTable.fillDimensionsAndMeasuresForTables()
        varcharDimIdxInNoDict.add(dim.getOrdinal() - dictDimCount);
      }
      if (!dim.hasEncoding(Encoding.DICTIONARY)) {
        noDicAndComplexColumns[noDicAndComp++] =
            new CarbonColumn(dim.getColumnSchema(), dim.getOrdinal(), dim.getSchemaOrdinal());
      }
    }

    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setSchemaUpdatedTimeStamp(carbonTable.getTableLastUpdatedTime());
    carbonFactDataHandlerModel.setDatabaseName(loadModel.getDatabaseName());
    carbonFactDataHandlerModel.setTableName(tableName);
    carbonFactDataHandlerModel.setMeasureCount(segmentProperties.getMeasures().size());
    carbonFactDataHandlerModel.setStoreLocation(tempStoreLocation);
    carbonFactDataHandlerModel.setDimLens(segmentProperties.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel.setSegmentId(loadModel.getSegmentId());
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

    carbonFactDataHandlerModel.setComplexIndexMap(
        convertComplexDimensionToGenericDataType(segmentProperties,
            loadModel.getSerializationNullFormat()));
    DataType[] measureDataTypes = new DataType[segmentProperties.getMeasures().size()];
    int i = 0;
    for (CarbonMeasure msr : segmentProperties.getMeasures()) {
      measureDataTypes[i++] = msr.getDataType();
    }
    carbonFactDataHandlerModel.setMeasureDataType(measureDataTypes);
    carbonFactDataHandlerModel.setNoDictAndComplexColumns(noDicAndComplexColumns);
    CarbonUtil.checkAndCreateFolderWithPermission(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setPrimitiveDimLens(segmentProperties.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    carbonFactDataHandlerModel.setColumnCompressor(loadModel.getColumnCompressor());

    carbonFactDataHandlerModel.tableSpec = new TableSpec(carbonTable);
    DataMapWriterListener listener = new DataMapWriterListener();
    listener.registerAllWriter(
        carbonTable,
        loadModel.getSegmentId(),
        CarbonTablePath.getShardName(
            CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(loadModel.getTaskNo()),
            carbonFactDataHandlerModel.getBucketId(),
            carbonFactDataHandlerModel.getTaskExtension(),
            String.valueOf(loadModel.getFactTimeStamp()),
            loadModel.getSegmentId()),
        segmentProperties);
    carbonFactDataHandlerModel.dataMapWriterlistener = listener;
    carbonFactDataHandlerModel.initNumberOfCores();
    carbonFactDataHandlerModel
        .setColumnLocalDictGenMap(CarbonUtil.getLocalDictionaryModel(carbonTable));
    carbonFactDataHandlerModel.setVarcharDimIdxInNoDict(varcharDimIdxInNoDict);
    return carbonFactDataHandlerModel;
  }

  /**
   * This routine takes the Complex Dimension and convert into generic DataType.
   *
   * @param segmentProperties
   * @param isNullFormat
   * @return
   */
  private static Map<Integer, GenericDataType> convertComplexDimensionToGenericDataType(
      SegmentProperties segmentProperties, String isNullFormat) {
    List<CarbonDimension> complexDimensions = segmentProperties.getComplexDimensions();
    Map<Integer, GenericDataType> complexIndexMap = new HashMap<>(complexDimensions.size());
    int dimensionCount = -1;
    if (segmentProperties.getDimensions().size() == 0) {
      dimensionCount = 0;
    } else {
      dimensionCount = segmentProperties.getDimensions().size() - segmentProperties
          .getNumberOfNoDictionaryDimension() - segmentProperties.getComplexDimensions().size();
    }
    for (CarbonDimension carbonDimension : complexDimensions) {
      if (carbonDimension.isComplex()) {
        GenericDataType genericDataType;
        DataType dataType = carbonDimension.getDataType();
        if (DataTypes.isArrayType(dataType)) {
          genericDataType =
              new ArrayDataType(carbonDimension.getColName(), "", carbonDimension.getColumnId());
        } else if (DataTypes.isStructType(dataType)) {
          genericDataType =
              new StructDataType(carbonDimension.getColName(), "", carbonDimension.getColumnId());
        } else {
          // Add Primitive type.
          throw new RuntimeException("Primitive Type should not be coming in first loop");
        }
        if (carbonDimension.getNumberOfChild() > 0) {
          addChildrenForComplex(carbonDimension.getListOfChildDimensions(), genericDataType,
              isNullFormat);
        }
        genericDataType.setOutputArrayIndex(0);
        complexIndexMap.put(dimensionCount++, genericDataType);
      }

    }
    return complexIndexMap;
  }

  private static void addChildrenForComplex(List<CarbonDimension> listOfChildDimensions,
      GenericDataType genericDataType, String isNullFormat) {
    for (CarbonDimension carbonDimension : listOfChildDimensions) {
      String parentColName =
          carbonDimension.getColName().substring(0, carbonDimension.getColName().lastIndexOf("."));
      DataType dataType = carbonDimension.getDataType();
      if (DataTypes.isArrayType(dataType)) {
        GenericDataType arrayGeneric =
            new ArrayDataType(carbonDimension.getColName(), parentColName,
                carbonDimension.getColumnId());
        if (carbonDimension.getNumberOfChild() > 0) {
          addChildrenForComplex(carbonDimension.getListOfChildDimensions(), arrayGeneric,
              isNullFormat);
        }
        genericDataType.addChildren(arrayGeneric);
      } else if (DataTypes.isStructType(dataType)) {
        GenericDataType structGeneric =
            new StructDataType(carbonDimension.getColName(), parentColName,
                carbonDimension.getColumnId());
        if (carbonDimension.getNumberOfChild() > 0) {
          addChildrenForComplex(carbonDimension.getListOfChildDimensions(), structGeneric,
              isNullFormat);
        }
        genericDataType.addChildren(structGeneric);
      } else {
        // Primitive Data Type
        genericDataType.addChildren(
            new PrimitiveDataType(carbonDimension.getColumnSchema().getColumnName(),
                dataType, parentColName, carbonDimension.getColumnId(),
                carbonDimension.getColumnSchema().hasEncoding(Encoding.DICTIONARY), isNullFormat));
      }
    }
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  private static String getCarbonDataFolderLocation(CarbonDataLoadConfiguration configuration) {
    // configuration.getDataWritePath will not be null only in case of partition
    if (configuration.getDataWritePath() != null) {
      String paths = configuration.getDataWritePath();
      AbsoluteTableIdentifier absoluteTableIdentifier = configuration.getTableIdentifier();
      String partPath = absoluteTableIdentifier.getTablePath();
      String[] dirs = paths.split(partPath);
      /* it will create folder one by one and apply the permissions
       else creation of folder in one go will set the permission for last directory only
       e.g. paths="/home/rahul/Documents/store/carbonTable1/emp_name=rahul/loc=india/dept=rd"
            So, dirs={"","/emp_name=rahul/loc=india/dept=rd"}
            if (dirs.length > 1) then partDirs ={"","emp_name=rahul","loc=india","dept=rd"}
            forEach partDirs partpath(say "/home/rahul/Documents/store/carbonTable1") will
            be keep appending with "emp_name=rahul","loc=india","dept=rd" sequentially
      */
      if (dirs.length > 1) {
        String[] partDirs = dirs[1].split(CarbonCommonConstants.FILE_SEPARATOR);
        for (String partDir : partDirs) {
          if (!partDir.isEmpty()) {
            partPath = partPath.concat(CarbonCommonConstants.FILE_SEPARATOR + partDir);
            CarbonUtil.checkAndCreateFolderWithPermission(partPath);
          }
        }
      } else {
        CarbonUtil.checkAndCreateFolderWithPermission(paths);
      }
      return paths;
    }
    AbsoluteTableIdentifier absoluteTableIdentifier = configuration.getTableIdentifier();
    String carbonDataDirectoryPath;
    if (!configuration.isCarbonTransactionalTable()) {
      carbonDataDirectoryPath = absoluteTableIdentifier.getTablePath();
    } else {
      carbonDataDirectoryPath = CarbonTablePath
          .getSegmentPath(absoluteTableIdentifier.getTablePath(),
              configuration.getSegmentId() + "");
    }
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
    initNumberOfCores();
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

  public Map<String, LocalDictionaryGenerator> getColumnLocalDictGenMap() {
    return columnLocalDictGenMap;
  }

  public void setColumnLocalDictGenMap(
      Map<String, LocalDictionaryGenerator> columnLocalDictGenMap) {
    this.columnLocalDictGenMap = columnLocalDictGenMap;
  }

  private void initNumberOfCores() {
    // in compaction flow the measure with decimal type will come as spark decimal.
    // need to convert it to byte array.
    if (this.isCompactionFlow()) {
      this.numberOfCores = CarbonProperties.getInstance().getNumberOfCompactingCores();
    } else {
      this.numberOfCores = CarbonProperties.getInstance().getNumberOfLoadingCores();
    }

    if (this.sortScope != null && this.sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT)) {
      this.numberOfCores = 1;
    }
    // Overriding it to the task specified cores.
    if (this.getWritingCoresCount() > 0) {
      this.numberOfCores = this.getWritingCoresCount();
    }
  }

  public int getNumberOfCores() {
    return numberOfCores;
  }

  public void setVarcharDimIdxInNoDict(List<Integer> varcharDimIdxInNoDict) {
    this.varcharDimIdxInNoDict = varcharDimIdxInNoDict;
  }

  public List<Integer> getVarcharDimIdxInNoDict() {
    return varcharDimIdxInNoDict;
  }

  public String getColumnCompressor() {
    return columnCompressor;
  }

  public void setColumnCompressor(String columnCompressor) {
    this.columnCompressor = columnCompressor;
  }

  public CarbonColumn[] getNoDictAndComplexColumns() {
    return noDictAndComplexColumns;
  }

  public void setNoDictAndComplexColumns(CarbonColumn[] noDictAndComplexColumns) {
    this.noDictAndComplexColumns = noDictAndComplexColumns;
  }
}

