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

package org.apache.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.schema.metadata.ColumnSchemaDetailsWrapper;
import org.apache.carbondata.processing.schema.metadata.HierarchiesInfo;
import org.apache.carbondata.processing.schema.metadata.TableOptionWrapper;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

public class CarbonCSVBasedSeqGenMeta extends BaseStepMeta implements StepMetaInterface {

  /**
   * pkg
   */
  private static Class<?> pkg = CarbonCSVBasedSeqGenMeta.class;
  /**
   * Foreign key and respective hierarchy Map
   */
  protected Map<String, String> foreignKeyHierarchyMap;
  /**
   * hier name
   */
  protected String[] hierNames;
  /**
   * dims
   */
  protected int[] dims;
  /**
   * dims
   */
  protected Map<String, GenericDataType> complexTypes =
      new HashMap<String, GenericDataType>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  protected Map<String, Map<String, String>> columnProperties =
      new HashMap<String, Map<String, String>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  /**
   * dimLens
   */
  protected int[] dimLens;
  /**
   * msrs
   */
  protected int[] msrs;
  /**
   * timehierName
   */
  protected String timehierName;
  /**
   * hirches
   */
  protected Map<String, int[]> hirches;
  /**
   * Hierarchy Column Name map
   */
  protected Map<String, String[]> hierColumnMap;
  /**
   * timeFormat
   */
  protected SimpleDateFormat timeFormat;
  /**
   * timeDimeIndex
   */
  protected int timeDimeIndex = -1;
  /**
   * timeLevels
   */
  protected String[] timeLevels = new String[0];
  /**
   * timeOrdinalCols
   */
  protected String[] timeOrdinalCols = new String[0];
  /**
   * timeOrdinalIndices
   */
  protected int[] timeOrdinalIndices = new int[0];
  /**
   * dimColNames
   */
  protected String[] dimColNames;
  /**
   *
   */
  protected String[] noDictionaryCols;
  /**
   *
   */
  protected Map<String, String> dimColDataTypes;
  /**
   * measureColumn
   */
  protected String[] measureColumn;
  /**
   * column data types
   */
  protected String[] dimColsDataType;
  /**
   * array of carbon measures
   */
  protected CarbonMeasure[] carbonMeasures;
  /**
   * msrMapping
   */
  protected int[] msrMapping;
  /**
   * dimMapping
   */
  protected int[] dimMapping;
  /**
   * dims
   */
  protected boolean[] dimPresent;
  protected int normLength;
  protected List<String> normHierList;
  /**
   * actualDimArrays
   */
  protected String[] actualDimArray;
  /**
   * mrsAggregators
   */
  protected String[] msrAggregators;
  /**
   * columnAndTableName_ColumnMapForAgg
   */
  protected Map<String, String> columnAndTableNameColumnMapForAggMap;
  /**
   * denormColumnList
   */
  protected List<String> denormColumnList;
  /**
   * Member Mapping
   */
  protected int[] memberMapping;
  protected Map<String, String> foreignKeyPrimaryKeyMap;
  /**
   * carbondim
   */
  private String carbondim;
  /**
   * carbonProps
   */
  private String carbonProps;
  /**
   * carbonmsr
   */
  private String carbonmsr;
  /**
   * carbonhier
   */
  private String carbonhier;
  /**
   * carbonMeasureNames
   */
  private String carbonMeasureNames;
  /**
   * carbonhierColumn
   */
  private String carbonhierColumn;
  /**
   * carbonTime
   */
  private String carbonTime;
  private String noDictionaryDims;
  /**
   * carbonSchema
   */
  private String carbonSchema;
  /**
   * batchSize
   */
  private int batchSize = 10000;
  /**
   * isAggregate
   */
  private boolean isAggregate;
  /**
   * generateDimFiles
   */
  private boolean generateDimFiles;
  /**
   * storeType
   */
  private String storeType;
  /**
   * metaHeirSQLQuery
   */
  private String metaHeirSQLQuery;
  /**
   * carbonMetaHier
   */
  private String carbonMetaHier;
  /**
   * Foreign key and respective hierarchy String
   */
  private String foreignKeyHierarchyString;
  /**
   * heirNadDimsLensString
   */
  private String heirNadDimsLensString;
  /**
   * measureDataType
   */
  private String measureDataType;
  /**
   * measureSurrogateRequired
   */
  private Map<String, Boolean> measureSurrogateRequired;
  private String heirKeySize;
  /**
   * checkPointFileExits
   */
  private String complexDelimiterLevel1;
  private String complexDelimiterLevel2;
  private String complexTypeString;

  private String columnPropertiesString;

  private String[] complexTypeColumns;
  /**
   * Primary Key String
   */
  private String primaryKeysString;
  /**
   * foreign key Column name string
   */
  private String forgienKeyPrimayKeyString;
  /**
   * Primary Key Map.
   */
  private Map<String, Boolean> primaryKeyMap;
  /**
   *
   */
  private Map<String, String> hierDimTableMap;
  /**
   * propColumns
   */
  private List<String>[] propColumns;
  /**
   * propTypes
   */
  private List<String>[] propTypes;
  /**
   * propIndxs
   */
  private int[][] propIndxs;
  /**
   * metahierVoList
   */
  private List<HierarchiesInfo> metahierVoList;
  /**
   * dimesionTableNames
   */
  private String dimesionTableNames;
  /**
   * column Ids of dimensions in a table
   */
  private String dimensionColumnIds;
  /**
   * dimTableArray
   */
  private String[] dimTableArray;
  /**
   * tableName
   */
  private String tableName;
  /**
   * MOdified Dimension
   */
  private String[] modifiedDimension;
  /**
   * actualDimNames
   */
  private String actualDimNames;
  private String normHiers;
  /**
   * msrAggregatorString
   */
  private String msrAggregatorString;
  /**
   * columnAndTableName_ColumnMapForAggString
   */
  private String columnAndTableNameColumnMapForAggString;
  private String connectionURL;
  private String driverClass;
  private String userName;
  private String password;
  /**
   * denormColumNames
   */
  private String denormColumNames;

  /**
   * databaseName
   */
  private String databaseName;
  /**
   * partitionID
   */
  private String partitionID;

  /**
   * Id of the load folder
   */
  private String segmentId;

  /***
   * String of columns ordinal and column datatype separated by HASH_SPC_CHARACTER
   */
  private String columnSchemaDetails;

  /**
   * String of key value pair separated by , and HASH_SPC_CHARACTER
   */
  private String tableOption;

  /**
   * wrapper object having the columnSchemaDetails
   */
  private ColumnSchemaDetailsWrapper columnSchemaDetailsWrapper;

  /**
   * Wrapper object holding the table options
   */
  private TableOptionWrapper tableOptionWrapper;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * column data type string.
   */
  private String columnsDataTypeString;

  public CarbonCSVBasedSeqGenMeta() {
    super();
  }

  public Map<String, GenericDataType> getComplexTypes() {
    return complexTypes;
  }

  public void setComplexTypes(Map<String, GenericDataType> complexTypes) {
    this.complexTypes = complexTypes;
  }

  public String getComplexDelimiterLevel1() {
    return complexDelimiterLevel1;
  }

  public void setComplexDelimiterLevel1(String complexDelimiterLevel1) {
    this.complexDelimiterLevel1 = complexDelimiterLevel1;
  }

  public String getComplexDelimiterLevel2() {
    return complexDelimiterLevel2;
  }

  public void setComplexDelimiterLevel2(String complexDelimiterLevel2) {
    this.complexDelimiterLevel2 = complexDelimiterLevel2;
  }

  public String getComplexTypeString() {
    return complexTypeString;
  }

  public void setComplexTypeString(String complexTypeString) {
    this.complexTypeString = complexTypeString;
  }

  public void setColumnPropertiesString(String columnPropertiesString) {
    this.columnPropertiesString = columnPropertiesString;
  }

  public String[] getComplexTypeColumns() {
    return complexTypeColumns;
  }

  public void setComplexTypeColumns(String[] complexTypeColumns) {
    this.complexTypeColumns = complexTypeColumns;
  }

  public String getCarbonMetaHier() {
    return carbonMetaHier;
  }

  public void setCarbonMetaHier(String carbonMetaHier) {
    this.carbonMetaHier = carbonMetaHier;
  }

  public String getMetaHeirSQLQueries() {
    return metaHeirSQLQuery;
  }

  public void setMetaMetaHeirSQLQueries(String metaHeirSQLQuery) {
    this.metaHeirSQLQuery = metaHeirSQLQuery;
  }

  public boolean isAggregate() {
    return isAggregate;
  }

  public void setAggregate(boolean isAggregate) {
    this.isAggregate = isAggregate;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public String getStoreType() {
    return storeType;
  }

  public void setStoreType(String storeType) {
    this.storeType = storeType;
  }

  public String getCarbonSchema() {
    return carbonSchema;
  }

  public void setCarbonSchema(String carbonSchema) {
    this.carbonSchema = carbonSchema;
  }

  public List<HierarchiesInfo> getMetahierVoList() {
    return metahierVoList;
  }

  public void setMetahierVoList(List<HierarchiesInfo> metahierVoList) {
    this.metahierVoList = metahierVoList;
  }

  public String getCarbonTime() {
    return carbonTime;
  }

  public void setCarbonTime(String carbonTime) {
    this.carbonTime = carbonTime;
  }

  // getters and setters for the step settings

  public String getCarbonProps() {
    return carbonProps;
  }

  public void setCarbonProps(String carbonProps) {
    this.carbonProps = carbonProps;
  }

  public String getCarbonmsr() {
    return carbonmsr;
  }

  public void setCarbonmsr(String carbonmsr) {
    this.carbonmsr = carbonmsr;
  }

  public String getCarbondim() {
    return carbondim;
  }

  public void setCarbondim(String carbondim) {
    this.carbondim = carbondim;
  }

  public String getCarbonHier() {
    return carbonhier;
  }

  public void setCarbonhier(String carbonhier) {
    this.carbonhier = carbonhier;
  }

  /**
   * @return the connectionURL
   */
  public String getConnectionURL() {
    return connectionURL;
  }

  /**
   * @param connectionURL the connectionURL to set
   */
  public void setConnectionURL(String connectionURL) {
    this.connectionURL = connectionURL;
  }

  /**
   * @return the driverClass
   */
  public String getDriverClass() {
    return driverClass;
  }

  //TODO SIMIAN

  /**
   * @param driverClass the driverClass to set
   */
  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
  }

  /**
   * @return the userName
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName the userName to set
   */
  public void setUserName(String userName) {
    this.userName = userName;
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
   * @return Returns the generateDimFiles.
   */
  public boolean isGenerateDimFiles() {
    return generateDimFiles;
  }

  /**
   * @param generateDimFiles The generateDimFiles to set.
   */
  public void setGenerateDimFiles(boolean generateDimFiles) {
    this.generateDimFiles = generateDimFiles;
  }

  /**
   * set sensible defaults for a new step
   *
   * @see StepMetaInterface#setDefault()
   */
  public void setDefault() {
    carbonProps = "";
    carbondim = "";
    carbonmsr = "";
    carbonhier = "";
    carbonTime = "";
    driverClass = "";
    connectionURL = "";
    userName = "";
    password = "";
    carbonSchema = "";
    storeType = "";
    isAggregate = false;
    metaHeirSQLQuery = "";
    carbonMetaHier = "";
    dimesionTableNames = "";
    dimensionColumnIds = "";
    noDictionaryDims = "";
    tableName = "";
    carbonhierColumn = "";
    foreignKeyHierarchyString = "";
    complexTypeString = "";
    columnPropertiesString = "";
    complexDelimiterLevel1 = "";
    complexDelimiterLevel2 = "";
    primaryKeysString = "";
    carbonMeasureNames = "";
    actualDimNames = "";
    normHiers = "";
    msrAggregatorString = "";
    heirKeySize = "";
    heirNadDimsLensString = "";
    measureDataType = "";
    columnAndTableNameColumnMapForAggString = "";
    denormColumNames = "";
    partitionID = "";
    segmentId = "";
    taskNo = "";
    columnSchemaDetails = "";
    columnsDataTypeString="";
    tableOption = "";
  }

  // helper method to allocate the arrays
  public void allocate(int nrkeys) {

  }

  public String getXML() throws KettleValueException {
    StringBuffer retval = new StringBuffer(150);
    retval.append("    ").append(XMLHandler.addTagValue("carbonProps", carbonProps));
    retval.append("    ").append(XMLHandler.addTagValue("dim", carbondim));
    retval.append("    ").append(XMLHandler.addTagValue("msr", carbonmsr));
    retval.append("    ").append(XMLHandler.addTagValue("hier", carbonhier));
    retval.append("    ").append(XMLHandler.addTagValue("time", carbonTime));
    retval.append("    ").append(XMLHandler.addTagValue("driverClass", driverClass));
    retval.append("    ").append(XMLHandler.addTagValue("connectionURL", connectionURL));
    retval.append("    ").append(XMLHandler.addTagValue("userName", userName));
    retval.append("    ").append(XMLHandler.addTagValue("password", password));
    retval.append("    ").append(XMLHandler.addTagValue("batchSize", batchSize));
    retval.append("    ").append(XMLHandler.addTagValue("genDimFiles", generateDimFiles));
    retval.append("    ").append(XMLHandler.addTagValue("isAggregate", isAggregate));
    retval.append("    ").append(XMLHandler.addTagValue("storeType", storeType));
    retval.append("    ").append(XMLHandler.addTagValue("metadataFilePath", metaHeirSQLQuery));
    retval.append("    ").append(XMLHandler.addTagValue("carbonMetaHier", carbonMetaHier));
    retval.append("    ")
        .append(XMLHandler.addTagValue("foreignKeyHierarchyString", foreignKeyHierarchyString));
    retval.append("    ").append(XMLHandler.addTagValue("complexTypeString", complexTypeString));
    retval.append("    ")
        .append(XMLHandler.addTagValue("columnPropertiesString", columnPropertiesString));
    retval.append("    ")
        .append(XMLHandler.addTagValue("complexDelimiterLevel1", complexDelimiterLevel1));
    retval.append("    ")
        .append(XMLHandler.addTagValue("complexDelimiterLevel2", complexDelimiterLevel2));
    retval.append("    ").append(XMLHandler.addTagValue("primaryKeysString", primaryKeysString));
    retval.append("    ").append(XMLHandler.addTagValue("carbonMeasureNames", carbonMeasureNames));
    retval.append("    ").append(XMLHandler.addTagValue("actualDimNames", actualDimNames));
    retval.append("    ")
        .append(XMLHandler.addTagValue("msrAggregatorString", msrAggregatorString));

    retval.append("    ").append(XMLHandler.addTagValue("dimHierReleation", dimesionTableNames));
    retval.append("    ").append(XMLHandler.addTagValue("dimensionColumnIds", dimensionColumnIds));
    retval.append("    ").append(XMLHandler.addTagValue("dimNoDictionary", noDictionaryDims));
    retval.append("    ").append(XMLHandler.addTagValue("dimColDataTypes", columnsDataTypeString));
    retval.append("    ").append(XMLHandler.addTagValue("factOrAggTable", tableName));
    retval.append("    ").append(XMLHandler.addTagValue("carbonhierColumn", carbonhierColumn));
    retval.append("    ").append(XMLHandler.addTagValue("normHiers", normHiers));
    retval.append("    ").append(XMLHandler.addTagValue("heirKeySize", heirKeySize));

    retval.append("    ")
        .append(XMLHandler.addTagValue("forgienKeyPrimayKeyString", forgienKeyPrimayKeyString));
    retval.append("    ")
        .append(XMLHandler.addTagValue("heirNadDimsLensString", heirNadDimsLensString));
    retval.append("    ").append(XMLHandler.addTagValue("measureDataType", measureDataType));
    retval.append("    ").append(XMLHandler.addTagValue("columnAndTableName_ColumnMapForAggString",
        columnAndTableNameColumnMapForAggString));
    retval.append("    ").append(XMLHandler.addTagValue("databaseName", databaseName));
    retval.append("    ").append(XMLHandler.addTagValue("tableName", tableName));
    retval.append("    ").append(XMLHandler.addTagValue("denormColumNames", denormColumNames));
    retval.append("    ").append(XMLHandler.addTagValue("partitionID", partitionID));
    retval.append("    ").append(XMLHandler.addTagValue("segmentId", segmentId));
    retval.append("    ").append(XMLHandler.addTagValue("taskNo", taskNo));
    retval.append("    ")
        .append(XMLHandler.addTagValue("columnSchemaDetails", columnSchemaDetails));
    retval.append("    ")
        .append(XMLHandler.addTagValue("tableOption", tableOption));
    return retval.toString();
  }

  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {

    try {

      carbonProps = XMLHandler.getTagValue(stepnode, "carbonProps");
      carbondim = XMLHandler.getTagValue(stepnode, "dim");
      carbonmsr = XMLHandler.getTagValue(stepnode, "msr");
      carbonhier = XMLHandler.getTagValue(stepnode, "hier");
      carbonTime = XMLHandler.getTagValue(stepnode, "time");
      driverClass = XMLHandler.getTagValue(stepnode, "driverClass");
      connectionURL = XMLHandler.getTagValue(stepnode, "connectionURL");
      userName = XMLHandler.getTagValue(stepnode, "userName");
      password = XMLHandler.getTagValue(stepnode, "password");
      carbonMetaHier = XMLHandler.getTagValue(stepnode, "carbonMetaHier");
      carbonhierColumn = XMLHandler.getTagValue(stepnode, "carbonhierColumn");
      foreignKeyHierarchyString = XMLHandler.getTagValue(stepnode, "foreignKeyHierarchyString");
      complexTypeString = XMLHandler.getTagValue(stepnode, "complexTypeString");
      columnPropertiesString = XMLHandler.getTagValue(stepnode, "columnPropertiesString");
      complexDelimiterLevel1 = XMLHandler.getTagValue(stepnode, "complexDelimiterLevel1");
      complexDelimiterLevel2 = XMLHandler.getTagValue(stepnode, "complexDelimiterLevel2");
      primaryKeysString = XMLHandler.getTagValue(stepnode, "primaryKeysString");
      carbonMeasureNames = XMLHandler.getTagValue(stepnode, "carbonMeasureNames");
      actualDimNames = XMLHandler.getTagValue(stepnode, "actualDimNames");
      normHiers = XMLHandler.getTagValue(stepnode, "normHiers");
      msrAggregatorString = XMLHandler.getTagValue(stepnode, "msrAggregatorString");
      heirKeySize = XMLHandler.getTagValue(stepnode, "heirKeySize");
      forgienKeyPrimayKeyString = XMLHandler.getTagValue(stepnode, "forgienKeyPrimayKeyString");
      heirNadDimsLensString = XMLHandler.getTagValue(stepnode, "heirNadDimsLensString");
      measureDataType = XMLHandler.getTagValue(stepnode, "measureDataType");
      columnAndTableNameColumnMapForAggString =
          XMLHandler.getTagValue(stepnode, "columnAndTableName_ColumnMapForAggString");
      dimesionTableNames = XMLHandler.getTagValue(stepnode, "dimHierReleation");
      dimensionColumnIds = XMLHandler.getTagValue(stepnode, "dimensionColumnIds");
      noDictionaryDims = XMLHandler.getTagValue(stepnode, "dimNoDictionary");
      columnsDataTypeString = XMLHandler.getTagValue(stepnode, "dimColDataTypes");
      tableName = XMLHandler.getTagValue(stepnode, "factOrAggTable");
      tableName = XMLHandler.getTagValue(stepnode, "tableName");
      databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      denormColumNames = XMLHandler.getTagValue(stepnode, "denormColumNames");
      partitionID = XMLHandler.getTagValue(stepnode, "partitionID");
      segmentId = XMLHandler.getTagValue(stepnode, "segmentId");
      taskNo = XMLHandler.getTagValue(stepnode, "taskNo");
      columnSchemaDetails = XMLHandler.getTagValue(stepnode, "columnSchemaDetails");
      tableOption = XMLHandler.getTagValue(stepnode, "tableOption");
      String batchConfig = XMLHandler.getTagValue(stepnode, "batchSize");

      if (batchConfig != null) {
        batchSize = Integer.parseInt(batchConfig);
      }

      String dimeFileConfig = XMLHandler.getTagValue(stepnode, "genDimFiles");
      if (dimeFileConfig != null) {
        generateDimFiles = Boolean.parseBoolean(dimeFileConfig);
      }

      storeType = XMLHandler.getTagValue(stepnode, "storeType");
      metaHeirSQLQuery = XMLHandler.getTagValue(stepnode, "metadataFilePath");

      isAggregate = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "isAggregate"));

      int nrKeys = XMLHandler.countNodes(stepnode, "lookup");
      allocate(nrKeys);

    } catch (Exception e) {
      throw new KettleXMLException("Template Plugin Unable to read step info from XML node", e);
    }

  }

  public void initialize() throws KettleException {
    this.columnSchemaDetailsWrapper = new ColumnSchemaDetailsWrapper(columnSchemaDetails);
    this.tableOptionWrapper = TableOptionWrapper.getTableOptionWrapperInstance();
    tableOptionWrapper.populateTableOptions(tableOption);

    updateDimensions(carbondim, carbonmsr, noDictionaryDims);
    dimColDataTypes=RemoveDictionaryUtil.extractDimColsDataTypeValues(columnsDataTypeString);
    if (null != complexTypeString) {
      complexTypes = getComplexTypesMap(complexTypeString);
    } else {
      complexTypeColumns = new String[0];
    }

    if (null != columnPropertiesString) {
      updateColumnPropertiesMap(columnPropertiesString);
    }
    hirches = getHierarichies(carbonhier);

    hierColumnMap = getHierarchiesColumnMap(carbonhierColumn);

    foreignKeyHierarchyMap = getForeignKeyHierMap(foreignKeyHierarchyString);

    primaryKeyMap = updatePrimaryKeyMap(primaryKeysString);

    foreignKeyPrimaryKeyMap = getForeignKeyColumnNameMap(forgienKeyPrimayKeyString);

    actualDimArray = getActualDimensionArray(actualDimNames);

    normHierList = getNormHierList(normHiers);

    //update non time dimension properties
    updateDimProperties();

    //update the meta Hierarichies list
    getMetaHierarichies(carbonMetaHier);

    updateMetaHierarichiesWithQueries(metaHeirSQLQuery);

    updateMeasureAggregator(msrAggregatorString);

    measureSurrogateRequired = getMeasureDatatypeMap(measureDataType);

    updateHierDimTableMap(dimesionTableNames);

    if (isAggregate) {
      columnAndTableNameColumnMapForAggMap =
          new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      String[] split =
          columnAndTableNameColumnMapForAggString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
      for (int i = 0; i < split.length; i++) {
        String[] split2 = split[i].split(CarbonCommonConstants.HYPHEN_SPC_CHARACTER);
        columnAndTableNameColumnMapForAggMap.put(split2[0], split2[1]);
      }
    }

    updateDenormColunList(denormColumNames);
  }

  private void updateColumnPropertiesMap(String columnPropertiesString) {
    String[] colsProperty = columnPropertiesString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
    for (String property : colsProperty) {
      String[] colKeyVals = property.split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      String colName = colKeyVals[0];
      Map<String, String> colPropMap = new HashMap<>();
      String[] keyVals = colKeyVals[1].split(CarbonCommonConstants.COMA_SPC_CHARACTER);
      for (int i = 0; i < keyVals.length; i++) {
        String[] keyVal = keyVals[i].split(CarbonCommonConstants.HYPHEN_SPC_CHARACTER);
        String key = keyVal[0];
        String value = keyVal[1];
        colPropMap.put(key, value);
      }
      columnProperties.put(colName, colPropMap);
    }
  }

  private void updateDenormColunList(String denormColumNames) {
    //
    if (null == denormColumNames || "".equals(denormColumNames)) {
      denormColumnList = new ArrayList<String>(1);
      return;
    }

    String[] columnNames = denormColumNames.split(CarbonCommonConstants.HASH_SPC_CHARACTER);

    if (null == denormColumnList) {
      denormColumnList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    }

    for (String colName : columnNames) {
      denormColumnList.add(colName);
    }
  }

  private void updateHierDimTableMap(String dimesionTableNames) {
    if (null == dimesionTableNames || "".equals(dimesionTableNames)) {
      return;
    }

    String[] hierTableName =
        dimesionTableNames.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    if (null == hierDimTableMap) {
      hierDimTableMap = new HashMap<String, String>(hierTableName.length);
    }
    if (null == dimTableArray) {
      dimTableArray = new String[hierTableName.length];
    }
    int i = 0;
    for (String hierTable : hierTableName) {
      String[] hierAndTable = hierTable.split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      hierDimTableMap.put(hierAndTable[0], hierAndTable[1]);
      dimTableArray[i++] = hierAndTable[1];
    }
  }

  private Map<String, Boolean> getMeasureDatatypeMap(String measureDataType) {
    return new HashMap<String, Boolean>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  private void updateMeasureAggregator(String msrAggregatorString) {
    String[] split = msrAggregatorString.split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
    msrAggregators = new String[split.length];
    System.arraycopy(split, 0, msrAggregators, 0, split.length);
  }

  private String[] getActualDimensionArray(String actualDimNames) {
    if (actualDimNames == null || "".equals(actualDimNames)) {
      return new String[0];
    }

    return actualDimNames.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

  }

  private List<String> getNormHierList(String normHier) {
    List<String> hierList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    if (null != normHier && normHier.length() != 0) {
      String[] split = normHier.split(CarbonCommonConstants.COMA_SPC_CHARACTER);

      for (int i = 0; i < split.length; i++) {
        hierList.add(split[i]);
      }
    }
    return hierList;
  }

  private Map<String, String> getForeignKeyColumnNameMap(String foreignKeyColumnNameString) {
    if (foreignKeyColumnNameString == null || "".equals(foreignKeyColumnNameString)) {
      return new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    Map<String, String> map =
        new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String[] foreignKeys =
        foreignKeyColumnNameString.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    for (int i = 0; i < foreignKeys.length; i++) {
      String[] foreignHierArray = foreignKeys[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      String hiers = map.get(foreignHierArray[0]);

      if (null == hiers) {
        map.put(foreignHierArray[0], foreignHierArray[1]);
      } else {
        map.put(foreignHierArray[0],
            hiers + CarbonCommonConstants.COMA_SPC_CHARACTER + foreignHierArray[1]);
      }
    }
    return map;
  }

  private Map<String, Boolean> updatePrimaryKeyMap(String primaryKeysString) {
    if (primaryKeysString == null || "".equals(primaryKeysString)) {
      return new HashMap<String, Boolean>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    Map<String, Boolean> resultMap =
        new HashMap<String, Boolean>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String[] primaryKeys = primaryKeysString.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    for (int i = 0; i < primaryKeys.length; i++) {
      resultMap.put(primaryKeys[i], true);
    }
    return resultMap;
  }

  public void updateHierMappings(RowMetaInterface metaInterface) {
    List<String> actualHierList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int j = 0; j < metaInterface.size(); j++) {
      String foreignKey = metaInterface.getValueMeta(j).getName();
      String actualHier = foreignKeyHierarchyMap.get(foreignKey);
      if (null != actualHier) {
        if (actualHier.contains(CarbonCommonConstants.COMA_SPC_CHARACTER)) {
          String[] splitHier = actualHier.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
          for (String hier : splitHier) {
            actualHierList.add(hier);
          }
        } else {
          actualHierList.add(actualHier);
        }
      }
    }

    hierNames = new String[actualHierList.size()];
    hierNames = actualHierList.toArray(new String[actualHierList.size()]);
  }

  private Map<String, GenericDataType> getComplexTypesMap(String complexTypeString) {
    Map<String, GenericDataType> complexTypesMap = new LinkedHashMap<String, GenericDataType>();
    String[] hierarchies = complexTypeString.split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
    complexTypeColumns = new String[hierarchies.length];
    for (int i = 0; i < hierarchies.length; i++) {
      String[] levels = hierarchies[i].split(CarbonCommonConstants.HASH_SPC_CHARACTER);
      String[] levelInfo = levels[0].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      GenericDataType g = levelInfo[1].equals(CarbonCommonConstants.ARRAY) ?
          new ArrayDataType(levelInfo[0], "", levelInfo[3]) :
          new StructDataType(levelInfo[0], "", levelInfo[3]);
      complexTypesMap.put(levelInfo[0], g);
      complexTypeColumns[i] = levelInfo[0];
      for (int j = 1; j < levels.length; j++) {
        levelInfo = levels[j].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
        switch (levelInfo[1]) {
          case CarbonCommonConstants.ARRAY:
            g.addChildren(new ArrayDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
            break;
          case CarbonCommonConstants.STRUCT:
            g.addChildren(new StructDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
            break;
          default:
            g.addChildren(new PrimitiveDataType(levelInfo[0], levelInfo[2], levelInfo[3],
                Integer.parseInt(levelInfo[4])));
        }
      }
    }
    return complexTypesMap;
  }

  private Map<String, String> getForeignKeyHierMap(String foreignKeyHierarchyString) {
    if (foreignKeyHierarchyString == null || "".equals(foreignKeyHierarchyString)) {
      return new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    Map<String, String> map =
        new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String[] hies = foreignKeyHierarchyString.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    for (int i = 0; i < hies.length; i++) {
      String[] foreignHierArray = hies[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      String hiers = map.get(foreignHierArray[0]);

      if (null == hiers) {
        map.put(foreignHierArray[0], foreignHierArray[1]);
      } else {
        map.put(foreignHierArray[0],
            hiers + CarbonCommonConstants.COMA_SPC_CHARACTER + foreignHierArray[1]);
      }

    }
    return map;
  }

  private Map<String, String[]> getHierarchiesColumnMap(String carbonhierColumn) {
    if (carbonhierColumn == null || "".equals(carbonhierColumn)) {
      return new HashMap<String, String[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    Map<String, String[]> map =
        new HashMap<String, String[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String[] hies = carbonhierColumn.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    for (int i = 0; i < hies.length; i++) {
      String hie = hies[i];

      String hierName = hie.substring(0, hie.indexOf(CarbonCommonConstants.COLON_SPC_CHARACTER));

      String[] columnArray = getStringArray(hie.substring(
          hie.indexOf(CarbonCommonConstants.COLON_SPC_CHARACTER)
              + CarbonCommonConstants.COLON_SPC_CHARACTER.length(), hie.length()));
      map.put(hierName, columnArray);
    }
    return map;
  }

  private String[] getStringArray(String columnNames) {
    String[] splitedColumnNames = columnNames.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
    String[] columns = new String[splitedColumnNames.length];

    System.arraycopy(splitedColumnNames, 0, columns, 0, columns.length);
    return columns;
  }

  private void getMetaHierarichies(String carbonMetaHier) {
    if (null == carbonMetaHier || "".equals(carbonMetaHier)) {
      return;
    }
    String[] metaHier = carbonMetaHier.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    metahierVoList = new ArrayList<HierarchiesInfo>(metaHier.length);
    Map<String, String[]> columnPropsMap =
        new HashMap<String, String[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < metaHier.length; i++) {
      HierarchiesInfo hierarichiesVo = new HierarchiesInfo();
      String[] isTimeDim = metaHier[i].split(CarbonCommonConstants.HASH_SPC_CHARACTER);
      String[] split = isTimeDim[0].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      String[] columnNames = new String[split.length - 1];
      int[] columnIndex = new int[split.length - 1];
      hierarichiesVo.setHierarichieName(split[0]);
      if (null != hirches.get(split[0])) {
        hierarichiesVo.setLoadToHierarichiTable(true);
      }
      int index = 0;
      for (int j = 1; j < split.length; j++) {
        String[] columnAndPropertyNames = split[j].split(
            CarbonCommonConstants.COMA_SPC_CHARACTER);//CHECKSTYLE:OFF    Approval No:Approval-323
        columnNames[index] = columnAndPropertyNames[0];//CHECKSTYLE:ON
        columnIndex[index] = getColumnIndex(columnNames[index]);
        String[] properties = new String[columnAndPropertyNames.length - 1];
        System
            .arraycopy(columnAndPropertyNames, 1, properties, 0, columnAndPropertyNames.length - 1);
        if (null == columnPropsMap.get(columnNames[index])) {
          columnPropsMap.put(columnNames[index], properties);
        }
        index++;
      }
      hierarichiesVo.setColumnIndex(columnIndex);
      hierarichiesVo.setColumnNames(columnNames);
      hierarichiesVo.setColumnPropMap(columnPropsMap);
      metahierVoList.add(hierarichiesVo);
    }
  }

  private void updateMetaHierarichiesWithQueries(String carbonLocation) {
    //
    if (null == carbonLocation) {
      return;
    }
    String[] hierWithQueries = carbonLocation.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
    //
    for (String hierarchyWithQuery : hierWithQueries) {
      String[] hierQueryStrings =
          hierarchyWithQuery.split(CarbonCommonConstants.COLON_SPC_CHARACTER);

      Iterator<HierarchiesInfo> iterator = metahierVoList.iterator();
      while (iterator.hasNext()) {
        //
        HierarchiesInfo next = iterator.next();
        if (hierQueryStrings[0].equalsIgnoreCase(next.getHierarichieName())) {
          next.setQuery(hierQueryStrings[1]);
          break;
        }

      }
    }

  }

  private int getColumnIndex(String colNames) {
    for (int j = 0; j < dimColNames.length; j++) {
      if (dimColNames[j].equalsIgnoreCase(colNames)) {
        return j;
      }
    }
    return -1;
  }

  /**
   * Parse the properties string.
   * Level Entries separated by '&'
   * Level and prop details separated by ':'
   * Property column name and index separated by ','
   * Level:p1,index1:p2,index2&Level2....
   */
  private void updateDimProperties() {
    Map<String, int[]> indices =
        new HashMap<String, int[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Map<String, String[]> columns =
        new HashMap<String, String[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Map<String, String[]> dbTypes =
        new HashMap<String, String[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    if (carbonProps != null && !"".equals(carbonProps)) {
      String[] entries = carbonProps.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
      for (int i = 0; i < entries.length; i++) {
        String[] levelEntry = entries[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
        String dimColumnName = levelEntry[0];
        int[] pIndices = new int[levelEntry.length - 1];
        String[] cols = new String[levelEntry.length - 1];
        String[] dbType = new String[levelEntry.length - 1];
        for (int j = 1; j < levelEntry.length; j++) {
          String[] propEntry = levelEntry[j].split(CarbonCommonConstants.COMA_SPC_CHARACTER);
          pIndices[j - 1] = Integer.parseInt(propEntry[1]);

          cols[j - 1] = propEntry[0];
          dbType[j - 1] = propEntry[2];
        }

        indices.put(dimColumnName, pIndices);
        columns.put(dimColumnName, cols);
        dbTypes.put(dimColumnName, dbType);
      }
    }

    if (indices.isEmpty()) {
      return;
    }

    propColumns = new List[dimColNames.length];
    propTypes = new List[dimColNames.length];
    propIndxs = new int[dimColNames.length][];

    //Fill the property details based on the map created
    for (int i = 0; i < dimColNames.length; i++) {
      //Properties present or not
      if (indices.containsKey(dimColNames[i])) {
        propColumns[i] = Arrays.asList(columns.get(dimColNames[i]));
        propTypes[i] = Arrays.asList(dbTypes.get(dimColNames[i]));
        propIndxs[i] = indices.get(dimColNames[i]);
      } else {
        propColumns[i] = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        propTypes[i] = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        propIndxs[i] = new int[0];
      }
    }
  }

  private Map<String, int[]> getHierarichies(String ds) {
    if (ds == null || "".equals(ds)) {
      return new HashMap<String, int[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    Map<String, int[]> map =
        new HashMap<String, int[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String[] hies = ds.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    for (int i = 0; i < hies.length; i++) {
      String hie = hies[i];

      String name = hie.substring(0, hie.indexOf(CarbonCommonConstants.COLON_SPC_CHARACTER));

      int[] a = getIntArray(hie.substring(hie.indexOf(CarbonCommonConstants.COLON_SPC_CHARACTER)
          + CarbonCommonConstants.COLON_SPC_CHARACTER.length(), hie.length()));
      map.put(name, a);
    }
    return map;
  }

  private int[] getIntArray(String ds) {

    String[] sp = ds.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
    int[] a = new int[sp.length];

    for (int i = 0; i < a.length; i++) {
      a[i] = Integer.parseInt(sp[i]);
    }
    return a;

  }

  private void updateDimensions(String ds, String msr, String noDictionaryDims) {
    String[] sp = null;
    if (null != ds) {
      sp = ds.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
    } else {
      sp = new String[0];
    }
    int[] dimsLocal = new int[sp.length];
    int[] lens = new int[sp.length];
    List<String> list = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    dimPresent = new boolean[sp.length];

    for (int i = 0; i < dimsLocal.length; i++) {
      String[] dim = sp[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      list.add(dim[0]);
      dimsLocal[i] = Integer.parseInt(dim[1]);
      lens[i] = Integer.parseInt(dim[2]);

      if ("Y".equals(dim[3])) {
        dimPresent[i] = true;
        normLength++;
      }
    }
    dims = dimsLocal;
    dimLens = lens;
    dimColNames = list.toArray(new String[list.size()]);

    // get high cardinality dimension Array
    noDictionaryCols = RemoveDictionaryUtil.extractNoDictionaryDimsArr(noDictionaryDims);

    String[] sm = msr.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
    int[] m = new int[sm.length];
    Set<String> mlist = new LinkedHashSet<String>();
    for (int i = 0; i < m.length; i++) {
      String[] ms = sm[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      mlist.add(ms[0]);
      m[i] = Integer.parseInt(ms[1]);
    }
    msrs = m;
    measureColumn = mlist.toArray(new String[mlist.size()]);
  }

  public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleException {
    try {
      //
      carbonProps = rep.getStepAttributeString(idStep, "carbonProps");
      carbonmsr = rep.getStepAttributeString(idStep, "msr");
      carbondim = rep.getStepAttributeString(idStep, "dim");
      carbonhier = rep.getStepAttributeString(idStep, "hier");
      carbonTime = rep.getStepAttributeString(idStep, "time");
      //
      driverClass = rep.getStepAttributeString(idStep, "driverClass");
      connectionURL = rep.getStepAttributeString(idStep, "connectionURL");
      userName = rep.getStepAttributeString(idStep, "userName");
      password = rep.getStepAttributeString(idStep, "password");
      isAggregate = rep.getStepAttributeBoolean(idStep, "isAggregate");
      metaHeirSQLQuery = rep.getStepAttributeString(idStep, "metadataFilePath");
      carbonMetaHier = rep.getStepAttributeString(idStep, "carbonMetaHier");
      carbonhierColumn = rep.getStepAttributeString(idStep, "carbonhierColumn");
      foreignKeyHierarchyString = rep.getStepAttributeString(idStep, "foreignKeyHierarchyString");
      primaryKeysString = rep.getStepAttributeString(idStep, "primaryKeysString");
      carbonMeasureNames = rep.getStepAttributeString(idStep, "carbonMeasureNames");
      actualDimNames = rep.getStepAttributeString(idStep, "actualDimNames");
      msrAggregatorString = rep.getStepAttributeString(idStep, "msrAggregatorString");

      dimesionTableNames = rep.getStepAttributeString(idStep, "dimHierReleation");
      dimensionColumnIds = rep.getStepAttributeString(idStep, "dimensionColumnIds");
      noDictionaryDims = rep.getStepAttributeString(idStep, "dimNoDictionary");
      columnsDataTypeString = rep.getStepAttributeString(idStep, "dimColDataTypes");
      normHiers = rep.getStepAttributeString(idStep, "normHiers");
      tableName = rep.getStepAttributeString(idStep, "factOrAggTable");
      batchSize = Integer.parseInt(rep.getStepAttributeString(idStep, "batchSize"));
      heirKeySize = rep.getStepAttributeString(idStep, "heirKeySize");
      forgienKeyPrimayKeyString = rep.getStepAttributeString(idStep, "forgienKeyPrimayKeyString");
      heirNadDimsLensString = rep.getStepAttributeString(idStep, "heirNadDimsLensString");
      measureDataType = rep.getStepAttributeString(idStep, "measureDataType");
      columnAndTableNameColumnMapForAggString =
          rep.getStepAttributeString(idStep, "columnAndTableName_ColumnMapForAggString");
      databaseName = rep.getStepAttributeString(idStep, "databaseName");

      tableName = rep.getStepAttributeString(idStep, "tableName");
      denormColumNames = rep.getStepAttributeString(idStep, "denormColumNames");
      partitionID = rep.getStepAttributeString(idStep, "partitionID");
      segmentId = rep.getStepAttributeString(idStep, "segmentId");
      taskNo = rep.getStepAttributeString(idStep, "taskNo");
      columnSchemaDetails = rep.getStepAttributeString(idStep, "columnSchemaDetails");
      tableOption = rep.getStepAttributeString(idStep, "tableOption");
      int nrKeys = rep.countNrStepAttributes(idStep, "lookup_keyfield");
      allocate(nrKeys);
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(pkg, "CarbonStep.Exception.UnexpectedErrorInReadingStepInfo"), e);
    }
  }

  public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
      throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, "dim", carbondim);
      rep.saveStepAttribute(idTransformation, idStep, "carbonProps", carbonProps);
      rep.saveStepAttribute(idTransformation, idStep, "msr", carbonmsr);
      rep.saveStepAttribute(idTransformation, idStep, "hier", carbonhier);
      rep.saveStepAttribute(idTransformation, idStep, "carbonhierColumn", carbonhierColumn);
      rep.saveStepAttribute(idTransformation, idStep, "columnAndTableName_ColumnMapForAggString",
          columnAndTableNameColumnMapForAggString);
      rep.saveStepAttribute(idTransformation, idStep, "time", carbonTime);
      rep.saveStepAttribute(idTransformation, idStep, "driverClass", driverClass);
      rep.saveStepAttribute(idTransformation, idStep, "connectionURL", connectionURL);
      rep.saveStepAttribute(idTransformation, idStep, "userName", userName);
      rep.saveStepAttribute(idTransformation, idStep, "password", password);
      rep.saveStepAttribute(idTransformation, idStep, "isInitialLoad", isAggregate);
      rep.saveStepAttribute(idTransformation, idStep, "metadataFilePath", metaHeirSQLQuery);
      rep.saveStepAttribute(idTransformation, idStep, "carbonMetaHier", carbonMetaHier);
      rep.saveStepAttribute(idTransformation, idStep, "batchSize", batchSize);
      rep.saveStepAttribute(idTransformation, idStep, "dimHierReleation", dimesionTableNames);
      rep.saveStepAttribute(idTransformation, idStep, "dimensionColumnIds", dimensionColumnIds);
      rep.saveStepAttribute(idTransformation, idStep, "dimNoDictionary", noDictionaryDims);
      rep.saveStepAttribute(idTransformation, idStep, "dimColDataTypes", columnsDataTypeString);
      rep.saveStepAttribute(idTransformation, idStep, "foreignKeyHierarchyString",
          foreignKeyHierarchyString);
      rep.saveStepAttribute(idTransformation, idStep, "primaryKeysString", primaryKeysString);
      rep.saveStepAttribute(idTransformation, idStep, "carbonMeasureNames", carbonMeasureNames);
      rep.saveStepAttribute(idTransformation, idStep, "actualDimNames", actualDimNames);
      rep.saveStepAttribute(idTransformation, idStep, "normHiers", normHiers);
      rep.saveStepAttribute(idTransformation, idStep, "msrAggregatorString", msrAggregatorString);
      rep.saveStepAttribute(idTransformation, idStep, "heirKeySize", heirKeySize);
      rep.saveStepAttribute(idTransformation, idStep, "forgienKeyPrimayKeyString",
          forgienKeyPrimayKeyString);
      rep.saveStepAttribute(idTransformation, idStep, "factOrAggTable", tableName);
      rep.saveStepAttribute(idTransformation, idStep, "heirNadDimsLensString",
          heirNadDimsLensString);
      rep.saveStepAttribute(idTransformation, idStep, "measureDataType", measureDataType);
      rep.saveStepAttribute(idTransformation, idStep, "databaseName", databaseName);
      rep.saveStepAttribute(idTransformation, idStep, "tableName", tableName);
      rep.saveStepAttribute(idTransformation, idStep, "denormColumNames", denormColumNames);
      rep.saveStepAttribute(idTransformation, idStep, "partitionID", partitionID);
      rep.saveStepAttribute(idTransformation, idStep, "segmentId", segmentId);
      rep.saveStepAttribute(idTransformation, idStep, "taskNo", taskNo);
      rep.saveStepAttribute(idTransformation, idStep, "columnSchemaDetails", columnSchemaDetails);
      rep.saveStepAttribute(idTransformation, idStep, "tableOption", tableOption);
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(pkg, "CarbonStep.Exception.UnableToSaveStepInfoToRepository")
              + idStep, e);
    }
  }

  public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta,
      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
    CarbonDataProcessorUtil.check(pkg, remarks, stepMeta, prev, input);
  }

  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
      TransMeta transMeta, Trans disp) {
    return new CarbonCSVBasedSeqGenStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
  }

  public StepDataInterface getStepData() {
    return new CarbonCSVBasedSeqGenData();
  }

  public List<String>[] getPropertiesColumns() {
    return propColumns;
  }

  public int[][] getPropertiesIndices() {
    return propIndxs;
  }

  public List<String>[] getPropTypes() {
    return propTypes;
  }

  public String getTableNames() {
    return dimesionTableNames;
  }

  public void setTableNames(String dimHierReleation) {
    this.dimesionTableNames = dimHierReleation;
  }

  /**
   * @return column Ids
   */
  public String[] getDimensionColumnIds() {
    return null != dimensionColumnIds ?
        dimensionColumnIds.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER) :
        new String[0];
  }

  /**
   * @param dimensionColumnIds column Ids for dimensions in a table
   */
  public void setDimensionColumnIds(String dimensionColumnIds) {
    this.dimensionColumnIds = dimensionColumnIds;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String[] getModifiedDimension() {
    return modifiedDimension;
  }

  public void setModifiedDimension(String[] modifiedDimension) {
    this.modifiedDimension = modifiedDimension;
  }

  public void setCarbonhierColumn(String carbonhierColumn) {
    this.carbonhierColumn = carbonhierColumn;
  }

  public void setForeignKeyHierarchyString(String foreignKeyHierarchyString) {
    this.foreignKeyHierarchyString = foreignKeyHierarchyString;
  }

  public void setPrimaryKeysString(String primaryKeysString) {
    this.primaryKeysString = primaryKeysString;
  }

  public Map<String, Boolean> getPrimaryKeyMap() {
    return primaryKeyMap;
  }

  public String getCarbonMeasureNames() {
    return carbonMeasureNames;
  }

  public void setCarbonMeasureNames(String carbonMeasureNames) {
    this.carbonMeasureNames = carbonMeasureNames;
  }

  public String getActualDimNames() {
    return actualDimNames;
  }

  public void setActualDimNames(String actualDimNames) {
    this.actualDimNames = actualDimNames;
  }

  public String getNormHiers() {
    return normHiers;
  }

  public void setNormHiers(String normHiers) {
    this.normHiers = normHiers;
  }

  public String getMsrAggregatorString() {
    return msrAggregatorString;
  }

  public void setMsrAggregatorString(String msrAggregatorString) {
    this.msrAggregatorString = msrAggregatorString;
  }

  public String getHeirKeySize() {
    return heirKeySize;
  }

  public void setHeirKeySize(String heirKeySize) {
    this.heirKeySize = heirKeySize;
  }

  public String getForgienKeyPrimayKeyString() {
    return forgienKeyPrimayKeyString;
  }

  public void setForgienKeyPrimayKeyString(String forgienKeyPrimayKeyString) {
    this.forgienKeyPrimayKeyString = forgienKeyPrimayKeyString;
  }

  public String getHeirNadDimsLensString() {
    return heirNadDimsLensString;
  }

  public void setHeirNadDimsLensString(String heirNadDimsLensString) {
    this.heirNadDimsLensString = heirNadDimsLensString;
  }

  public String getMeasureDataType() {
    return measureDataType;
  }

  public void setMeasureDataType(String measureDataType) {
    this.measureDataType = measureDataType;
  }

  public Map<String, Boolean> getMeasureSurrogateRequired() {
    return measureSurrogateRequired;
  }

  public void setMeasureSurrogateRequired(Map<String, Boolean> measureSurrogateRequired) {
    this.measureSurrogateRequired = measureSurrogateRequired;
  }

  public Map<String, String> getHierDimTableMap() {
    return hierDimTableMap;
  }

  public String[] getDimTableArray() {
    return dimTableArray;
  }

  public String getColumnAndTableNameColumnMapForAggString() {
    return columnAndTableNameColumnMapForAggString;
  }

  public void setColumnAndTableNameColumnMapForAggString(
      String columnAndTableNameColumnMapForAggString) {
    this.columnAndTableNameColumnMapForAggString = columnAndTableNameColumnMapForAggString;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getDenormColumNames() {
    return denormColumNames;
  }

  public void setDenormColumNames(String denormColumNames) {
    this.denormColumNames = denormColumNames;
  }

  public String getNoDictionaryDims() {
    return noDictionaryDims;
  }

  public void setNoDictionaryDims(String noDictionaryDims) {
    this.noDictionaryDims = noDictionaryDims;
  }

  /**
   * @return columngroups
   */
  public String getDimensionColumnsDataType() {
    return columnsDataTypeString;
  }

  /**
   * @param columnsDataTypeString
   */
  public void setDimensionColumnsDataType(String columnsDataTypeString) {
    this.columnsDataTypeString = columnsDataTypeString;

  }

  /**
   * @return partitionId
   */
  public String getPartitionID() {
    return partitionID;
  }

  /**
   * @param partitionID
   */
  public void setPartitionID(String partitionID) {
    this.partitionID = partitionID;
  }

  /**
   * set the the serialized String of columnSchemaDetails
   *
   * @param columnSchemaDetails
   */
  public void setColumnSchemaDetails(String columnSchemaDetails) {
    this.columnSchemaDetails = columnSchemaDetails;
  }

  /**
   * return segmentId
   *
   * @return
   */
  public int getSegmentId() {
    return Integer.parseInt(segmentId);
  }

  /**
   * set segment Id
   *
   * @param segmentId
   */
  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  /**
   * @param taskNo
   */
  public void setTaskNo(String taskNo) {
    this.taskNo = taskNo;
  }

  /**
   * @return
   */
  public String getTaskNo() {
    return taskNo;
  }

  public Map<String, Map<String, String>> getColumnPropertiesMap() {
    return columnProperties;
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
   * the method set the TableOption details
   * @param tableOption
   */
  public void setTableOption(String tableOption) {
    this.tableOption = tableOption;
  }

  /**
   * the method returns the wrapper object of tableoption
   * @return
   */
  public TableOptionWrapper getTableOptionWrapper() {
    return tableOptionWrapper;
  }
}

