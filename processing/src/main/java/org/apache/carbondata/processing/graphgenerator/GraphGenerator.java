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

package org.apache.carbondata.processing.graphgenerator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.api.dataloader.DataLoadModel;
import org.apache.carbondata.processing.api.dataloader.SchemaInfo;
import org.apache.carbondata.processing.csvreaderstep.BlockDetails;
import org.apache.carbondata.processing.csvreaderstep.CsvInputMeta;
import org.apache.carbondata.processing.graphgenerator.configuration.GraphConfigurationInfo;
import org.apache.carbondata.processing.mdkeygen.MDKeyGenStepMeta;
import org.apache.carbondata.processing.merger.step.CarbonSliceMergerStepMeta;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.schema.metadata.TableOptionWrapper;
import org.apache.carbondata.processing.sortandgroupby.sortdatastep.SortKeyStepMeta;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedSeqGenMeta;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.CarbonSchemaParser;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.hadoopfileinput.HadoopFileInputMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

public class GraphGenerator {

  public static final HashMap<String, BlockDetails[]> blockInfo = new HashMap<>();
  /**
   * DEFAUL_BLOCKLET_SIZE
   */
  private static final String DEFAUL_BLOCKLET_SIZE = "8192";
  /**
   * DEFAULE_MAX_BLOCKLET_IN_FILE
   */
  private static final String DEFAULE_MAX_BLOCKLET_IN_FILE = "100";
  /**
   * DEFAULT_NUMBER_CORES
   */
  private static final String DEFAULT_NUMBER_CORES = "2";
  /**
   * DEFAULT_BATCH_SIZE
   */
  private static final String DEFAULT_BATCH_SIZE = "1000";
  /**
   * DEFAULT_SORT_SIZE
   */
  private static final String DEFAULT_SORT_SIZE = "100000";
  /**
   * drivers
   */
  private static final Map<String, String> DRIVERS;
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(GraphGenerator.class.getName());
  /**
   * kettleInitialized
   */
  private static boolean kettleInitialized = false;

  static {

    DRIVERS = new HashMap<String, String>(1);

    DRIVERS.put("oracle.jdbc.OracleDriver", CarbonCommonConstants.TYPE_ORACLE);
    DRIVERS.put("com.mysql.jdbc.Driver", CarbonCommonConstants.TYPE_MYSQL);
    DRIVERS.put("org.gjt.mm.mysql.Driver", CarbonCommonConstants.TYPE_MYSQL);
    DRIVERS.put("com.microsoft.sqlserver.jdbc.SQLServerDriver", CarbonCommonConstants.TYPE_MSSQL);
    DRIVERS.put("com.sybase.jdbc3.jdbc.SybDriver", CarbonCommonConstants.TYPE_SYBASE);
  }

  /**
   * OUTPUT_LOCATION
   */
  private String outputLocation = "";
  /**
   * xAxixLocation
   */
  private int xAxixLocation = 50;
  /**
   * yAxixLocation
   */
  private int yAxixLocation = 100;
  /**
   * databaseName
   */
  private String databaseName;
  /**
   * table
   */
  //    private Table table;
  /**
   * instance
   */
  private CarbonProperties instance;
  /**
   * schemaInfo
   */
  private SchemaInfo schemaInfo;
  /**
   * Table name
   */
  private String tableName;
  /**
   * Is CSV Load request
   */
  private boolean isCSVLoad;
  /**
   * Modified dimension
   */
  private String[] modifiedDimension;
  /**
   * isAutoAggRequest
   */
  private boolean isAutoAggRequest;
  /**
   * schema
   */
  private CarbonDataLoadSchema carbonDataLoadSchema;
  /**
   * isUpdateMemberRequest
   */
  private boolean isUpdateMemberRequest;
  /**
   * If the CSV file is present in HDFS?
   */
  private boolean isHDFSReadMode;
  /**
   * partitionID
   */
  private String partitionID;
  private boolean isColumnar;
  private String factTableName;
  private String factStoreLocation;
  private String blocksID;
  private String escapeCharacter;
  private String quoteCharacter;
  private String commentCharacter;
  private String dateFormat;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * load Id
   */
  private String segmentId;
  /**
   * new load start time
   */
  private String factTimeStamp;
  /**
   * max number of columns configured by user to be parsed in a row
   */
  private String maxColumns;

  private String rddIteratorKey;

  public GraphGenerator(DataLoadModel dataLoadModel, String partitionID, String factStoreLocation,
      CarbonDataLoadSchema carbonDataLoadSchema, String segmentId) {
    CarbonMetadata.getInstance().addCarbonTable(carbonDataLoadSchema.getCarbonTable());
    this.schemaInfo = dataLoadModel.getSchemaInfo();
    this.tableName = dataLoadModel.getTableName();
    this.isCSVLoad = dataLoadModel.isCsvLoad();
    this.isAutoAggRequest = schemaInfo.isAutoAggregateRequest();
    this.carbonDataLoadSchema = carbonDataLoadSchema;
    this.databaseName = carbonDataLoadSchema.getCarbonTable().getDatabaseName();
    this.partitionID = partitionID;
    this.factStoreLocation = factStoreLocation;
    this.isColumnar = Boolean.parseBoolean(CarbonCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE);
    this.blocksID = dataLoadModel.getBlocksID();
    this.taskNo = dataLoadModel.getTaskNo();
    this.quoteCharacter = dataLoadModel.getQuoteCharacter();
    this.commentCharacter = dataLoadModel.getCommentCharacter();
    this.dateFormat = dataLoadModel.getDateFormat();
    this.factTimeStamp = dataLoadModel.getFactTimeStamp();
    this.segmentId = segmentId;
    this.escapeCharacter = dataLoadModel.getEscapeCharacter();
    this.maxColumns = dataLoadModel.getMaxColumns();
    initialise();
    LOGGER.info("************* Is Columnar Storage" + isColumnar);
  }

  public GraphGenerator(DataLoadModel dataLoadModel, String partitionID, String factStoreLocation,
      CarbonDataLoadSchema carbonDataLoadSchema, String segmentId, String outputLocation) {
    this(dataLoadModel, partitionID, factStoreLocation, carbonDataLoadSchema, segmentId);
    this.outputLocation = outputLocation;
    this.rddIteratorKey = dataLoadModel.getRddIteratorKey();
  }

  /**
   * Generate the graph file ...
   *
   * @param transMeta
   * @param graphFile
   * @throws KettleException
   */
  private static void generateGraphFile(TransMeta transMeta, String graphFile)
      throws GraphGeneratorException {
    //
    DataOutputStream dos = null;
    try {
      String xml = transMeta.getXML();
      dos = new DataOutputStream(new FileOutputStream(new File(graphFile)));
      dos.write(xml.getBytes(CarbonCommonConstants.DEFAULT_CHARSET));
    } catch (KettleException kettelException) {
      throw new GraphGeneratorException("Error while getting the graph XML", kettelException);
    }
    //
    catch (FileNotFoundException e) {
      throw new GraphGeneratorException("Unable to find the graph fileL", e);
    }
    //
    catch (UnsupportedEncodingException ue) {
      throw new GraphGeneratorException("Error while Converting the graph xml string to bytes", ue);
    }
    //
    catch (IOException ioe) {
      throw new GraphGeneratorException("Error while writing the graph file", ioe);
    } finally {
      //
      if (dos != null) {
        try {
          dos.close();
        } catch (IOException e) {
          e.getMessage();
        }
      }
    }
  }

  private void initialise() {
    this.instance = CarbonProperties.getInstance();
    //TO-DO need to take care while supporting aggregate table using new schema.
    //aggregateTable = CarbonSchemaParser.getAggregateTable(table, schema);
    this.factTableName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
  }

  public void generateGraph() throws GraphGeneratorException {
    validateAndInitialiseKettelEngine();
    GraphConfigurationInfo graphConfigInfoForFact = getGraphConfigInfoForFact(carbonDataLoadSchema);
    generateGraph(graphConfigInfoForFact, graphConfigInfoForFact.getTableName() + ": Graph",
        isCSVLoad, graphConfigInfoForFact);
  }

  private void validateAndInitialiseKettelEngine() throws GraphGeneratorException {
    File file = new File(
        outputLocation + File.separator + schemaInfo.getDatabaseName() + File.separator
            + this.tableName + File.separator + this.segmentId + File.separator + this.taskNo
            + File.separator);
    boolean isDirCreated = false;
    if (!file.exists()) {
      isDirCreated = file.mkdirs();

      if (!isDirCreated) {
        LOGGER.error(
            "Unable to create directory or directory already exist" + file.getAbsolutePath());
        throw new GraphGeneratorException("INTERNAL_SYSTEM_ERROR");
      }
    }

    synchronized (DRIVERS) {
      try {
        if (!kettleInitialized) {
          EnvUtil.environmentInit();
          KettleEnvironment.init();
          kettleInitialized = true;
        }
      } catch (KettleException kettlExp) {
        LOGGER.error(kettlExp);
        throw new GraphGeneratorException("Error While Initializing the Kettel Engine ", kettlExp);
      }
    }
  }

  private void generateGraph(GraphConfigurationInfo configurationInfo, String transName,
      boolean isCSV, GraphConfigurationInfo configurationInfoForFact)
      throws GraphGeneratorException {

    TransMeta trans = new TransMeta();
    trans.setName(transName);

    if (!isCSV) {
      trans.addDatabase(getDatabaseMeta(configurationInfo));
    }

    trans.setSizeRowset(Integer.parseInt(instance
        .getProperty(CarbonCommonConstants.GRAPH_ROWSET_SIZE,
            CarbonCommonConstants.GRAPH_ROWSET_SIZE_DEFAULT)));

    StepMeta inputStep = null;
    StepMeta carbonSurrogateKeyStep = null;
    StepMeta selectValueToChangeTheDataType = null;

    // get all step
    if (isCSV) {
      if (isHDFSReadMode) {
        inputStep = getHadoopInputStep(configurationInfo);
      } else {
        inputStep = getCSVInputStep(configurationInfo);
      }
    } else {
      inputStep = getTableInputStep(configurationInfo);
      selectValueToChangeTheDataType = getSelectValueToChangeTheDataType(configurationInfo, 1);
    }
    carbonSurrogateKeyStep = getCarbonCSVBasedSurrogateKeyStep(configurationInfo);
    StepMeta sortStep = getSortStep(configurationInfo);
    StepMeta carbonMDKeyStep = getMDKeyStep(configurationInfo);
    StepMeta carbonSliceMergerStep = null;
    carbonSliceMergerStep = getSliceMeregerStep(configurationInfo, configurationInfoForFact);

    // add all steps to trans
    trans.addStep(inputStep);

    if (!isCSV) {
      trans.addStep(selectValueToChangeTheDataType);
    }

    trans.addStep(carbonSurrogateKeyStep);
    trans.addStep(sortStep);
    trans.addStep(carbonMDKeyStep);

    trans.addStep(carbonSliceMergerStep);
    TransHopMeta inputStepToSelectValueHop = null;
    TransHopMeta tableInputToSelectValue = null;

    if (isCSV) {
      inputStepToSelectValueHop = new TransHopMeta(inputStep, carbonSurrogateKeyStep);
    } else {
      inputStepToSelectValueHop = new TransHopMeta(inputStep, selectValueToChangeTheDataType);
      tableInputToSelectValue =
          new TransHopMeta(selectValueToChangeTheDataType, carbonSurrogateKeyStep);
    }

    // create hop
    TransHopMeta surrogateKeyToSortHop = new TransHopMeta(carbonSurrogateKeyStep, sortStep);
    TransHopMeta sortToMDKeyHop = new TransHopMeta(sortStep, carbonMDKeyStep);
    TransHopMeta mdkeyToSliceMerger = null;
    mdkeyToSliceMerger = new TransHopMeta(carbonMDKeyStep, carbonSliceMergerStep);

    if (isCSV) {
      trans.addTransHop(inputStepToSelectValueHop);
    } else {
      trans.addTransHop(inputStepToSelectValueHop);
      trans.addTransHop(tableInputToSelectValue);
    }

    trans.addTransHop(surrogateKeyToSortHop);
    trans.addTransHop(sortToMDKeyHop);
    trans.addTransHop(mdkeyToSliceMerger);

    String graphFilePath =
        outputLocation + File.separator + schemaInfo.getDatabaseName() + File.separator
            + this.tableName + File.separator + segmentId + File.separator + this.taskNo
            + File.separator + this.tableName + ".ktr";
    generateGraphFile(trans, graphFilePath);
  }

  private StepMeta getHadoopInputStep(GraphConfigurationInfo graphConfiguration)
      throws GraphGeneratorException {
    HadoopFileInputMeta fileInputMeta = new HadoopFileInputMeta();
    fileInputMeta.setFilenameField("filename");
    fileInputMeta.setFileName(new String[] { "${csvInputFilePath}" });
    fileInputMeta.setDefault();
    fileInputMeta.setEncoding(CarbonCommonConstants.DEFAULT_CHARSET);
    fileInputMeta.setEnclosure("\"");
    fileInputMeta.setHeader(true);
    fileInputMeta.setSeparator(",");
    fileInputMeta.setAcceptingFilenames(true);
    fileInputMeta.setAcceptingStepName("getFileNames");
    fileInputMeta.setFileFormat("mixed");
    StepMeta csvDataStep = new StepMeta("HadoopFileInputPlugin", (StepMetaInterface) fileInputMeta);
    csvDataStep.setLocation(100, 100);
    int copies = Integer.parseInt(instance.getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
        CarbonCommonConstants.DEFAULT_NUMBER_CORES));
    if (copies > 1) {
      csvDataStep.setCopies(4);
    }
    csvDataStep.setDraw(true);
    csvDataStep.setDescription("Read raw data from " + GraphGeneratorConstants.CSV_INPUT);

    return csvDataStep;
  }

  private StepMeta getCSVInputStep(GraphConfigurationInfo graphConfiguration)
      throws GraphGeneratorException {
    CsvInputMeta csvInputMeta = new CsvInputMeta();
    // Init the Filename...
    csvInputMeta.setFilename("${csvInputFilePath}");
    csvInputMeta.setDefault();
    csvInputMeta.setEncoding(CarbonCommonConstants.DEFAULT_CHARSET);
    csvInputMeta.setEnclosure("\"");
    csvInputMeta.setHeaderPresent(true);
    csvInputMeta.setMaxColumns(maxColumns);
    StepMeta csvDataStep =
        new StepMeta(GraphGeneratorConstants.CSV_INPUT, (StepMetaInterface) csvInputMeta);
    csvDataStep.setLocation(100, 100);
    csvInputMeta.setFilenameField("filename");
    csvInputMeta.setLazyConversionActive(false);
    csvInputMeta.setBufferSize(instance.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
    //set blocks info id
    csvInputMeta.setBlocksID(this.blocksID);
    csvInputMeta.setPartitionID(this.partitionID);
    csvInputMeta.setEscapeCharacter(this.escapeCharacter);
    csvInputMeta.setQuoteCharacter(this.quoteCharacter);
    csvInputMeta.setCommentCharacter(this.commentCharacter);
    csvInputMeta.setRddIteratorKey(this.rddIteratorKey == null ? "" : this.rddIteratorKey);
    csvDataStep.setDraw(true);
    csvDataStep.setDescription("Read raw data from " + GraphGeneratorConstants.CSV_INPUT);

    return csvDataStep;
  }

  private StepMeta getSliceMeregerStep(GraphConfigurationInfo configurationInfo,
      GraphConfigurationInfo graphjConfigurationForFact) {
    CarbonSliceMergerStepMeta sliceMerger = new CarbonSliceMergerStepMeta();
    sliceMerger.setDefault();
    sliceMerger.setPartitionID(partitionID);
    sliceMerger.setSegmentId(segmentId);
    sliceMerger.setTaskNo(taskNo);
    sliceMerger.setHeirAndKeySize(configurationInfo.getHeirAndKeySizeString());
    sliceMerger.setMdkeySize(configurationInfo.getMdkeySize());
    sliceMerger.setMeasureCount(configurationInfo.getMeasureCount());
    sliceMerger.setTabelName(configurationInfo.getTableName());
    sliceMerger.setTableName(schemaInfo.getTableName());
    sliceMerger.setDatabaseName(schemaInfo.getDatabaseName());
    sliceMerger.setGroupByEnabled(isAutoAggRequest + "");
    if (isAutoAggRequest) {
      String[] aggType = configurationInfo.getAggType();
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < aggType.length - 1; i++) {
        if (aggType[i].equals(CarbonCommonConstants.COUNT)) {
          builder.append(CarbonCommonConstants.SUM);
        } else {
          builder.append(aggType[i]);
        }
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
      builder.append(aggType[aggType.length - 1]);
      sliceMerger.setAggregatorString(builder.toString());
      String[] aggClass = configurationInfo.getAggClass();
      builder = new StringBuilder();
      for (int i = 0; i < aggClass.length - 1; i++) {
        builder.append(aggClass[i]);
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
      builder.append(aggClass[aggClass.length - 1]);
      sliceMerger.setAggregatorClassString(builder.toString());
    } else {
      sliceMerger.setAggregatorClassString(CarbonCommonConstants.HASH_SPC_CHARACTER);
      sliceMerger.setAggregatorString(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    sliceMerger.setFactDimLensString("");
    sliceMerger.setLevelAnddataTypeString(configurationInfo.getLevelAnddataType());
    StepMeta sliceMergerMeta =
        new StepMeta(GraphGeneratorConstants.CARBON_SLICE_MERGER + configurationInfo.getTableName(),
            (StepMetaInterface) sliceMerger);
    sliceMergerMeta.setStepID(GraphGeneratorConstants.CARBON_SLICE_MERGER_ID);
    xAxixLocation += 120;
    sliceMergerMeta.setLocation(xAxixLocation, yAxixLocation);
    sliceMergerMeta.setDraw(true);
    sliceMergerMeta.setDescription(
        "SliceMerger: " + GraphGeneratorConstants.CARBON_SLICE_MERGER + configurationInfo
            .getTableName());
    return sliceMergerMeta;
  }

  private DatabaseMeta getDatabaseMeta(GraphConfigurationInfo configurationInfo)
      throws GraphGeneratorException {
    return new DatabaseMeta();
  }

  private StepMeta getTableInputStep(GraphConfigurationInfo configurationInfo)
      throws GraphGeneratorException {
    TableInputMeta tableInput = new TableInputMeta();
    tableInput.setDatabaseMeta(getDatabaseMeta(configurationInfo));
    tableInput.setSQL(configurationInfo.getTableInputSqlQuery());
    //
    StepMeta tableInputStep =
        new StepMeta(GraphGeneratorConstants.TABLE_INPUT, (StepMetaInterface) tableInput);
    xAxixLocation += 120;
    tableInputStep.setLocation(xAxixLocation, yAxixLocation);
    //
    tableInputStep.setDraw(true);
    tableInputStep
        .setDescription("Read Data From Fact Table: " + GraphGeneratorConstants.TABLE_INPUT);

    return tableInputStep;
  }

  private StepMeta getCarbonCSVBasedSurrogateKeyStep(GraphConfigurationInfo graphConfiguration) {
    //
    CarbonCSVBasedSeqGenMeta seqMeta = new CarbonCSVBasedSeqGenMeta();
    seqMeta.setPartitionID(partitionID);
    seqMeta.setSegmentId(segmentId);
    seqMeta.setTaskNo(taskNo);
    seqMeta.setCarbondim(graphConfiguration.getDimensionString());
    seqMeta.setComplexTypeString(graphConfiguration.getComplexTypeString());
    seqMeta.setColumnPropertiesString(graphConfiguration.getColumnPropertiesString());
    seqMeta.setBatchSize(Integer.parseInt(graphConfiguration.getBatchSize()));
    seqMeta.setNoDictionaryDims(graphConfiguration.getNoDictionaryDims());
    seqMeta.setDimensionColumnsDataType(graphConfiguration.getDimensionColumnsDataType());
    seqMeta.setTableName(schemaInfo.getTableName());
    seqMeta.setDatabaseName(schemaInfo.getDatabaseName());
    seqMeta.setComplexDelimiterLevel1(schemaInfo.getComplexDelimiterLevel1());
    seqMeta.setComplexDelimiterLevel2(schemaInfo.getComplexDelimiterLevel2());
    seqMeta.setCarbonmsr(graphConfiguration.getMeasuresString());
    seqMeta.setCarbonProps(graphConfiguration.getPropertiesString());
    seqMeta.setCarbonhier(graphConfiguration.getHiersString());
    seqMeta.setCarbonhierColumn(graphConfiguration.getHierColumnString());
    seqMeta.setDimensionColumnIds(graphConfiguration.getDimensionColumnIds());
    seqMeta.setMetaMetaHeirSQLQueries(graphConfiguration.getDimensionSqlQuery());
    seqMeta.setColumnAndTableNameColumnMapForAggString(
        graphConfiguration.getColumnAndTableNameColumnMapForAgg());
    seqMeta.setForgienKeyPrimayKeyString(graphConfiguration.getForgienKeyAndPrimaryKeyMapString());
    seqMeta.setTableName(graphConfiguration.getTableName());
    seqMeta.setDateFormat(dateFormat);
    seqMeta.setModifiedDimension(modifiedDimension);
    seqMeta.setForeignKeyHierarchyString(graphConfiguration.getForeignKeyHierarchyString());
    seqMeta.setPrimaryKeysString(graphConfiguration.getPrimaryKeyString());
    seqMeta.setCarbonMeasureNames(graphConfiguration.getMeasureNamesString());
    seqMeta.setHeirNadDimsLensString(graphConfiguration.getHeirAndDimLens());
    seqMeta.setActualDimNames(graphConfiguration.getActualDimensionColumns());
    seqMeta.setNormHiers(graphConfiguration.getNormHiers());
    seqMeta.setHeirKeySize(graphConfiguration.getHeirAndKeySizeString());
    seqMeta.setColumnSchemaDetails(graphConfiguration.getColumnSchemaDetails().toString());
    seqMeta.setTableOption(graphConfiguration.getTableOptionWrapper().toString());
    String[] aggType = graphConfiguration.getAggType();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < aggType.length; i++) {
      builder.append(aggType[i]);
      builder.append(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
    }
    seqMeta.setMsrAggregatorString(builder.toString());

    seqMeta.setDriverClass(graphConfiguration.getDriverclass());
    seqMeta.setConnectionURL(graphConfiguration.getConnectionUrl());
    seqMeta.setUserName(graphConfiguration.getUsername());
    seqMeta.setPassword(graphConfiguration.getPassword());
    seqMeta.setMeasureDataType(graphConfiguration.getMeasureDataTypeInfo());
    seqMeta.setDenormColumNames(graphConfiguration.getDenormColumns());
    seqMeta.setAggregate(graphConfiguration.isAGG());
    seqMeta.setTableNames(graphConfiguration.getDimensionTableNames());
    StepMeta mdkeyStepMeta = new StepMeta(GraphGeneratorConstants.CARBON_SURROGATE_KEY_GENERATOR,
        (StepMetaInterface) seqMeta);
    mdkeyStepMeta.setStepID(GraphGeneratorConstants.CARBON_CSV_BASED_SURROAGATEGEN_ID);
    xAxixLocation += 120;
    //
    mdkeyStepMeta.setLocation(xAxixLocation, yAxixLocation);
    mdkeyStepMeta.setDraw(true);
    mdkeyStepMeta.setDescription("Generate Surrogate For Table Data: "
        + GraphGeneratorConstants.CARBON_SURROGATE_KEY_GENERATOR);
    return mdkeyStepMeta;
  }

  private StepMeta getMDKeyStep(GraphConfigurationInfo graphConfiguration) {
    MDKeyGenStepMeta carbonMdKey = new MDKeyGenStepMeta();
    carbonMdKey.setIsUseInvertedIndex(
        NonDictionaryUtil.convertBooleanArrToString(graphConfiguration.getIsUseInvertedIndex()));
    carbonMdKey.setPartitionID(partitionID);
    carbonMdKey.setSegmentId(segmentId);
    carbonMdKey.setNumberOfCores(graphConfiguration.getNumberOfCores());
    carbonMdKey.setTableName(graphConfiguration.getTableName());
    carbonMdKey.setDatabaseName(schemaInfo.getDatabaseName());
    carbonMdKey.setTableName(schemaInfo.getTableName());
    carbonMdKey.setComplexTypeString(graphConfiguration.getComplexTypeString());
    carbonMdKey.setAggregateLevels(CarbonDataProcessorUtil
        .getLevelCardinalitiesString(graphConfiguration.getDimCardinalities(),
            graphConfiguration.getDimensions()));
    carbonMdKey.setNoDictionaryDimsMapping(NonDictionaryUtil
        .convertBooleanArrToString(graphConfiguration.getIsNoDictionaryDimMapping()));
    carbonMdKey.setMeasureCount(graphConfiguration.getMeasureCount() + "");
    carbonMdKey.setColumnGroupsString(graphConfiguration.getColumnGroupsString());
    carbonMdKey.setDimensionCount(graphConfiguration.getActualDims().length + "");
    carbonMdKey.setComplexDimsCount(graphConfiguration.getComplexTypeString().isEmpty() ?
        "0" :
        graphConfiguration.getComplexTypeString()
            .split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER).length + "");
    carbonMdKey.setMeasureDataType(graphConfiguration.getMeasureDataTypeInfo());
    carbonMdKey.setTaskNo(taskNo);
    carbonMdKey.setFactTimeStamp(factTimeStamp);
    StepMeta mdkeyStepMeta =
        new StepMeta(GraphGeneratorConstants.MDKEY_GENERATOR + graphConfiguration.getTableName(),
            (StepMetaInterface) carbonMdKey);
    mdkeyStepMeta
        .setName(GraphGeneratorConstants.MDKEY_GENERATOR_ID + graphConfiguration.getTableName());
    mdkeyStepMeta.setStepID(GraphGeneratorConstants.MDKEY_GENERATOR_ID);
    //
    xAxixLocation += 120;
    mdkeyStepMeta.setLocation(xAxixLocation, yAxixLocation);
    mdkeyStepMeta.setDraw(true);
    mdkeyStepMeta.setDescription(
        "Generate MDKey For Table Data: " + GraphGeneratorConstants.MDKEY_GENERATOR
            + graphConfiguration.getTableName());
    carbonMdKey.setNoDictionaryDims(graphConfiguration.getNoDictionaryDims());

    return mdkeyStepMeta;
  }

  private StepMeta getSelectValueToChangeTheDataType(GraphConfigurationInfo graphConfiguration,
      int counter) {
    //
    SelectValuesMeta selectValues = new SelectValuesMeta();
    selectValues.allocate(0, 0, 0);
    StepMeta selectValueMeta = new StepMeta(
        GraphGeneratorConstants.SELECT_REQUIRED_VALUE + "Change Dimension And Measure DataType"
            + System.currentTimeMillis() + counter, (StepMetaInterface) selectValues);
    xAxixLocation += 120;
    selectValueMeta.setName("SelectValueToChangeChangeData");
    selectValueMeta.setLocation(xAxixLocation, yAxixLocation);
    selectValueMeta.setDraw(true);
    selectValueMeta.setDescription(
        "Change The Data Type For Measures: " + GraphGeneratorConstants.SELECT_REQUIRED_VALUE);

    String inputQuery = graphConfiguration.getTableInputSqlQuery();
    String[] columns = parseQueryAndReturnColumns(inputQuery);

    SelectMetadataChange[] changeMeta = new SelectMetadataChange[columns.length];
    Map<String, Boolean> measureDatatypeMap =
        getMeasureDatatypeMap(graphConfiguration.getMeasureDataTypeInfo());
    String[] measures = graphConfiguration.getMeasures();
    String dimensionString = graphConfiguration.getActualDimensionColumns();
    String[] dimension = dimensionString.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    for (int i = 0; i < columns.length; i++) {
      changeMeta[i] = new SelectMetadataChange(selectValues);
      changeMeta[i].setName(columns[i]);
      changeMeta[i].setType(2);
      if (isMeasureColumn(measures, columns[i]) && isNotDimesnionColumn(dimension, columns[i])) {
        Boolean isString = measureDatatypeMap.get(columns[i]);
        if (isString != null && isString) {
          changeMeta[i].setType(2);
        } else {
          changeMeta[i].setType(6);
        }
      }
      changeMeta[i].setStorageType(0);
    }
    //
    selectValues.setMeta(changeMeta);
    return selectValueMeta;
  }

  private boolean isMeasureColumn(String[] measures, String column) {
    for (int i = 0; i < measures.length; i++) {
      if (measures[i].equals(column)) {
        return true;
      }
    }
    return false;
  }

  private boolean isNotDimesnionColumn(String[] dimension, String column) {
    for (int i = 0; i < dimension.length; i++) {
      if (dimension[i].equals(column)) {
        return false;
      }
    }
    return true;
  }

  private Map<String, Boolean> getMeasureDatatypeMap(String measureDataType) {
    if (measureDataType == null || "".equals(measureDataType)) {
      return new HashMap<String, Boolean>(1);
    }
    Map<String, Boolean> resultMap = new HashMap<String, Boolean>(1);

    String[] measures = measureDataType.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    String[] measureValue = null;
    for (int i = 0; i < measures.length; i++) {
      measureValue = measures[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      resultMap.put(measureValue[0], Boolean.valueOf(measureValue[1]));
    }
    return resultMap;
  }

  /**
   * @param inputQuery
   * @return
   */
  private String[] parseQueryAndReturnColumns(String inputQuery) {
    Set<String> cols = new LinkedHashSet<String>();
    String columnString =
        inputQuery.substring(inputQuery.indexOf("SELECT") + 6, inputQuery.indexOf("FROM"));
    String[] columns = columnString.split(",");
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].indexOf("\"") > -1) {
        columns[i] = columns[i].replace("\"", "");
        if (columns[i].contains(".")) {
          columns[i] = columns[i].split("\\.")[1];
        }
      }

      cols.add(columns[i].replaceAll(System.getProperty("line.separator"), "").trim());
    }
    return cols.toArray(new String[cols.size()]);
  }

  private StepMeta getSortStep(GraphConfigurationInfo graphConfiguration)
      throws GraphGeneratorException {
    String[] actualMeasures = graphConfiguration.getMeasures();

    SortKeyStepMeta sortRowsMeta = new SortKeyStepMeta();
    sortRowsMeta.setPartitionID(partitionID);
    sortRowsMeta.setSegmentId(segmentId);
    sortRowsMeta.setTaskNo(taskNo);
    sortRowsMeta.setTabelName(graphConfiguration.getTableName());
    sortRowsMeta.setTableName(schemaInfo.getTableName());
    sortRowsMeta.setDatabaseName(schemaInfo.getDatabaseName());
    sortRowsMeta.setOutputRowSize(actualMeasures.length + 1 + "");
    sortRowsMeta.setDimensionCount(graphConfiguration.getDimensions().length + "");
    sortRowsMeta.setComplexDimensionCount(graphConfiguration.getComplexTypeString().isEmpty() ?
        "0" :
        graphConfiguration.getComplexTypeString()
            .split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER).length + "");
    sortRowsMeta.setIsUpdateMemberRequest(isUpdateMemberRequest + "");
    sortRowsMeta.setMeasureCount(graphConfiguration.getMeasureCount() + "");
    sortRowsMeta.setNoDictionaryDims(graphConfiguration.getNoDictionaryDims());
    sortRowsMeta.setMeasureDataType(graphConfiguration.getMeasureDataTypeInfo());
    sortRowsMeta.setNoDictionaryDimsMapping(NonDictionaryUtil
        .convertBooleanArrToString(graphConfiguration.getIsNoDictionaryDimMapping()));

    StepMeta sortRowsStep = new StepMeta(
        GraphGeneratorConstants.SORT_KEY_AND_GROUPBY + graphConfiguration.getTableName(),
        (StepMetaInterface) sortRowsMeta);

    xAxixLocation += 120;
    sortRowsStep.setDraw(true);
    sortRowsStep.setLocation(xAxixLocation, yAxixLocation);
    sortRowsStep.setStepID(GraphGeneratorConstants.SORTKEY_ID);
    sortRowsStep.setDescription(
        "Sort Key: " + GraphGeneratorConstants.SORT_KEY + graphConfiguration.getTableName());
    sortRowsStep.setName(
        "Sort Key: " + GraphGeneratorConstants.SORT_KEY + graphConfiguration.getTableName());
    return sortRowsStep;
  }

  private GraphConfigurationInfo getGraphConfigInfoForFact(
      CarbonDataLoadSchema carbonDataLoadSchema) throws GraphGeneratorException {
    //
    GraphConfigurationInfo graphConfiguration = new GraphConfigurationInfo();
    List<CarbonDimension> dimensions = carbonDataLoadSchema.getCarbonTable()
        .getDimensionByTableName(carbonDataLoadSchema.getCarbonTable().getFactTableName());
    prepareIsUseInvertedIndex(dimensions, graphConfiguration);
    graphConfiguration
        .setDimensions(CarbonSchemaParser.getTableDimensions(dimensions, carbonDataLoadSchema));
    graphConfiguration
        .setActualDims(CarbonSchemaParser.getTableDimensions(dimensions, carbonDataLoadSchema));
    graphConfiguration
        .setColumnPropertiesString(CarbonSchemaParser.getColumnPropertiesString(dimensions));
    graphConfiguration.setComplexTypeString(CarbonSchemaParser.getComplexTypeString(dimensions));
    prepareNoDictionaryMapping(dimensions, graphConfiguration);
    graphConfiguration
        .setColumnSchemaDetails(CarbonSchemaParser.getColumnSchemaDetails(dimensions));
    graphConfiguration.setTableOptionWrapper(getTableOptionWrapper());
    String factTableName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
    graphConfiguration.setTableName(factTableName);
    StringBuilder dimString = new StringBuilder();
    //
    int currentCount =
        CarbonSchemaParser.getDimensionString(dimensions, dimString, 0, carbonDataLoadSchema);
    StringBuilder noDictionarydimString = new StringBuilder();
    CarbonSchemaParser
        .getNoDictionaryDimensionString(dimensions, noDictionarydimString, 0, carbonDataLoadSchema);
    graphConfiguration.setNoDictionaryDims(noDictionarydimString.toString());

    String tableString =
        CarbonSchemaParser.getTableNameString(dimensions, carbonDataLoadSchema);
    String dimensionColumnIds = CarbonSchemaParser.getColumnIdString(dimensions);
    graphConfiguration.setDimensionTableNames(tableString);
    graphConfiguration.setDimensionString(dimString.toString());
    graphConfiguration.setDimensionColumnIds(dimensionColumnIds);
    graphConfiguration
        .setForignKey(CarbonSchemaParser.getForeignKeyForTables(dimensions, carbonDataLoadSchema));
    List<CarbonMeasure> measures = carbonDataLoadSchema.getCarbonTable()
        .getMeasureByTableName(carbonDataLoadSchema.getCarbonTable().getFactTableName());
    graphConfiguration
        .setMeasuresString(CarbonSchemaParser.getMeasureString(measures, currentCount));
    graphConfiguration
        .setHiersString(CarbonSchemaParser.getHierarchyString(dimensions, carbonDataLoadSchema));
    graphConfiguration.setHierColumnString(
        CarbonSchemaParser.getHierarchyStringWithColumnNames(dimensions, carbonDataLoadSchema));
    graphConfiguration.setMeasureUniqueColumnNamesString(
        CarbonSchemaParser.getMeasuresUniqueColumnNamesString(measures));
    graphConfiguration.setForeignKeyHierarchyString(CarbonSchemaParser
        .getForeignKeyHierarchyString(dimensions, carbonDataLoadSchema, factTableName));
    graphConfiguration.setConnectionName("target");
    graphConfiguration.setHeirAndDimLens(
        CarbonSchemaParser.getHeirAndCardinalityString(dimensions, carbonDataLoadSchema));
    //setting dimension store types
    graphConfiguration.setColumnGroupsString(CarbonSchemaParser.getColumnGroups(dimensions));
    graphConfiguration.setPrimaryKeyString(
        CarbonSchemaParser.getPrimaryKeyString(dimensions, carbonDataLoadSchema));
    graphConfiguration
        .setDenormColumns(CarbonSchemaParser.getDenormColNames(dimensions, carbonDataLoadSchema));

    graphConfiguration.setLevelAnddataType(
        CarbonSchemaParser.getLevelAndDataTypeMapString(dimensions, carbonDataLoadSchema));

    graphConfiguration.setForgienKeyAndPrimaryKeyMapString(CarbonSchemaParser
        .getForeignKeyAndPrimaryKeyMapString(carbonDataLoadSchema.getDimensionRelationList()));

    graphConfiguration.setMdkeySize(CarbonSchemaParser.getMdkeySizeForFact(dimensions));
    Set<String> measureColumn = new HashSet<String>(measures.size());
    for (int i = 0; i < measures.size(); i++) {
      measureColumn.add(measures.get(i).getColName());
    }
    char[] type = new char[measureColumn.size()];
    Arrays.fill(type, 'n');
    graphConfiguration.setType(type);
    graphConfiguration.setMeasureCount(measureColumn.size() + "");
    graphConfiguration.setHeirAndKeySizeString(
        CarbonSchemaParser.getHeirAndKeySizeMapForFact(dimensions, carbonDataLoadSchema));
    graphConfiguration.setAggType(CarbonSchemaParser.getMeasuresAggragatorArray(measures));
    graphConfiguration.setMeasureNamesString(CarbonSchemaParser.getMeasuresNamesString(measures));
    graphConfiguration
        .setActualDimensionColumns(CarbonSchemaParser.getActualDimensions(dimensions));
    graphConfiguration
        .setDimensionColumnsDataType(CarbonSchemaParser.getDimensionsDataTypes(dimensions));
    //graphConfiguration.setNormHiers(CarbonSchemaParser.getNormHiers(table, schema));
    graphConfiguration.setMeasureDataTypeInfo(CarbonSchemaParser.getMeasuresDataType(measures));
    graphConfiguration.setStoreLocation(
        this.databaseName + '/' + carbonDataLoadSchema.getCarbonTable().getFactTableName());
    graphConfiguration.setBlockletSize(
        (instance.getProperty("com.huawei.unibi.carbon.blocklet.size", DEFAUL_BLOCKLET_SIZE)));
    graphConfiguration.setMaxBlockletInFile(
        (instance.getProperty("carbon.max.blocklet.in.file", DEFAULE_MAX_BLOCKLET_IN_FILE)));
    graphConfiguration.setNumberOfCores(
        (instance.getProperty(CarbonCommonConstants.NUM_CORES_LOADING, DEFAULT_NUMBER_CORES)));

    // check quotes required in query or Not
    boolean isQuotesRequired = true;
    String quote = CarbonSchemaParser.QUOTES;
    graphConfiguration.setTableInputSqlQuery(CarbonSchemaParser
        .getTableInputSQLQuery(dimensions, measures,
            carbonDataLoadSchema.getCarbonTable().getFactTableName(), isQuotesRequired,
            carbonDataLoadSchema));
    graphConfiguration
        .setBatchSize((instance.getProperty("carbon.batch.size", DEFAULT_BATCH_SIZE)));
    graphConfiguration.setSortSize((instance.getProperty("carbon.sort.size", DEFAULT_SORT_SIZE)));
    graphConfiguration.setDimensionSqlQuery(CarbonSchemaParser
        .getDimensionSQLQueries(dimensions, carbonDataLoadSchema, isQuotesRequired, quote));
    graphConfiguration.setMetaHeirString(
        CarbonSchemaParser.getMetaHeirString(dimensions, carbonDataLoadSchema.getCarbonTable()));
    graphConfiguration
        .setDimCardinalities(CarbonSchemaParser.getCardinalities(dimensions, carbonDataLoadSchema));

    graphConfiguration.setMeasures(CarbonSchemaParser.getMeasures(measures));
    graphConfiguration.setAGG(false);
    return graphConfiguration;
  }

  /**
   * the method returns the table option wrapper
   *
   * @return
   */
  private TableOptionWrapper getTableOptionWrapper() {
    TableOptionWrapper tableOptionWrapper = TableOptionWrapper.getTableOptionWrapperInstance();
    tableOptionWrapper.setTableOption(schemaInfo.getSerializationNullFormat());
    tableOptionWrapper.setTableOption(schemaInfo.getBadRecordsLoggerEnable());
    tableOptionWrapper.setTableOption(schemaInfo.getBadRecordsLoggerAction());
    return tableOptionWrapper;
  }

  public CarbonTable getTable() {
    return carbonDataLoadSchema.getCarbonTable();
  }

  /**
   * Preparing the boolean [] to map whether the dimension is no Dictionary or not.
   *
   * @param dims
   * @param graphConfig
   */
  private void prepareNoDictionaryMapping(List<CarbonDimension> dims,
      GraphConfigurationInfo graphConfig) {
    List<Boolean> noDictionaryMapping = new ArrayList<Boolean>();
    for (CarbonDimension dimension : dims) {
      // for  complex type need to break the loop
      if (dimension.getNumberOfChild() > 0) {
        break;
      }

      if (!dimension.getEncoder().contains(Encoding.DICTIONARY)) {
        noDictionaryMapping.add(true);
        //NoDictionaryMapping[index] = true;
      } else {
        noDictionaryMapping.add(false);
      }
    }

    graphConfig.setIsNoDictionaryDimMapping(
        noDictionaryMapping.toArray(new Boolean[noDictionaryMapping.size()]));
  }

  /**
   * Preparing the boolean [] to map whether the dimension use inverted index or not.
   *
   * @param dims
   * @param graphConfig
   */
  private void prepareIsUseInvertedIndex(List<CarbonDimension> dims,
      GraphConfigurationInfo graphConfig) {
    List<Boolean> isUseInvertedIndexList = new ArrayList<Boolean>();
    for (CarbonDimension dimension : dims) {
      if (dimension.isUseInvertedIndex()) {
        isUseInvertedIndexList.add(true);
      } else {
        isUseInvertedIndexList.add(false);
      }
    }
    graphConfig.setIsUseInvertedIndex(
        isUseInvertedIndexList.toArray(new Boolean[isUseInvertedIndexList.size()]));
  }
}
