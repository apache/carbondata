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

package org.carbondata.processing.graphgenerator;

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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.csvreader.checkpoint.CheckPointHanlder;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.load.BlockDetails;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.processing.aggregatesurrogategenerator.step.CarbonAggregateSurrogateGeneratorMeta;
import org.carbondata.processing.api.dataloader.DataLoadModel;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.autoaggregategraphgenerator.step.CarbonAutoAGGGraphGeneratorMeta;
import org.carbondata.processing.csvreaderstep.CsvInputMeta;
import org.carbondata.processing.factreader.step.CarbonFactReaderMeta;
import org.carbondata.processing.graphgenerator.configuration.GraphConfigurationInfo;
import org.carbondata.processing.mdkeygen.MDKeyGenStepMeta;
import org.carbondata.processing.merger.step.CarbonSliceMergerStepMeta;
import org.carbondata.processing.schema.metadata.AggregateTable;
import org.carbondata.processing.sortandgroupby.sortdatastep.SortKeyStepMeta;
import org.carbondata.processing.sortandgroupby.step.CarbonSortKeyAndGroupByStepMeta;
import org.carbondata.processing.store.CarbonDataWriterStepMeta;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedSeqGenMeta;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.carbondata.processing.util.RemoveDictionaryUtil;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.getfilenames.GetFileNamesMeta;
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
   * kettleIntialized
   */
  private static boolean kettleIntialized;

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
  private final String outputLocation = CarbonProperties.getInstance()
      .getProperty("store_output_location", "../unibi-solutions/system/carbon/etl");
  /**
   * Segment names
   */
  private String loadNames;
  /**
   * map having segment name  as key and segment Modification time stamp as value
   */
  private String modificationOrDeletionTime;
  /**
   * xAxixLocation
   */
  private int xAxixLocation = 50;
  /**
   * yAxixLocation
   */
  private int yAxixLocation = 100;
  /**
   * schemaName
   */
  private String schemaName;
  /**
   * cubeName
   */
  private String cubeName;
  /**
   * cube
   */
  //    private Cube cube;
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
   * aggregateTable
   */
  private AggregateTable[] aggregateTable;
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
  private int currentRestructNumber;
  private int allocate;
  private String blocksID;
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

  public GraphGenerator(DataLoadModel dataLoadModel, boolean isHDFSReadMode, String partitionID,
      String factStoreLocation, int currentRestructNum, int allocate,
      CarbonDataLoadSchema carbonDataLoadSchema, String segmentId) {
    CarbonMetadata.getInstance().addCarbonTable(carbonDataLoadSchema.getCarbonTable());
    this.schemaInfo = dataLoadModel.getSchemaInfo();
    this.tableName = dataLoadModel.getTableName();
    this.isCSVLoad = dataLoadModel.isCsvLoad();
    this.modifiedDimension = dataLoadModel.getModifiedDimesion();
    this.isAutoAggRequest = schemaInfo.isAutoAggregateRequest();
    //this.schema = schema;
    this.carbonDataLoadSchema = carbonDataLoadSchema;
    this.schemaName = carbonDataLoadSchema.getCarbonTable().getDatabaseName();
    this.partitionID = partitionID;
    this.factStoreLocation = factStoreLocation;
    this.isColumnar = Boolean.parseBoolean(CarbonCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE);
    this.currentRestructNumber = currentRestructNum;
    this.allocate = allocate > 1 ? allocate : 1;
    this.loadNames = dataLoadModel.getLoadNames();
    this.modificationOrDeletionTime = dataLoadModel.getModificationOrDeletionTime();
    this.blocksID = dataLoadModel.getBlocksID();
    this.taskNo = dataLoadModel.getTaskNo();
    this.factTimeStamp = dataLoadModel.getFactTimeStamp();
    this.segmentId = segmentId;
    initialise();
    LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
        "************* Is Columnar Storage" + isColumnar);
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
      dos.write(xml.getBytes("UTF-8"));
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
    this.cubeName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
    this.instance = CarbonProperties.getInstance();
    //TO-DO need to take care while supporting aggregate table using new schema.
    //aggregateTable = CarbonSchemaParser.getAggregateTable(cube, schema);
    this.factTableName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
  }

  public void generateGraph() throws GraphGeneratorException {
    validateAndInitialiseKettelEngine();
    String factTableName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
    GraphConfigurationInfo graphConfigInfoForFact = getGraphConfigInfoForFact(carbonDataLoadSchema);
    if (factTableName.equals(tableName)) {
      generateGraph(graphConfigInfoForFact, graphConfigInfoForFact.getTableName() + ": Graph",
          isCSVLoad, graphConfigInfoForFact);
      return;
    }
    if (null != aggregateTable) {
      if (schemaInfo.isAggregateTableCSVLoadRequest()) {
        for (int i = 0; i < aggregateTable.length; i++) {
          // Default location to start a step
          xAxixLocation = 50;
          if (aggregateTable[i].getAggregateTableName().equals(tableName)) {
            GraphConfigurationInfo graphConfigInfoForAGG =
                getGraphConfigInfoForCSVBasedAGG(aggregateTable[i], carbonDataLoadSchema);

            generateGraph(graphConfigInfoForAGG, graphConfigInfoForAGG.getTableName() + ": Graph",
                isCSVLoad, graphConfigInfoForFact);
            break;
          }
        }
      } else {
        String[] tableNames = tableName.split(",");
        int size = tableNames.length;

        AggregateTable[] aggregateTableArray = new AggregateTable[size];
        for (int i = 0; i < size; i++) {
          for (int j = 0; j < aggregateTable.length; j++) {
            if (aggregateTable[j].getAggregateTableName().equals(tableName)) {
              aggregateTableArray[i] = aggregateTable[j];
            }
          }
        }

        generateGraphForManualAgg(aggregateTableArray, factTableName,
            getAutoAggGraphGeneratorStep(factTableName, aggregateTableArray));
      }
    }
  }

  private void validateAndInitialiseKettelEngine() throws GraphGeneratorException {
    File file = new File(
        outputLocation + File.separator + schemaInfo.getSchemaName() + File.separator
            + this.tableName + File.separator + this.segmentId + File.separator + this.taskNo
            + File.separator);
    boolean isDirCreated = false;
    if (!file.exists()) {
      isDirCreated = file.mkdirs();

      if (!isDirCreated) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "Unable to create directory or Directory already exist" + file.getAbsolutePath());
        throw new GraphGeneratorException("INTERNAL_SYSTEM_ERROR");
      }
    }

    if (!kettleIntialized) {
      synchronized (DRIVERS) {
        try {
          EnvUtil.environmentInit();
          KettleEnvironment.init();
          kettleIntialized = false;
        } catch (KettleException kettlExp) {
          LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
              "Invalid kettle path :: " + kettlExp.getMessage());
          LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, kettlExp);
          throw new GraphGeneratorException("Error While Initializing the Kettel Engine ",
              kettlExp);
        }
      }
    }
  }

  /**
   * Below method will be used to generate the graph
   *
   * @throws GraphGeneratorException
   */
  public String generateGraph(String factTableName, String factStorePath,
      AggregateTable[] aggTables, boolean isGeneratedFromFactTables, String... aggTableList)
      throws GraphGeneratorException {
    int[] factCardinality;
    validateAndInitialiseKettelEngine();
    StringBuilder builder = new StringBuilder();
    GraphConfigurationInfo graphConfigInfoForFact = null;

    if (isGeneratedFromFactTables) {
      graphConfigInfoForFact = getGraphConfigInfoForFact(carbonDataLoadSchema);
    } else {
      AggregateTable aggTablesUsedForgeneration = null;
      for (int i = 0; i < aggregateTable.length; i++) {
        if (aggregateTable[i].getAggregateTableName().equals(factTableName)) {
          aggTablesUsedForgeneration = aggregateTable[i];
          graphConfigInfoForFact =
              getGraphConfigInfoForCSVBasedAGG(aggTablesUsedForgeneration, carbonDataLoadSchema);
          break;
        }
      }

    }

    List<List<StepMeta>> stepMetaList = new ArrayList<List<StepMeta>>(1);
    xAxixLocation = 50;
    boolean isFactMdKeyInOutRow = false;
    String[] aggType = null;
    int aggTableListSize = aggTableList.length;
    AggregateTable[] aggregateTableArray = new AggregateTable[aggTableListSize];

    String factLevelCardinalityFile = factStorePath + File.separator +
        CarbonCommonConstants.LEVEL_METADATA_FILE + factTableName + ".metadata";

    try {
      factCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(factLevelCardinalityFile);
    } catch (CarbonUtilException e) {
      throw new GraphGeneratorException("Error while reading fact cardinality", e);
    }

    for (int i = 0; i < aggTableListSize; i++) {
      for (int j = 0; j < aggTables.length; j++) {
        if (aggTables[j].getAggregateTableName().equals(aggTableList[i])) {
          aggregateTableArray[i] = aggTables[j];
        }
      }
    }
    GraphConfigurationInfo graphConfigInfoForAGG = null;
    for (int i = 0; i < aggTableListSize; i++) {
      for (int j = 0; j < aggTables.length; j++) {
        if (aggTables[j].getAggregateTableName().equals(aggTableList[i])) {
          graphConfigInfoForAGG =
              getGraphConfigInfoForCSVBasedAGG(aggTables[j], carbonDataLoadSchema);
          aggType = graphConfigInfoForAGG.getAggType();
          for (int k = 0; k < aggType.length; k++) {
            if (aggType[k].equals(CarbonCommonConstants.CUSTOM)) {
              isFactMdKeyInOutRow = true;
            }
          }
          stepMetaList.add(generateStepMetaForAutoAgg(graphConfigInfoForFact, graphConfigInfoForAGG,
              aggTables[j], aggregateTableArray, factCardinality, factStorePath));
          builder.append(aggTableList[i]);
          xAxixLocation += 50;
          break;
        }
      }
    }

    return generateGraph(stepMetaList, graphConfigInfoForFact, builder.toString(), factTableName,
        isFactMdKeyInOutRow, false, aggregateTableArray, factStorePath, factCardinality,
        graphConfigInfoForAGG.getDimensions());
  }

  private void generateGraphForManualAgg(AggregateTable[] aggTableList, String factTableName,
      StepMeta grapgGeneratorStep) throws GraphGeneratorException {
    StringBuilder builder = new StringBuilder();
    GraphConfigurationInfo graphConfigInfoForFact = getGraphConfigInfoForFact(carbonDataLoadSchema);
    List<StepMeta> stepMetaList = new ArrayList<StepMeta>();
    xAxixLocation = 50;
    for (int i = 0; i < aggTableList.length; i++) {
      for (int j = 0; j < aggregateTable.length; j++) {
        if (aggregateTable[j].getAggregateTableName()
            .equals(aggTableList[i].getAggregateTableName())) {
          GraphConfigurationInfo graphConfigInfoForAGG =
              getGraphConfigInfoForCSVBasedAGG(aggregateTable[j], carbonDataLoadSchema);
          stepMetaList.add(getSliceMeregerStep(graphConfigInfoForAGG, graphConfigInfoForFact));
          builder.append(aggTableList[i].getAggregateTableName());
          xAxixLocation += 50;
          break;
        }
      }
    }

    grapgGeneratorStep.setDistributes(false);
    TransMeta trans = new TransMeta();
    trans.setName("Auto AGG KTR");
    trans.addStep(grapgGeneratorStep);
    for (int i = 0; i < stepMetaList.size(); i++) {
      trans.addStep(stepMetaList.get(i));
    }
    TransHopMeta hopMeta = null;
    for (int i = 0; i < stepMetaList.size(); i++) {
      hopMeta = new TransHopMeta(grapgGeneratorStep, stepMetaList.get(i));
      trans.addTransHop(hopMeta);
    }

    String graphFilePath =
        outputLocation + File.separator + schemaName + File.separator + cubeName + File.separator
            + builder.toString() + "graphgenerator" + ".ktr";
    generateGraphFile(trans, graphFilePath);
  }

  private String generateGraph(List<List<StepMeta>> stepMetaList,
      GraphConfigurationInfo configurationInfoFact, String aggTables, String factTableName,
      boolean isFactMdKeyInOutRow, boolean isManualCall, AggregateTable[] aggTablesArray,
      String factStorePath, int[] factCardinality, String[] aggregateLevels)
      throws GraphGeneratorException {
    StepMeta carbonFactReader = null;
    carbonFactReader =
        getCarbonFactReader(configurationInfoFact, true, isFactMdKeyInOutRow, aggTablesArray,
            factCardinality, factStorePath, aggregateLevels);
    carbonFactReader.setDistributes(false);
    TransMeta trans = new TransMeta();

    if (!isManualCall) {
      trans.setName("Auto AGG KTR");
    } else {
      trans.setName("Manual AGG KTR");
    }

    trans.addStep(carbonFactReader);
    List<StepMeta> list = null;
    int stepMetaSize = stepMetaList.size();
    int listSize = 0;

    for (int i = 0; i < stepMetaSize; i++) {
      list = stepMetaList.get(i);
      listSize = list.size();
      for (int j = 0; j < listSize; j++) {
        trans.addStep(list.get(j));
      }
    }

    TransHopMeta hopMeta = null;
    stepMetaSize = stepMetaList.size();
    for (int i = 0; i < stepMetaSize; i++) {
      list = stepMetaList.get(i);
      listSize = list.size();
      hopMeta = new TransHopMeta(carbonFactReader, list.get(0));
      trans.addTransHop(hopMeta);
      for (int j = 1; j < listSize; j++) {
        hopMeta = new TransHopMeta(list.get(j - 1), list.get(j));
        trans.addTransHop(hopMeta);
      }
    }

    String graphFilePath = null;
    if (!isManualCall) {
      graphFilePath =
          outputLocation + File.separator + schemaName + File.separator + cubeName + File.separator
              + factTableName + CarbonCommonConstants.CARBON_AUTO_AGG_CONST + aggTables + ".ktr";
    } else {
      graphFilePath =
          outputLocation + File.separator + schemaName + File.separator + cubeName + File.separator
              + aggTables + ".ktr";
    }

    generateGraphFile(trans, graphFilePath);
    return graphFilePath;
  }

  private List<StepMeta> generateStepMetaForAutoAgg(GraphConfigurationInfo configurationInfoFact,
      GraphConfigurationInfo configurationInfoAgg, AggregateTable aggregateTable,
      AggregateTable[] aggregateTableArray, int[] factCardinality, String factStorePath)
      throws GraphGeneratorException {
    List<StepMeta> stepMetaList = new ArrayList<StepMeta>(1);
    StepMeta carbonAggregateGeneratorStep =
        getCarbonAutoAggregateSurrogateKeyGeneratorStep(configurationInfoFact, configurationInfoAgg,
            aggregateTable, false, aggregateTableArray, factCardinality, factStorePath);
    stepMetaList.add(carbonAggregateGeneratorStep);
    StepMeta carbonSortRowAndGroupByStep =
        getCarbonSortRowAndGroupByStep(configurationInfoAgg, aggregateTable, factStorePath,
            configurationInfoFact, aggregateTableArray, factCardinality, false);
    stepMetaList.add(carbonSortRowAndGroupByStep);
    StepMeta carbonDataWriter =
        getCarbonDataWriter(configurationInfoAgg, configurationInfoFact, aggregateTableArray,
            factCardinality);
    stepMetaList.add(carbonDataWriter);
    return stepMetaList;
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
        outputLocation + File.separator + schemaInfo.getSchemaName() + File.separator
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
    fileInputMeta.setEncoding("UTF-8");
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
    csvInputMeta.setEncoding("UTF-8");
    csvInputMeta.setEnclosure("\"");
    csvInputMeta.setHeaderPresent(true);
    csvInputMeta.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
    StepMeta csvDataStep =
        new StepMeta(GraphGeneratorConstants.CSV_INPUT, (StepMetaInterface) csvInputMeta);
    csvDataStep.setLocation(100, 100);
    csvInputMeta.setFilenameField("filename");
    csvInputMeta.setLazyConversionActive(false);
    csvInputMeta.setBufferSize(instance.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
    //set blocks info id
    csvInputMeta.setBlocksID(this.blocksID);
    int copies = 1;
    try {
      copies = Integer.parseInt(instance.getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
          CarbonCommonConstants.DEFAULT_NUMBER_CORES));
    } catch (NumberFormatException e) {
      copies = Integer.parseInt(CarbonCommonConstants.DEFAULT_NUMBER_CORES);
    }
    //        if (copies > 1) {
    //            csvDataStep.setCopies(copies);
    //        }
    csvDataStep.setDraw(true);
    csvDataStep.setDescription("Read raw data from " + GraphGeneratorConstants.CSV_INPUT);

    return csvDataStep;
  }

  private StepMeta getFileNamesStep(GraphConfigurationInfo graphConfiguration)
      throws GraphGeneratorException {
    GetFileNamesMeta inputFileNamesMeta = new GetFileNamesMeta();
    // Init the Filename...
    inputFileNamesMeta.allocate(allocate);
    inputFileNamesMeta.setFilterFileType(0);
    inputFileNamesMeta.setFileRequired(new String[] { "N" });

    StepMeta inputFileNames =
        new StepMeta("Get File Names", (StepMetaInterface) inputFileNamesMeta);
    inputFileNames.setName("getFileNames");

    inputFileNames.setLocation(100, 100);
    inputFileNames.setDraw(true);
    inputFileNames.setDescription("Read raw data from " + GraphGeneratorConstants.CSV_INPUT);

    return inputFileNames;
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
    sliceMerger.setCubeName(schemaInfo.getCubeName());
    sliceMerger.setSchemaName(schemaInfo.getSchemaName());
    if (null != this.factStoreLocation) {
      sliceMerger.setCurrentRestructNumber(
          CarbonUtil.getRestructureNumber(this.factStoreLocation, this.factTableName));
    } else {
      sliceMerger.setCurrentRestructNumber(configurationInfo.getCurrentRestructNumber());
    }
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

  private StepMeta getAutoAggGraphGeneratorStep(String tableName, AggregateTable[] aggregateTable) {
    CarbonAutoAGGGraphGeneratorMeta meta = new CarbonAutoAGGGraphGeneratorMeta();
    meta.setCubeName(cubeName);
    meta.setSchemaName(schemaName);
    //TO-DO Generate graph for aggregate table from GraphGenerator instead of generating in
    // CarbonAutoAGGGraphGeneratorStep
    // meta.setSchema(schema.toXML());
    meta.setIsHDFSMode(isHDFSReadMode + "");
    meta.setFactStoreLocation(factStoreLocation);
    meta.setCurrentRestructNumber(currentRestructNumber);
    meta.setPartitionId(null == partitionID ? "" : partitionID);
    meta.setAggTables(this.tableName);
    meta.setFactTableName(tableName);
    meta.setAutoMode(Boolean.toString(schemaInfo.isAutoAggregateRequest()));
    meta.setLoadNames(this.loadNames);
    meta.setModificationOrDeletionTime(this.modificationOrDeletionTime);

    StepMeta aggTableMeta = new StepMeta(GraphGeneratorConstants.CARBON_AUTO_AGG_GRAPH_GENERATOR,
        (StepMetaInterface) meta);
    aggTableMeta.setStepID(GraphGeneratorConstants.CARBON_AUTO_AGG_GRAPH_GENERATOR_ID);
    xAxixLocation += 120;
    aggTableMeta.setLocation(xAxixLocation, yAxixLocation);
    aggTableMeta.setDraw(true);
    aggTableMeta
        .setDescription("SliceMerger: " + GraphGeneratorConstants.CARBON_AUTO_AGG_GRAPH_GENERATOR);
    return aggTableMeta;
  }

  private StepMeta getCarbonFactReader(GraphConfigurationInfo configurationInfo,
      boolean isReadOnlyInProgress, boolean isFactMdkeyInOutputRow, AggregateTable[] aggTables,
      int[] dimLens, String storeLocation, String[] aggregateLevels) {
    CarbonFactReaderMeta factReader = new CarbonFactReaderMeta();
    factReader.setDefault();
    factReader.setTableName(configurationInfo.getTableName());
    factReader.setAggregateTableName(tableName);
    factReader.setCubeName(cubeName);
    factReader.setSchemaName(schemaName);
    //Pass all information required in CarbonFactReaderStep
    //factReader.setSchema(schema.toXML());
    factReader.setPartitionID(partitionID);
    factReader.setLoadNames(this.loadNames);
    factReader.setModificationOrDeletionTime(this.modificationOrDeletionTime);
    String[] factDimensions = configurationInfo.getDimensions();
    factReader.setDimLensString(CarbonDataProcessorUtil.getLevelCardinalitiesString(dimLens));
    SliceMetaData sliceMetaData =
        CarbonDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
    int[] globalDimensioncardinality = CarbonDataProcessorUtil
        .getKeyGenerator(factDimensions, aggregateLevels, dimLens, sliceMetaData.getNewDimensions(),
            sliceMetaData.getNewDimLens());
    factReader.setGlobalDimLensString(
        CarbonDataProcessorUtil.getLevelCardinalitiesString(globalDimensioncardinality));

    factReader.setFactStoreLocation(storeLocation);

    factReader.setReadOnlyInProgress(!isReadOnlyInProgress + "");
    factReader.setMeasureCountString(configurationInfo.getMeasureCount());
    char[] type = configurationInfo.getType();
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < type.length - 1; i++) {
      builder.append(type[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(type[type.length - 1]);
    factReader.setAggTypeString(builder.toString());

    if (isColumnar) {
      Set<String> aggLevels = new LinkedHashSet<String>();
      for (int i = 0; i < aggTables.length; i++) {
        String[] aggLevelsActualName = aggTables[i].getAggLevelsActualName();
        for (int j = 0; j < aggLevelsActualName.length; j++) {
          aggLevels.add(aggLevelsActualName[j]);
        }
      }

      int[] blockIndex = new int[aggLevels.size()];
      int index = 0;
      for (int j = 0; j < factDimensions.length; j++) {
        if (aggLevels.contains(factDimensions[j])) {
          blockIndex[index++] = j;
        }
      }
      builder = new StringBuilder();
      for (int i = 0; i < blockIndex.length - 1; i++) {
        builder.append(blockIndex[i]);
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
      builder.append(blockIndex[blockIndex.length - 1]);
      factReader.setBlockIndexString(builder.toString());
    } else {
      factReader.setBlockIndexString(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    StepMeta factReaderMeta =
        new StepMeta(GraphGeneratorConstants.CARBON_FACT_READER, (StepMetaInterface) factReader);
    factReaderMeta.setStepID(GraphGeneratorConstants.CARBON_FACT_READER_ID);
    xAxixLocation += 120;
    factReaderMeta.setLocation(xAxixLocation, yAxixLocation);
    factReaderMeta.setDraw(true);
    factReaderMeta
        .setDescription("Carbon Fact Reader : " + GraphGeneratorConstants.CARBON_FACT_READER);
    return factReaderMeta;
  }

  private StepMeta getCarbonAutoAggregateSurrogateKeyGeneratorStep(
      GraphConfigurationInfo configurationInfoFact, GraphConfigurationInfo configurationInfoAgg,
      AggregateTable aggregateTable, boolean isManualAggregationRequest,
      AggregateTable[] aggTableArray, int[] factCardinality, String factStorePath) {
    CarbonAggregateSurrogateGeneratorMeta meta = new CarbonAggregateSurrogateGeneratorMeta();
    meta.setHeirAndDimLens(configurationInfoFact.getHeirAndDimLens());
    meta.setHeirAndKeySize(configurationInfoFact.getHeirAndKeySizeString());
    meta.setAggDimeLensString(CarbonDataProcessorUtil
        .getLevelCardinalitiesString(configurationInfoAgg.getDimCardinalities(),
            configurationInfoAgg.getDimensions()));
    meta.setFactStorePath(factStorePath);

    String[] aggType = configurationInfoAgg.getAggType();
    StringBuilder builder = new StringBuilder();
    String[] factDimensions = configurationInfoFact.getDimensions();
    if (isColumnar) {
      factDimensions = getFactDim(aggTableArray, factDimensions);
    }
    boolean isMdkeyInOutRowRequired = false;

    for (int i = 0; i < aggType.length; i++) {
      if (aggType[i].equals(CarbonCommonConstants.CUSTOM)) {
        isMdkeyInOutRowRequired = true;
      }
      builder.append(aggType[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(aggType[aggType.length - 1]);
    meta.setAggregatorString(builder.toString());
    meta.setMdkeyInOutRowRequired(isMdkeyInOutRowRequired + "");
    builder = new StringBuilder();

    for (int i = 0; i < factDimensions.length - 1; i++) {
      builder.append(factDimensions[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(factDimensions[factDimensions.length - 1]);
    meta.setFactLevelsString(builder.toString());

    if (this.factTableName.equals(configurationInfoFact.getTableName())) {
      meta.setFactMeasureString(configurationInfoFact.getMeasureUniqueColumnNamesString());
    } else {
      meta.setFactMeasureString(configurationInfoFact.getMeasureNamesString());
    }

    meta.setFactTableName(configurationInfoFact.getTableName());
    meta.setIsManualAutoAggRequest(isManualAggregationRequest + "");

    String[] actualAggLevelsActualName = aggregateTable.getAggLevelsActualName();
    builder = new StringBuilder();

    for (int i = 0; i < actualAggLevelsActualName.length - 1; i++) {
      builder.append(actualAggLevelsActualName[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(actualAggLevelsActualName[actualAggLevelsActualName.length - 1]);
    meta.setAggregateLevelsString(builder.toString());
    builder = new StringBuilder();
    String[] aggName = aggregateTable.getAggColuName();
    for (int i = 0; i < aggName.length - 1; i++) {
      builder.append(aggName[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    builder.append(aggName[aggName.length - 1]);
    meta.setAggregateMeasuresString(builder.toString());

    builder = new StringBuilder();
    String[] aggMeasure = aggregateTable.getAggMeasure();

    for (int i = 0; i < aggMeasure.length - 1; i++) {
      builder.append(aggMeasure[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(aggMeasure[aggMeasure.length - 1]);
    meta.setAggregateMeasuresColumnNameString(builder.toString());
    meta.setTableName(configurationInfoAgg.getTableName());
    meta.setSchemaName(schemaName);
    meta.setCubeName(cubeName);
    meta.setMeasureCountString(configurationInfoFact.getMeasureCount());
    meta.setFactTableName(configurationInfoFact.getTableName());
    meta.setFactDimLensString(CarbonDataProcessorUtil.getLevelCardinalitiesString(factCardinality));
    StepMeta mdkeyStepMeta = new StepMeta(
        GraphGeneratorConstants.CARBON_AGGREGATE_SURROGATE_GENERATOR + configurationInfoAgg
            .getTableName(), (StepMetaInterface) meta);
    mdkeyStepMeta.setName(
        GraphGeneratorConstants.CARBON_AGGREGATE_SURROGATE_GENERATOR + configurationInfoAgg
            .getTableName());
    mdkeyStepMeta.setStepID(GraphGeneratorConstants.CARBON_AGGREGATE_SURROGATE_GENERATOR_ID);
    xAxixLocation += 120;
    mdkeyStepMeta.setLocation(xAxixLocation, yAxixLocation);
    mdkeyStepMeta.setDraw(true);
    mdkeyStepMeta.setDescription(
        "Generate Aggregate Data " + GraphGeneratorConstants.CARBON_AGGREGATE_SURROGATE_GENERATOR
            + configurationInfoAgg.getTableName());
    return mdkeyStepMeta;
  }

  private String[] getFactDim(AggregateTable[] aggTableArray, String[] factDimensions) {
    Set<String> aggLevels = new LinkedHashSet<String>();
    for (int i = 0; i < aggTableArray.length; i++) {
      String[] aggLevelsActualName = aggTableArray[i].getAggLevelsActualName();
      for (int j = 0; j < aggLevelsActualName.length; j++) {
        aggLevels.add(aggLevelsActualName[j]);
      }
    }
    String[] selectedFactDims = new String[aggLevels.size()];
    int index = 0;
    for (int j = 0; j < factDimensions.length; j++) {
      if (aggLevels.contains(factDimensions[j])) {
        selectedFactDims[index++] = factDimensions[j];
      }
    }
    factDimensions = selectedFactDims;
    return factDimensions;
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
    seqMeta.setBatchSize(Integer.parseInt(graphConfiguration.getBatchSize()));
    seqMeta.setNoDictionaryDims(graphConfiguration.getNoDictionaryDims());
    seqMeta.setCubeName(schemaInfo.getCubeName());
    seqMeta.setSchemaName(schemaInfo.getSchemaName());
    seqMeta.setComplexDelimiterLevel1(schemaInfo.getComplexDelimiterLevel1());
    seqMeta.setComplexDelimiterLevel2(schemaInfo.getComplexDelimiterLevel2());
    seqMeta.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
    seqMeta.setCarbonMetaHier(graphConfiguration.getMetaHeirString());
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
    seqMeta.setModifiedDimension(modifiedDimension);
    seqMeta.setForeignKeyHierarchyString(graphConfiguration.getForeignKeyHierarchyString());
    seqMeta.setPrimaryKeysString(graphConfiguration.getPrimaryKeyString());
    seqMeta.setCarbonMeasureNames(graphConfiguration.getMeasureNamesString());
    seqMeta.setHeirNadDimsLensString(graphConfiguration.getHeirAndDimLens());
    seqMeta.setActualDimNames(graphConfiguration.getActualDimensionColumns());
    seqMeta.setNormHiers(graphConfiguration.getNormHiers());
    seqMeta.setHeirKeySize(graphConfiguration.getHeirAndKeySizeString());
    seqMeta.setDirectDictionaryColumns(graphConfiguration.getDirectDictionaryColumnString());
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
    seqMeta.setCheckPointFileExits("false");
    String checkPointFile =
        outputLocation + File.separator + graphConfiguration.getStoreLocation() + File.separator
            + CarbonCommonConstants.CHECKPOINT_FILE_NAME + CarbonCommonConstants.CHECKPOINT_EXT;
    File file = new File(checkPointFile);
    if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
      if (file.exists()) {
        seqMeta.setCheckPointFileExits("true");
      }
    }
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
    carbonMdKey.setPartitionID(partitionID);
    carbonMdKey.setSegmentId(segmentId);
    carbonMdKey.setNumberOfCores(graphConfiguration.getNumberOfCores());
    carbonMdKey.setTableName(graphConfiguration.getTableName());
    carbonMdKey.setSchemaName(schemaInfo.getSchemaName());
    carbonMdKey.setCubeName(schemaInfo.getCubeName());
    carbonMdKey.setComplexTypeString(graphConfiguration.getComplexTypeString());
    carbonMdKey.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
    carbonMdKey.setAggregateLevels(CarbonDataProcessorUtil
        .getLevelCardinalitiesString(graphConfiguration.getDimCardinalities(),
            graphConfiguration.getDimensions()));

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

  private StepMeta getCarbonDataWriter(GraphConfigurationInfo graphConfiguration,
      GraphConfigurationInfo graphjConfigurationForFact, AggregateTable[] aggTableArray,
      int[] factDimCardinality) {
    //
    CarbonDataWriterStepMeta carbonDataWriter = getCarbonDataWriter(graphConfiguration);
    String[] factDimensions = graphjConfigurationForFact.getDimensions();
    carbonDataWriter.setCurrentRestructNumber(
        CarbonUtil.getRestructureNumber(this.factStoreLocation, this.factTableName));

    // getting the high cardinality string from graphjConfigurationForFact.
    String[] NoDictionaryDims = RemoveDictionaryUtil
        .extractNoDictionaryDimsArr(graphjConfigurationForFact.getNoDictionaryDims());
    // using the string [] of high card dims , trying to get the count of the high cardinality
    // dims in the agg query.
    carbonDataWriter.setNoDictionaryCount(
        getNoDictionaryDimsCountInAggQuery(NoDictionaryDims, graphConfiguration.getDimensions()));

    processAutoAggRequest(graphConfiguration, graphjConfigurationForFact, carbonDataWriter);

    if (null != factDimCardinality) {
      SliceMetaData sliceMetaData =
          CarbonDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
      int[] globalDimensioncardinality = CarbonDataProcessorUtil
          .getKeyGenerator(factDimensions, graphConfiguration.getDimensions(), factDimCardinality,
              sliceMetaData.getNewDimensions(), sliceMetaData.getNewDimLens());
      carbonDataWriter.setFactDimLensString(
          CarbonDataProcessorUtil.getLevelCardinalitiesString(globalDimensioncardinality));
    }

    carbonDataWriter.setIsUpdateMemberRequest(isUpdateMemberRequest + "");
    StringBuilder builder = new StringBuilder();

    String[] dimensions = graphConfiguration.getDimensions();
    for (int i = 0; i < dimensions.length - 1; i++) {
      builder.append(dimensions[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    builder.append(dimensions[dimensions.length - 1]);
    String mdkeySize = graphConfiguration.getMdkeySize();
    if (isAutoAggRequest) {
      mdkeySize =
          getMdkeySizeInCaseOfAutoAggregate(graphConfiguration.getDimensions(), factDimensions,
              factDimCardinality);
    }
    carbonDataWriter.setMdkeyLength(mdkeySize);
    carbonDataWriter.setAggregateLevelsString(builder.toString());
    builder = new StringBuilder();
    for (int i = 0; i < factDimensions.length - 1; i++) {
      builder.append(factDimensions[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    builder.append(factDimensions[factDimensions.length - 1]);
    carbonDataWriter.setFactLevelsString(builder.toString());
    //
    StepMeta carbonDataWriterStep =
        new StepMeta(GraphGeneratorConstants.CARBON_DATA_WRITER + graphConfiguration.getTableName(),
            (StepMetaInterface) carbonDataWriter);
    carbonDataWriterStep.setStepID(GraphGeneratorConstants.CARBON_DATA_WRITER_ID);
    xAxixLocation += 120;
    carbonDataWriterStep.setLocation(xAxixLocation, yAxixLocation);
    carbonDataWriterStep.setDraw(true);
    carbonDataWriterStep
        .setName(GraphGeneratorConstants.CARBON_DATA_WRITER + graphConfiguration.getTableName());
    carbonDataWriterStep.setDescription(
        "Write Carbon Data To File: " + GraphGeneratorConstants.CARBON_DATA_WRITER
            + graphConfiguration.getTableName());
    return carbonDataWriterStep;
  }

  private void processAutoAggRequest(GraphConfigurationInfo graphConfiguration,
      GraphConfigurationInfo graphjConfigurationForFact,
      CarbonDataWriterStepMeta carbonDataWriter) {
    boolean isFactMdkeyInInputRow = false;
    if (isAutoAggRequest) {
      String[] aggType = graphConfiguration.getAggType();
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < aggType.length - 1; i++) {
        builder.append(aggType[i]);
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
      builder.append(aggType[aggType.length - 1]);
      carbonDataWriter.setAggregatorString(builder.toString());
      String[] aggClass = graphConfiguration.getAggClass();
      builder = new StringBuilder();
      for (int i = 0; i < aggClass.length - 1; i++) {
        builder.append(aggClass[i]);
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
      builder.append(aggClass[aggClass.length - 1]);
      carbonDataWriter.setAggregatorClassString(builder.toString());

      for (int i = 0; i < aggType.length; i++) {
        if (aggType[i].equals(CarbonCommonConstants.CUSTOM)) {
          isFactMdkeyInInputRow = true;
          break;
        }
      }
      if (isUpdateMemberRequest) {
        isFactMdkeyInInputRow = false;
      }
      carbonDataWriter.setIsFactMDkeyInInputRow(isFactMdkeyInInputRow + "");
      if (isFactMdkeyInInputRow) {
        carbonDataWriter.setFactMdkeyLength(graphjConfigurationForFact.getMdkeySize());
      } else {
        carbonDataWriter.setFactMdkeyLength(0 + "");
      }
    } else {
      carbonDataWriter.setAggregatorClassString(CarbonCommonConstants.HASH_SPC_CHARACTER);
      carbonDataWriter.setAggregatorString(CarbonCommonConstants.HASH_SPC_CHARACTER);
      carbonDataWriter.setIsFactMDkeyInInputRow(isFactMdkeyInInputRow + "");
      carbonDataWriter.setFactMdkeyLength(0 + "");

    }
  }

  private CarbonDataWriterStepMeta getCarbonDataWriter(GraphConfigurationInfo graphConfiguration) {
    CarbonDataWriterStepMeta carbonDataWriter = new CarbonDataWriterStepMeta();
    carbonDataWriter.setCubeName(cubeName);
    carbonDataWriter.setSchemaName(schemaName);
    carbonDataWriter.setBlockletSize(graphConfiguration.getBlockletSize());
    carbonDataWriter.setMaxBlocklet(graphConfiguration.getMaxBlockletInFile());
    carbonDataWriter.setTabelName(graphConfiguration.getTableName());
    carbonDataWriter.setMeasureCount(graphConfiguration.getMeasureCount());
    carbonDataWriter.setGroupByEnabled("true");
    return carbonDataWriter;
  }

  private String getMdkeySizeInCaseOfAutoAggregate(String[] aggreateLevels, String[] factLevels,
      int[] factDimCardinality) {
    int[] surrogateIndex = new int[aggreateLevels.length];
    Arrays.fill(surrogateIndex, -1);
    for (int i = 0; i < aggreateLevels.length; i++) {
      for (int j = 0; j < factLevels.length; j++) {
        if (aggreateLevels[i].equals(factLevels[j])) {
          surrogateIndex[i] = j;
          break;
        }
      }
    }
    SliceMetaData sliceMetaData =
        CarbonDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
    int[] factTableDimensioncardinality = CarbonDataProcessorUtil
        .getKeyGenerator(factLevels, aggreateLevels, factDimCardinality,
            sliceMetaData.getNewDimensions(), sliceMetaData.getNewDimLens());
    KeyGenerator keyGenerator = KeyGeneratorFactory.getKeyGenerator(factTableDimensioncardinality);
    int[] maskedByteRanges = CarbonDataProcessorUtil.getMaskedByte(surrogateIndex, keyGenerator);
    return maskedByteRanges.length + "";
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

  /*
   * Obselete
   */
  private StepMeta getCarbonSortRowAndGroupByStep(GraphConfigurationInfo graphConfiguration,
      AggregateTable aggregateTable, String factStorePath,
      GraphConfigurationInfo configurationInfoFact, AggregateTable[] aggTableArray,
      int[] factCardinality, boolean isManualAggregationRequest) throws GraphGeneratorException {
    CarbonSortKeyAndGroupByStepMeta sortRowsMeta = new CarbonSortKeyAndGroupByStepMeta();
    sortRowsMeta.setTabelName(graphConfiguration.getTableName());
    sortRowsMeta.setCubeName(cubeName);
    sortRowsMeta.setSchemaName(schemaName);
    sortRowsMeta.setCurrentRestructNumber(
        CarbonUtil.getRestructureNumber(this.factStoreLocation, this.factTableName));

    String[] NoDictionaryDims = RemoveDictionaryUtil
        .extractNoDictionaryDimsArr(configurationInfoFact.getNoDictionaryDims());
    sortRowsMeta.setNoDictionaryCount(
        getNoDictionaryDimsCountInAggQuery(NoDictionaryDims, graphConfiguration.getDimensions()));
    sortRowsMeta.setIsAutoAggRequest(isAutoAggRequest + "");
    boolean isFactMdKeyInInputRow = false;
    StringBuilder builder = null;
    if (isAutoAggRequest) {
      String[] aggType = graphConfiguration.getAggType();
      builder = new StringBuilder();
      for (int i = 0; i < aggType.length - 1; i++) {
        builder.append(aggType[i]);
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
        if (aggType[i].equals(CarbonCommonConstants.CUSTOM)) {
          isFactMdKeyInInputRow = true;
          break;
        }
      }
      if (isUpdateMemberRequest) {
        isFactMdKeyInInputRow = false;
      }
      builder.append(aggType[aggType.length - 1]);
      sortRowsMeta.setAggregatorString(builder.toString());
      String[] aggClass = graphConfiguration.getAggClass();
      builder = new StringBuilder();
      for (int i = 0; i < aggClass.length - 1; i++) {
        builder.append(aggClass[i]);
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
      builder.append(aggClass[aggClass.length - 1]);
      sortRowsMeta.setAggregatorClassString(builder.toString());
    } else {
      sortRowsMeta.setAggregatorClassString(CarbonCommonConstants.HASH_SPC_CHARACTER);
      sortRowsMeta.setAggregatorString(CarbonCommonConstants.HASH_SPC_CHARACTER);
      sortRowsMeta.setFactDimLensString(CarbonCommonConstants.COMA_SPC_CHARACTER);

    }
    String[] factDimensions = configurationInfoFact.getDimensions();
    if (isColumnar) {
      Set<String> aggLevels = new LinkedHashSet<String>();
      for (int i = 0; i < aggTableArray.length; i++) {
        String[] aggLevelsActualName = aggTableArray[i].getAggLevelsActualName();
        for (int j = 0; j < aggLevelsActualName.length; j++) {
          aggLevels.add(aggLevelsActualName[j]);
        }
      }
    }
    String mdkeySize =
        getMdkeySizeInCaseOfAutoAggregate(graphConfiguration.getDimensions(), factDimensions,
            factCardinality);
    sortRowsMeta.setMdkeyLength(mdkeySize);
    SliceMetaData sliceMetaData =
        CarbonDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
    int[] globalDimensioncardinality = CarbonDataProcessorUtil
        .getKeyGenerator(factDimensions, graphConfiguration.getDimensions(), factCardinality,
            sliceMetaData.getNewDimensions(), sliceMetaData.getNewDimLens());
    sortRowsMeta.setFactDimLensString(
        CarbonDataProcessorUtil.getLevelCardinalitiesString(globalDimensioncardinality));
    builder = new StringBuilder();
    for (int i = 0; i < factDimensions.length - 1; i++) {
      builder.append(factDimensions[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(factDimensions[factDimensions.length - 1]);
    sortRowsMeta.setFactLevelsString(builder.toString());

    builder = new StringBuilder();
    String[] aggMeasure = aggregateTable.getAggMeasure();

    for (int i = 0; i < aggMeasure.length - 1; i++) {
      builder.append(aggMeasure[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(aggMeasure[aggMeasure.length - 1]);
    sortRowsMeta.setAggregateMeasuresColumnNameString(builder.toString());

    builder = new StringBuilder();
    String[] aggName = aggregateTable.getAggColuName();
    for (int i = 0; i < aggName.length - 1; i++) {
      builder.append(aggName[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    builder.append(aggName[aggName.length - 1]);
    sortRowsMeta.setAggregateMeasuresString(builder.toString());
    sortRowsMeta.setFactStorePath(factStorePath);
    sortRowsMeta.setFactTableName(configurationInfoFact.getTableName());
    if (this.factTableName.equals(configurationInfoFact.getTableName())) {
      sortRowsMeta.setFactMeasureString(configurationInfoFact.getMeasureUniqueColumnNamesString());
    } else {
      sortRowsMeta.setFactMeasureString(configurationInfoFact.getMeasureNamesString());
    }
    String[] actualAggLevelsActualName = aggregateTable.getAggLevelsActualName();
    builder = new StringBuilder();

    for (int i = 0; i < actualAggLevelsActualName.length - 1; i++) {
      builder.append(actualAggLevelsActualName[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(actualAggLevelsActualName[actualAggLevelsActualName.length - 1]);
    sortRowsMeta.setAggregateLevelsString(builder.toString());
    sortRowsMeta.setFactMdKeyInInputRow(isFactMdKeyInInputRow + "");
    sortRowsMeta.setMeasureCount(graphConfiguration.getMeasureCount() + "");
    sortRowsMeta.setIsUpdateMemberRequest(isUpdateMemberRequest + "");
    // sortRowsMeta.setMeasureCount(linkedHashSet.size()+"");
    // sortRowsMeta.setMdKeyLength(graphConfiguration.getMdkeySize());
    sortRowsMeta.setHeirAndDimLens(configurationInfoFact.getHeirAndDimLens());
    sortRowsMeta.setHeirAndKeySize(configurationInfoFact.getHeirAndKeySizeString());
    sortRowsMeta.setIsManualAutoAggRequest(isManualAggregationRequest + "");
    StepMeta sortRowsStep = new StepMeta(
        GraphGeneratorConstants.SORT_KEY_AND_GROUPBY + graphConfiguration.getTableName(),
        (StepMetaInterface) sortRowsMeta);
    xAxixLocation += 120;
    sortRowsStep.setLocation(xAxixLocation, yAxixLocation);
    sortRowsStep.setDraw(true);
    sortRowsStep.setStepID(GraphGeneratorConstants.CARBON_SORTKEY_AND_GROUPBY_ID);
    sortRowsStep.setDescription(
        "Sort Key: " + GraphGeneratorConstants.SORT_KEY_AND_GROUPBY + graphConfiguration
            .getTableName());
    sortRowsStep.setName(
        "Sort Key: " + GraphGeneratorConstants.SORT_KEY_AND_GROUPBY + graphConfiguration
            .getTableName());
    return sortRowsStep;
  }

  private StepMeta getSortStep(GraphConfigurationInfo graphConfiguration)
      throws GraphGeneratorException {
    String[] actualMeasures = graphConfiguration.getMeasures();

    SortKeyStepMeta sortRowsMeta = new SortKeyStepMeta();
    sortRowsMeta.setPartitionID(partitionID);
    sortRowsMeta.setSegmentId(segmentId);
    sortRowsMeta.setTaskNo(taskNo);
    sortRowsMeta.setTabelName(graphConfiguration.getTableName());
    sortRowsMeta.setCubeName(schemaInfo.getCubeName());
    sortRowsMeta.setSchemaName(schemaInfo.getSchemaName());
    sortRowsMeta.setOutputRowSize(actualMeasures.length + 1 + "");
    sortRowsMeta.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
    sortRowsMeta.setDimensionCount(graphConfiguration.getDimensions().length + "");
    sortRowsMeta.setComplexDimensionCount(graphConfiguration.getComplexTypeString().isEmpty() ?
        "0" :
        graphConfiguration.getComplexTypeString()
            .split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER).length + "");
    sortRowsMeta.setIsUpdateMemberRequest(isUpdateMemberRequest + "");
    sortRowsMeta.setMeasureCount(graphConfiguration.getMeasureCount() + "");
    sortRowsMeta.setNoDictionaryDims(graphConfiguration.getNoDictionaryDims());
    sortRowsMeta.setMeasureDataType(graphConfiguration.getMeasureDataTypeInfo());
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
    graphConfiguration.setCurrentRestructNumber(currentRestructNumber);
    List<CarbonDimension> dimensions = carbonDataLoadSchema.getCarbonTable()
        .getDimensionByTableName(carbonDataLoadSchema.getCarbonTable().getFactTableName());
    graphConfiguration
        .setDimensions(CarbonSchemaParser.getCubeDimensions(dimensions, carbonDataLoadSchema));
    graphConfiguration
        .setActualDims(CarbonSchemaParser.getCubeDimensions(dimensions, carbonDataLoadSchema));
    graphConfiguration.setComplexTypeString(CarbonSchemaParser.getComplexTypeString(dimensions));
    graphConfiguration.setDirectDictionaryColumnString(
        CarbonSchemaParser.getDirectDictionaryColumnString(dimensions, carbonDataLoadSchema));
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
        CarbonSchemaParser.getTableNameString(factTableName, dimensions, carbonDataLoadSchema);
    String dimensionColumnIds = CarbonSchemaParser.getColumnIdString(dimensions);
    graphConfiguration.setDimensionTableNames(tableString);
    graphConfiguration.setDimensionString(dimString.toString());
    graphConfiguration.setDimensionColumnIds(dimensionColumnIds);
    StringBuilder propString = new StringBuilder();
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
    graphConfiguration
        .setColumnGroupsString(CarbonSchemaParser.getColumnGroups(dimensions));
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
    //TODO: It will always be empty
    //graphConfiguration.setNormHiers(CarbonSchemaParser.getNormHiers(cube, schema));
    graphConfiguration.setMeasureDataTypeInfo(CarbonSchemaParser.getMeasuresDataType(measures));

    graphConfiguration.setStoreLocation(
        this.schemaName + '/' + carbonDataLoadSchema.getCarbonTable().getFactTableName());
    graphConfiguration.setBlockletSize(
        (instance.getProperty("com.huawei.unibi.carbon.blocklet.size", DEFAUL_BLOCKLET_SIZE)));
    graphConfiguration.setMaxBlockletInFile(
        (instance.getProperty("carbon.max.blocklet.in.file", DEFAULE_MAX_BLOCKLET_IN_FILE)));
    graphConfiguration.setNumberOfCores(
        (instance.getProperty(CarbonCommonConstants.NUM_CORES_LOADING, DEFAULT_NUMBER_CORES)));

    // check quotes required in query or Not
    boolean isQuotesRequired = true;
    String quote = CarbonSchemaParser.QUOTES;
    if (null != schemaInfo.getSrcDriverName()) {
      quote = getQuoteType(schemaInfo);
    }
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
    graphConfiguration.setUsername(schemaInfo.getSrcUserName());
    graphConfiguration.setPassword(schemaInfo.getSrcPwd());
    graphConfiguration.setDriverclass(schemaInfo.getSrcDriverName());
    graphConfiguration.setConnectionUrl(schemaInfo.getSrcConUrl());
    return graphConfiguration;
  }

  private GraphConfigurationInfo getGraphConfigInfoForCSVBasedAGG(AggregateTable aggtable,
      CarbonDataLoadSchema schema) throws GraphGeneratorException {
    GraphConfigurationInfo graphConfiguration = new GraphConfigurationInfo();
    graphConfiguration.setCurrentRestructNumber(currentRestructNumber);
    List<CarbonDimension> dimensions = carbonDataLoadSchema.getCarbonTable()
        .getDimensionByTableName(carbonDataLoadSchema.getCarbonTable().getFactTableName());
    graphConfiguration.setDimensions(aggtable.getAggLevelsActualName());
    graphConfiguration.setActualDims(aggtable.getAggLevels());
    graphConfiguration.setMeasures(aggtable.getAggMeasure());
    graphConfiguration.setAggClass(aggtable.getAggregateClass());
    String factTableName = schema.getCarbonTable().getFactTableName();
    Map<String, String> cardinalities = CarbonSchemaParser.getCardinalities(dimensions, schema);
    graphConfiguration.setDimCardinalities(cardinalities);
    graphConfiguration.setDimensionTableNames(
        CarbonSchemaParser.getTableNameString(factTableName, dimensions, schema));
    graphConfiguration.setTableName(aggtable.getAggregateTableName());
    StringBuilder dimString = new StringBuilder();
    graphConfiguration.setLevelAnddataType(CarbonCommonConstants.HASH_SPC_CHARACTER);

    String[] levelWithTableName = aggtable.getActualAggLevels();
    String[] actualLevalName = aggtable.getAggLevels();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < levelWithTableName.length - 1; i++) {
      builder.append(actualLevalName[i]);
      builder.append(CarbonCommonConstants.HYPHEN_SPC_CHARACTER);
      builder.append(levelWithTableName[i]);
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    builder.append(actualLevalName[actualLevalName.length - 1]);
    builder.append(CarbonCommonConstants.HYPHEN_SPC_CHARACTER);
    builder.append(levelWithTableName[actualLevalName.length - 1]);
    graphConfiguration.setColumnAndTableNameColumnMapForAgg(builder.toString());

    int currentCount = CarbonSchemaParser
        .getDimensionStringForAgg(aggtable.getActualAggLevels(), dimString, 0, cardinalities,
            aggtable.getAggLevelsActualName());
    graphConfiguration.setDimensionString(dimString.toString());
    graphConfiguration.setMeasuresString(
        CarbonSchemaParser.getMeasureStringForAgg(aggtable.getAggMeasure(), currentCount));

    graphConfiguration.setMeasureCount(aggtable.getAggMeasure().length + "");
    graphConfiguration.setMdkeySize(
        CarbonSchemaParser.getMdkeySizeForAgg(aggtable.getAggLevelsActualName(), cardinalities));
    graphConfiguration.setHeirAndKeySizeString(
        CarbonSchemaParser.getHeirAndKeySizeMapForFact(dimensions, schema));
    //
    graphConfiguration.setConnectionName("target");
    graphConfiguration
        .setHeirAndDimLens(CarbonSchemaParser.getHeirAndCardinalityString(dimensions, schema));
    graphConfiguration.setMeasureNamesString(
        CarbonSchemaParser.getMeasuresNamesStringForAgg(aggtable.getAggColuName()));
    graphConfiguration.setHierColumnString(
        CarbonSchemaParser.getHierarchyStringWithColumnNames(dimensions, schema));
    graphConfiguration.setForeignKeyHierarchyString(
        CarbonSchemaParser.getForeignKeyHierarchyString(dimensions, schema, factTableName));
    graphConfiguration.setActualDimensionColumns(
        CarbonSchemaParser.getActualDimensionsForAggregate(aggtable.getAggLevels()));
    char[] type = new char[aggtable.getAggregator().length];
    Arrays.fill(type, 'n');
    for (int i = 0; i < type.length; i++) {
      if (aggtable.getAggregator()[i].equals(CarbonCommonConstants.DISTINCT_COUNT) || aggtable
          .getAggregator()[i].equals(CarbonCommonConstants.CUSTOM)) {
        type[i] = 'c';
      }
    }
    graphConfiguration.setType(type);
    graphConfiguration.setAggType(aggtable.getAggregator());
    graphConfiguration.setStoreLocation(
        this.schemaName + '/' + carbonDataLoadSchema.getCarbonTable().getFactTableName());
    graphConfiguration.setBlockletSize(
        (instance.getProperty("com.huawei.unibi.carbon.blocklet.size", DEFAUL_BLOCKLET_SIZE)));
    graphConfiguration.setMaxBlockletInFile(
        (instance.getProperty("carbon.max.blocklet.in.file", DEFAULE_MAX_BLOCKLET_IN_FILE)));
    graphConfiguration.setNumberOfCores(
        (instance.getProperty(CarbonCommonConstants.NUM_CORES_LOADING, DEFAULT_NUMBER_CORES)));

    // check quotes required in query or Not
    boolean isQuotesRequired = false;
    if (null != schemaInfo.getSrcDriverName()) {
      isQuotesRequired = isQuotesRequired(schemaInfo);
    }

    //
    graphConfiguration.setTableInputSqlQuery(CarbonSchemaParser
        .getTableInputSQLQueryForAGG(aggtable.getAggLevels(), aggtable.getAggMeasure(),
            aggtable.getAggregateTableName(), isQuotesRequired));
    graphConfiguration
        .setBatchSize((instance.getProperty("carbon.batch.size", DEFAULT_BATCH_SIZE)));
    graphConfiguration.setSortSize((instance.getProperty("carbon.sort.size", DEFAULT_SORT_SIZE)));
    //
    graphConfiguration.setAGG(true);
    graphConfiguration.setUsername(schemaInfo.getSrcUserName());
    graphConfiguration.setPassword(schemaInfo.getSrcPwd());
    graphConfiguration.setDriverclass(schemaInfo.getSrcDriverName());
    graphConfiguration.setConnectionUrl(schemaInfo.getSrcConUrl());
    return graphConfiguration;
  }

  private boolean isQuotesRequired(SchemaInfo schemaInfo) throws GraphGeneratorException {
    String driverCls = schemaInfo.getSrcDriverName();
    String type = DRIVERS.get(driverCls);

    if (null == type) {
      LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
          "Driver : \"" + driverCls + " \"Not Supported.");
      throw new GraphGeneratorException("Driver : \"" + driverCls + " \"Not Supported.");
    }

    if (type.equals(CarbonCommonConstants.TYPE_ORACLE) || type
        .equals(CarbonCommonConstants.TYPE_MSSQL)) {
      return true;
    } else if (type.equals(CarbonCommonConstants.TYPE_MYSQL)) {
      return true;
    }

    return true;
  }

  private String getQuoteType(SchemaInfo schemaInfo) throws GraphGeneratorException {
    String driverClass = schemaInfo.getSrcDriverName();
    String type = DRIVERS.get(driverClass);

    if (null == type) {
      LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
          "Driver : \"" + driverClass + " \"Not Supported.");
      throw new GraphGeneratorException("Driver : \"" + driverClass + " \"Not Supported.");
    }

    if (type.equals(CarbonCommonConstants.TYPE_ORACLE) || type
        .equals(CarbonCommonConstants.TYPE_MSSQL)) {
      return CarbonSchemaParser.QUOTES;
    } else if (type.equals(CarbonCommonConstants.TYPE_MYSQL)) {
      return CarbonSchemaParser.QUOTES;
    }

    return CarbonSchemaParser.QUOTES;
  }

  public AggregateTable[] getAllAggTables() {
    return aggregateTable;
  }

  public CarbonTable getCube() {
    return carbonDataLoadSchema.getCarbonTable();
  }

  private int getNoDictionaryDimsCountInAggQuery(String[] NoDictionaryDims, String[] actualDims) {
    int count = 0;
    for (String eachSelectedDim : actualDims) {
      for (String eachNoDictionaryDim : NoDictionaryDims) {
        if (eachSelectedDim.equalsIgnoreCase(eachNoDictionaryDim)) {
          count++;
        }
      }
    }
    return count;
  }
}
