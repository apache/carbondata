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

package com.huawei.unibi.molap.graphgenerator;

import java.io.*;
import java.util.*;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.aggregatesurrogategenerator.step.MolapAggregateSurrogateGeneratorMeta;
import com.huawei.unibi.molap.api.dataloader.DataLoadModel;
import com.huawei.unibi.molap.api.dataloader.SchemaInfo;
import com.huawei.unibi.molap.autoaggregategraphgenerator.step.MolapAutoAGGGraphGeneratorMeta;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvreader.CsvReaderMeta;
import com.huawei.unibi.molap.csvreader.checkpoint.CheckPointHanlder;
import com.huawei.unibi.molap.csvreaderstep.CsvInputMeta;
import com.huawei.unibi.molap.factreader.step.MolapFactReaderMeta;
import com.huawei.unibi.molap.graphgenerator.configuration.GraphConfigurationInfo;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.mdkeygen.MDKeyGenStepMeta;
import com.huawei.unibi.molap.merger.step.MolapSliceMergerStepMeta;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Measure;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.schema.metadata.AggregateTable;
import com.huawei.unibi.molap.sortandgroupby.sortDataStep.SortKeyStepMeta;
import com.huawei.unibi.molap.sortandgroupby.step.MolapSortKeyAndGroupByStepMeta;
import com.huawei.unibi.molap.store.MolapDataWriterStepMeta;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedSeqGenMeta;
import com.huawei.unibi.molap.util.*;
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

    /**
     * DEFAULE_LEAF_NODE_SIZE
     */
    private static final String DEFAULE_LEAF_NODE_SIZE = "8192";
    /**
     * DEFAULE_MAX_LEAF_NODE_IN_FILE
     */
    private static final String DEFAULE_MAX_LEAF_NODE_IN_FILE = "100";
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

        DRIVERS.put("oracle.jdbc.OracleDriver", MolapCommonConstants.TYPE_ORACLE);
        DRIVERS.put("com.mysql.jdbc.Driver", MolapCommonConstants.TYPE_MYSQL);
        DRIVERS.put("org.gjt.mm.mysql.Driver", MolapCommonConstants.TYPE_MYSQL);
        DRIVERS.put("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                MolapCommonConstants.TYPE_MSSQL);
        DRIVERS.put("com.sybase.jdbc3.jdbc.SybDriver", MolapCommonConstants.TYPE_SYBASE);
    }

    /**
     * OUTPUT_LOCATION
     */
    private final String outputLocation = MolapProperties.getInstance()
            .getProperty("store_output_location", "../unibi-solutions/system/molap/etl");
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
     * instance
     */
    private MolapProperties instance;
    /**
     * cube
     */
    private Cube cube;
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
    private Schema schema;
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

    public GraphGenerator(DataLoadModel dataLoadModel, boolean isHDFSReadMode, String partitionID,
            Schema schema, String factStoreLocation, int currentRestructNum, int allocate) {
        this.schemaInfo = dataLoadModel.getSchemaInfo();
        this.tableName = dataLoadModel.getTableName();
        this.isCSVLoad = dataLoadModel.isCsvLoad();
        this.modifiedDimension = dataLoadModel.getModifiedDimesion();
        this.isAutoAggRequest = schemaInfo.isAutoAggregateRequest();
        this.schema = schema;
        this.schemaName = schema.name;
        this.partitionID = partitionID;
        this.factStoreLocation = factStoreLocation;
        this.isColumnar =
                Boolean.parseBoolean(MolapCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE);
        this.currentRestructNumber = currentRestructNum;
        this.allocate = allocate > 1 ? allocate : 1;
        initialise();
        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
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
            throw new GraphGeneratorException(
                    "Error while Converting the graph xml string to bytes", ue);
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
        if (null == schema) {
            schema = MolapSchemaParser.loadXML(schemaInfo.getSchemaPath());
            String originalSchemaName = schema.name;
            if (partitionID != null) {
                schema.name = originalSchemaName + "_" + partitionID;
                cube = MolapSchemaParser.getMondrianCube(schema, schemaInfo.getCubeName());
                cube.name = cube.name + "_" + partitionID;
            }
        }
        this.cube = MolapSchemaParser.getMondrianCube(schema, schemaInfo.getCubeName());
        this.cubeName = cube.name;
        this.instance = MolapProperties.getInstance();
        aggregateTable = MolapSchemaParser.getAggregateTable(cube, schema);
        this.factTableName = MolapSchemaParser.getFactTableName(cube);
    }

    public void generateGraph() throws GraphGeneratorException {
        validateAndInitialiseKettelEngine();
        String factTableName = MolapSchemaParser.getFactTableName(cube);
        GraphConfigurationInfo graphConfigInfoForFact = getGraphConfigInfoForFact(schema);
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
                                getGraphConfigInfoForCSVBasedAGG(aggregateTable[i], schema);

                        generateGraph(graphConfigInfoForAGG,
                                graphConfigInfoForAGG.getTableName() + ": Graph", isCSVLoad,
                                graphConfigInfoForFact);
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
                outputLocation + File.separator + schemaName + File.separator + cubeName
                        + File.separator);
        boolean isDirCreated = false;
        if (!file.exists()) {
            isDirCreated = file.mkdirs();

            if (!isDirCreated) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Unable to create directory or Directory already exist" + file
                                .getAbsolutePath());
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
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Invalid kettle path :: " + kettlExp.getMessage());
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, kettlExp);
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
            graphConfigInfoForFact = getGraphConfigInfoForFact(schema);
        } else {
            AggregateTable aggTablesUsedForgeneration = null;
            for (int i = 0; i < aggregateTable.length; i++) {
                if (aggregateTable[i].getAggregateTableName().equals(factTableName)) {
                    aggTablesUsedForgeneration = aggregateTable[i];
                    graphConfigInfoForFact =
                            getGraphConfigInfoForCSVBasedAGG(aggTablesUsedForgeneration, schema);
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
                MolapCommonConstants.LEVEL_METADATA_FILE + factTableName + ".metadata";

        try {
            factCardinality =
                    MolapUtil.getCardinalityFromLevelMetadataFile(factLevelCardinalityFile);
        } catch (MolapUtilException e) {
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
                    graphConfigInfoForAGG = getGraphConfigInfoForCSVBasedAGG(aggTables[j], schema);
                    aggType = graphConfigInfoForAGG.getAggType();
                    for (int k = 0; k < aggType.length; k++) {
                        if (aggType[k].equals(MolapCommonConstants.CUSTOM)) {
                            isFactMdKeyInOutRow = true;
                        }
                    }
                    stepMetaList.add(generateStepMetaForAutoAgg(graphConfigInfoForFact,
                            graphConfigInfoForAGG, aggTables[j], aggregateTableArray,
                            factCardinality, factStorePath));
                    builder.append(aggTableList[i]);
                    xAxixLocation += 50;
                    break;
                }
            }
        }

        return generateGraph(stepMetaList, graphConfigInfoForFact, builder.toString(),
                factTableName, isFactMdKeyInOutRow, false, aggregateTableArray, factStorePath,
                factCardinality, graphConfigInfoForAGG.getDimensions());
    }

    private void generateGraphForManualAgg(AggregateTable[] aggTableList, String factTableName,
            StepMeta grapgGeneratorStep) throws GraphGeneratorException {
        StringBuilder builder = new StringBuilder();
        GraphConfigurationInfo graphConfigInfoForFact = getGraphConfigInfoForFact(schema);
        List<StepMeta> stepMetaList = new ArrayList<StepMeta>();
        xAxixLocation = 50;
        for (int i = 0; i < aggTableList.length; i++) {
            for (int j = 0; j < aggregateTable.length; j++) {
                if (aggregateTable[j].getAggregateTableName()
                        .equals(aggTableList[i].getAggregateTableName())) {
                    GraphConfigurationInfo graphConfigInfoForAGG =
                            getGraphConfigInfoForCSVBasedAGG(aggregateTable[j], schema);
                    stepMetaList.add(getSliceMeregerStep(graphConfigInfoForAGG,
                            graphConfigInfoForFact));
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
                outputLocation + File.separator + schemaName + File.separator + cubeName
                        + File.separator + builder.toString() + "graphgenerator" + ".ktr";
        generateGraphFile(trans, graphFilePath);
    }

    private String generateGraph(List<List<StepMeta>> stepMetaList,
            GraphConfigurationInfo configurationInfoFact, String aggTables, String factTableName,
            boolean isFactMdKeyInOutRow, boolean isManualCall, AggregateTable[] aggTablesArray,
            String factStorePath, int[] factCardinality, String[] aggregateLevels)
            throws GraphGeneratorException {
        StepMeta molapFactReader = null;
        molapFactReader =
                getMolapFactReader(configurationInfoFact, true, isFactMdKeyInOutRow, aggTablesArray,
                        factCardinality, factStorePath, aggregateLevels);
        molapFactReader.setDistributes(false);
        TransMeta trans = new TransMeta();

        if (!isManualCall) {
            trans.setName("Auto AGG KTR");
        } else {
            trans.setName("Manual AGG KTR");
        }

        trans.addStep(molapFactReader);
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
            hopMeta = new TransHopMeta(molapFactReader, list.get(0));
            trans.addTransHop(hopMeta);
            for (int j = 1; j < listSize; j++) {
                hopMeta = new TransHopMeta(list.get(j - 1), list.get(j));
                trans.addTransHop(hopMeta);
            }
        }

        String graphFilePath = null;
        if (!isManualCall) {
            graphFilePath = outputLocation + File.separator + schemaName + File.separator + cubeName
                    + File.separator + factTableName + MolapCommonConstants.MOLAP_AUTO_AGG_CONST
                    + aggTables + ".ktr";
        } else {
            graphFilePath = outputLocation + File.separator + schemaName + File.separator + cubeName
                    + File.separator + aggTables + ".ktr";
        }

        generateGraphFile(trans, graphFilePath);
        return graphFilePath;
    }

    private List<StepMeta> generateStepMetaForAutoAgg(GraphConfigurationInfo configurationInfoFact,
            GraphConfigurationInfo configurationInfoAgg, AggregateTable aggregateTable,
            AggregateTable[] aggregateTableArray, int[] factCardinality, String factStorePath)
            throws GraphGeneratorException {
        List<StepMeta> stepMetaList = new ArrayList<StepMeta>(1);
        StepMeta molapAggregateGeneratorStep =
                getMolapAutoAggregateSurrogateKeyGeneratorStep(configurationInfoFact,
                        configurationInfoAgg, aggregateTable, false, aggregateTableArray,
                        factCardinality, factStorePath);
        stepMetaList.add(molapAggregateGeneratorStep);
        StepMeta molapSortRowAndGroupByStep =
                getMolapSortRowAndGroupByStep(configurationInfoAgg, aggregateTable, factStorePath,
                        configurationInfoFact, aggregateTableArray, factCardinality, false);
        stepMetaList.add(molapSortRowAndGroupByStep);
        StepMeta molapDataWriter =
                getMolapDataWriter(configurationInfoAgg, configurationInfoFact, aggregateTableArray,
                        factCardinality);
        stepMetaList.add(molapDataWriter);
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

        trans.setSizeRowset(Integer.parseInt(
                instance.getProperty(MolapCommonConstants.GRAPH_ROWSET_SIZE,
                        MolapCommonConstants.GRAPH_ROWSET_SIZE_DEFAULT)));

        StepMeta inputStep = null;
        StepMeta molapSurrogateKeyStep = null;
        StepMeta selectValueToChangeTheDataType = null;
        StepMeta getFileNames = null;

        // get all step
        if (isCSV) {
            getFileNames = getFileNamesStep(configurationInfo);
            if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
                inputStep = getCsvReaderStep(configurationInfo);
            } else {
                if (isHDFSReadMode) {
                    inputStep = getHadoopInputStep(configurationInfo);
                } else {
                    inputStep = getCSVInputStep(configurationInfo);
                }
            }
        } else {
            inputStep = getTableInputStep(configurationInfo);
            selectValueToChangeTheDataType =
                    getSelectValueToChangeTheDataType(configurationInfo, 1);
        }

        molapSurrogateKeyStep = getMolapCSVBasedSurrogateKeyStep(configurationInfo);
        StepMeta sortStep = getSortStep(configurationInfo);
        StepMeta molapMDKeyStep = getMDKeyStep(configurationInfo);
        StepMeta molapSliceMergerStep = null;
        molapSliceMergerStep = getSliceMeregerStep(configurationInfo, configurationInfoForFact);

        // add all steps to trans
        if (isCSV) {
            trans.addStep(getFileNames);
        }
        trans.addStep(inputStep);

        if (!isCSV) {
            trans.addStep(selectValueToChangeTheDataType);
        }

        trans.addStep(molapSurrogateKeyStep);
        trans.addStep(sortStep);
        trans.addStep(molapMDKeyStep);

        trans.addStep(molapSliceMergerStep);
        TransHopMeta getFilesInputTocsvFileInput = null;
        TransHopMeta inputStepToSelectValueHop = null;
        TransHopMeta tableInputToSelectValue = null;

        if (isCSV) {
            getFilesInputTocsvFileInput = new TransHopMeta(getFileNames, inputStep);
            inputStepToSelectValueHop = new TransHopMeta(inputStep, molapSurrogateKeyStep);
        } else {
            inputStepToSelectValueHop = new TransHopMeta(inputStep, selectValueToChangeTheDataType);
            tableInputToSelectValue =
                    new TransHopMeta(selectValueToChangeTheDataType, molapSurrogateKeyStep);
        }

        // create hop
        TransHopMeta surrogateKeyToSortHop = new TransHopMeta(molapSurrogateKeyStep, sortStep);
        TransHopMeta sortToMDKeyHop = new TransHopMeta(sortStep, molapMDKeyStep);
        TransHopMeta mdkeyToSliceMerger = null;
        mdkeyToSliceMerger = new TransHopMeta(molapMDKeyStep, molapSliceMergerStep);

        if (isCSV) {
            trans.addTransHop(getFilesInputTocsvFileInput);
            trans.addTransHop(inputStepToSelectValueHop);
        } else {
            trans.addTransHop(inputStepToSelectValueHop);
            trans.addTransHop(tableInputToSelectValue);
        }

        trans.addTransHop(surrogateKeyToSortHop);
        trans.addTransHop(sortToMDKeyHop);
        trans.addTransHop(mdkeyToSliceMerger);

        String graphFilePath =
                outputLocation + File.separator + schemaName + File.separator + cubeName
                        + File.separator + configurationInfo.getTableName() + ".ktr";
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
        StepMeta csvDataStep =
                new StepMeta("HadoopFileInputPlugin", (StepMetaInterface) fileInputMeta);
        csvDataStep.setLocation(100, 100);
        int copies = Integer.parseInt(instance.getProperty(MolapCommonConstants.NUM_CORES_LOADING,
                MolapCommonConstants.DEFAULT_NUMBER_CORES));
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
        csvInputMeta.setBufferSize(instance.getProperty(MolapCommonConstants.CSV_READ_BUFFER_SIZE,
                MolapCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
        int copies = 1;
        try {
            copies = Integer.parseInt(instance.getProperty(MolapCommonConstants.NUM_CORES_LOADING,
                    MolapCommonConstants.DEFAULT_NUMBER_CORES));
        } catch (NumberFormatException e) {
            copies = Integer.parseInt(MolapCommonConstants.DEFAULT_NUMBER_CORES);
        }
        if (copies > 1) {
            csvDataStep.setCopies(copies);
        }
        csvDataStep.setDraw(true);
        csvDataStep.setDescription("Read raw data from " + GraphGeneratorConstants.CSV_INPUT);

        return csvDataStep;
    }

    private StepMeta getCsvReaderStep(GraphConfigurationInfo graphConfiguration)
            throws GraphGeneratorException {
        CsvReaderMeta csvReaderMeta = new CsvReaderMeta();
        csvReaderMeta.setDefault();
        // Init the Filename...
        csvReaderMeta.setFilename("${csvInputFilePath}");
        csvReaderMeta.setDefault();
        csvReaderMeta.setEncoding("UTF-8");
        csvReaderMeta.setEnclosure("\"");
        csvReaderMeta.setHeaderPresent(true);
        StepMeta csvDataStep =
                new StepMeta(GraphGeneratorConstants.CSV_INPUT, (StepMetaInterface) csvReaderMeta);
        csvDataStep.setLocation(100, 100);
        csvReaderMeta.setFilenameField("filename");
        csvReaderMeta.setLazyConversionActive(false);
        csvReaderMeta.setBufferSize(instance.getProperty(MolapCommonConstants.CSV_READ_BUFFER_SIZE,
                MolapCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
        int copies = Integer.parseInt(instance.getProperty(MolapCommonConstants.NUM_CORES_LOADING,
                MolapCommonConstants.DEFAULT_NUMBER_CORES));
        if (copies > 1) {
            csvDataStep.setCopies(copies);
        }
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
            csvReaderMeta.setIncludingFilename(true);
            csvReaderMeta.setRowNumField("rownum");
        }
        csvDataStep.setStepID("MolapCSVReaderStep");
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
        MolapSliceMergerStepMeta sliceMerger = new MolapSliceMergerStepMeta();
        sliceMerger.setDefault();
        sliceMerger.setHeirAndKeySize(configurationInfo.getHeirAndKeySizeString());
        sliceMerger.setMdkeySize(configurationInfo.getMdkeySize());
        sliceMerger.setMeasureCount(configurationInfo.getMeasureCount());
        sliceMerger.setTabelName(configurationInfo.getTableName());
        sliceMerger.setCubeName(cubeName);
        sliceMerger.setSchemaName(schemaName);
        if (null != this.factStoreLocation) {
            sliceMerger.setCurrentRestructNumber(
                    MolapUtil.getRestructureNumber(this.factStoreLocation, this.factTableName));
        } else {
            sliceMerger.setCurrentRestructNumber(configurationInfo.getCurrentRestructNumber());
        }
        sliceMerger.setGroupByEnabled(isAutoAggRequest + "");
        if (isAutoAggRequest) {
            String[] aggType = configurationInfo.getAggType();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < aggType.length - 1; i++) {
                if (aggType[i].equals(MolapCommonConstants.COUNT)) {
                    builder.append(MolapCommonConstants.SUM);
                } else {
                    builder.append(aggType[i]);
                }
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
            }
            builder.append(aggType[aggType.length - 1]);
            sliceMerger.setAggregatorString(builder.toString());
            String[] aggClass = configurationInfo.getAggClass();
            builder = new StringBuilder();
            for (int i = 0; i < aggClass.length - 1; i++) {
                builder.append(aggClass[i]);
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
            }
            builder.append(aggClass[aggClass.length - 1]);
            sliceMerger.setAggregatorClassString(builder.toString());
        } else {
            sliceMerger.setAggregatorClassString(MolapCommonConstants.HASH_SPC_CHARACTER);
            sliceMerger.setAggregatorString(MolapCommonConstants.HASH_SPC_CHARACTER);
        }
        sliceMerger.setFactDimLensString("");
        sliceMerger.setLevelAnddataTypeString(configurationInfo.getLevelAnddataType());
        StepMeta sliceMergerMeta = new StepMeta(
                GraphGeneratorConstants.MOLAP_SLICE_MERGER + configurationInfo.getTableName(),
                (StepMetaInterface) sliceMerger);
        sliceMergerMeta.setStepID(GraphGeneratorConstants.MOLAP_SLICE_MERGER_ID);
        xAxixLocation += 120;
        sliceMergerMeta.setLocation(xAxixLocation, yAxixLocation);
        sliceMergerMeta.setDraw(true);
        sliceMergerMeta.setDescription(
                "SliceMerger: " + GraphGeneratorConstants.MOLAP_SLICE_MERGER + configurationInfo
                        .getTableName());
        return sliceMergerMeta;
    }

    private StepMeta getAutoAggGraphGeneratorStep(String tableName,
            AggregateTable[] aggregateTable) {
        MolapAutoAGGGraphGeneratorMeta meta = new MolapAutoAGGGraphGeneratorMeta();
        meta.setCubeName(cubeName);
        meta.setSchemaName(schemaName);
        meta.setSchema(schema.toXML());
        meta.setIsHDFSMode(isHDFSReadMode + "");
        meta.setFactStoreLocation(factStoreLocation);
        meta.setCurrentRestructNumber(currentRestructNumber);
        meta.setPartitionId(null == partitionID ? "" : partitionID);
        meta.setAggTables(this.tableName);
        meta.setFactTableName(tableName);
        meta.setAutoMode(Boolean.toString(schemaInfo.isAutoAggregateRequest()));

        StepMeta aggTableMeta = new StepMeta(GraphGeneratorConstants.MOLAP_AUTO_AGG_GRAPH_GENERATOR,
                (StepMetaInterface) meta);
        aggTableMeta.setStepID(GraphGeneratorConstants.MOLAP_AUTO_AGG_GRAPH_GENERATOR_ID);
        xAxixLocation += 120;
        aggTableMeta.setLocation(xAxixLocation, yAxixLocation);
        aggTableMeta.setDraw(true);
        aggTableMeta.setDescription(
                "SliceMerger: " + GraphGeneratorConstants.MOLAP_AUTO_AGG_GRAPH_GENERATOR);
        return aggTableMeta;
    }

    private StepMeta getMolapFactReader(GraphConfigurationInfo configurationInfo,
            boolean isReadOnlyInProgress, boolean isFactMdkeyInOutputRow,
            AggregateTable[] aggTables, int[] dimLens, String storeLocation,
            String[] aggregateLevels) {
        MolapFactReaderMeta factReader = new MolapFactReaderMeta();
        factReader.setDefault();
        factReader.setTableName(configurationInfo.getTableName());
        factReader.setAggregateTableName(tableName);
        factReader.setCubeName(cubeName);
        factReader.setSchemaName(schemaName);
        factReader.setSchema(schema.toXML());
        factReader.setPartitionID(partitionID);
        String[] factDimensions = configurationInfo.getDimensions();
        factReader.setDimLensString(MolapDataProcessorUtil.getLevelCardinalitiesString(dimLens));
        SliceMetaData sliceMetaData =
                MolapDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
        int[] globalDimensioncardinality = MolapDataProcessorUtil
                .getKeyGenerator(factDimensions, aggregateLevels, dimLens,
                        sliceMetaData.getNewDimensions(), sliceMetaData.getNewDimLens());
        factReader.setGlobalDimLensString(
                MolapDataProcessorUtil.getLevelCardinalitiesString(globalDimensioncardinality));

        factReader.setFactStoreLocation(storeLocation);

        factReader.setReadOnlyInProgress(!isReadOnlyInProgress + "");
        factReader.setMeasureCountString(configurationInfo.getMeasureCount());
        char[] type = configurationInfo.getType();
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < type.length - 1; i++) {
            builder.append(type[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
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
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
            }
            builder.append(blockIndex[blockIndex.length - 1]);
            factReader.setBlockIndexString(builder.toString());
        } else {
            factReader.setBlockIndexString(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        StepMeta factReaderMeta = new StepMeta(GraphGeneratorConstants.MOLAP_FACT_READER,
                (StepMetaInterface) factReader);
        factReaderMeta.setStepID(GraphGeneratorConstants.MOLAP_FACT_READER_ID);
        xAxixLocation += 120;
        factReaderMeta.setLocation(xAxixLocation, yAxixLocation);
        factReaderMeta.setDraw(true);
        factReaderMeta
                .setDescription("Molap Fact Reader : " + GraphGeneratorConstants.MOLAP_FACT_READER);
        return factReaderMeta;
    }

    private StepMeta getMolapAutoAggregateSurrogateKeyGeneratorStep(
            GraphConfigurationInfo configurationInfoFact,
            GraphConfigurationInfo configurationInfoAgg, AggregateTable aggregateTable,
            boolean isManualAggregationRequest, AggregateTable[] aggTableArray,
            int[] factCardinality, String factStorePath) {
        MolapAggregateSurrogateGeneratorMeta meta = new MolapAggregateSurrogateGeneratorMeta();
        meta.setHeirAndDimLens(configurationInfoFact.getHeirAndDimLens());
        meta.setHeirAndKeySize(configurationInfoFact.getHeirAndKeySizeString());
        meta.setAggDimeLensString(MolapDataProcessorUtil
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
            if (aggType[i].equals(MolapCommonConstants.CUSTOM)) {
                isMdkeyInOutRowRequired = true;
            }
            builder.append(aggType[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        builder.append(aggType[aggType.length - 1]);
        meta.setAggregatorString(builder.toString());
        meta.setMdkeyInOutRowRequired(isMdkeyInOutRowRequired + "");
        builder = new StringBuilder();

        for (int i = 0; i < factDimensions.length - 1; i++) {
            builder.append(factDimensions[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
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
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        builder.append(actualAggLevelsActualName[actualAggLevelsActualName.length - 1]);
        meta.setAggregateLevelsString(builder.toString());
        builder = new StringBuilder();
        String[] aggName = aggregateTable.getAggColuName();
        for (int i = 0; i < aggName.length - 1; i++) {
            builder.append(aggName[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }
        builder.append(aggName[aggName.length - 1]);
        meta.setAggregateMeasuresString(builder.toString());

        builder = new StringBuilder();
        String[] aggMeasure = aggregateTable.getAggMeasure();

        for (int i = 0; i < aggMeasure.length - 1; i++) {
            builder.append(aggMeasure[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        builder.append(aggMeasure[aggMeasure.length - 1]);
        meta.setAggregateMeasuresColumnNameString(builder.toString());
        meta.setTableName(configurationInfoAgg.getTableName());
        meta.setSchemaName(schemaName);
        meta.setCubeName(cubeName);
        meta.setMeasureCountString(configurationInfoFact.getMeasureCount());
        meta.setFactTableName(configurationInfoFact.getTableName());
        meta.setFactDimLensString(
                MolapDataProcessorUtil.getLevelCardinalitiesString(factCardinality));
        StepMeta mdkeyStepMeta = new StepMeta(
                GraphGeneratorConstants.MOLAP_AGGREGATE_SURROGATE_GENERATOR + configurationInfoAgg
                        .getTableName(), (StepMetaInterface) meta);
        mdkeyStepMeta.setName(
                GraphGeneratorConstants.MOLAP_AGGREGATE_SURROGATE_GENERATOR + configurationInfoAgg
                        .getTableName());
        mdkeyStepMeta.setStepID(GraphGeneratorConstants.MOLAP_AGGREGATE_SURROGATE_GENERATOR_ID);
        xAxixLocation += 120;
        mdkeyStepMeta.setLocation(xAxixLocation, yAxixLocation);
        mdkeyStepMeta.setDraw(true);
        mdkeyStepMeta.setDescription("Generate Aggregate Data "
                + GraphGeneratorConstants.MOLAP_AGGREGATE_SURROGATE_GENERATOR + configurationInfoAgg
                .getTableName());
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
        tableInputStep.setDescription(
                "Read Data From Fact Table: " + GraphGeneratorConstants.TABLE_INPUT);

        return tableInputStep;
    }

    private StepMeta getMolapCSVBasedSurrogateKeyStep(GraphConfigurationInfo graphConfiguration) {
        //
        MolapCSVBasedSeqGenMeta seqMeta = new MolapCSVBasedSeqGenMeta();
        seqMeta.setMolapdim(graphConfiguration.getDimensionString());
        seqMeta.setComplexTypeString(graphConfiguration.getComplexTypeString());
        seqMeta.setBatchSize(Integer.parseInt(graphConfiguration.getBatchSize()));
        seqMeta.setHighCardinalityDims(graphConfiguration.getHighCardinalityDims());
        seqMeta.setCubeName(cubeName);
        seqMeta.setSchemaName(schemaName);
        seqMeta.setComplexDelimiterLevel1(schemaInfo.getComplexDelimiterLevel1());
        seqMeta.setComplexDelimiterLevel2(schemaInfo.getComplexDelimiterLevel2());
        seqMeta.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
        seqMeta.setMolapMetaHier(graphConfiguration.getMetaHeirString());
        seqMeta.setMolapmsr(graphConfiguration.getMeasuresString());
        seqMeta.setMolapProps(graphConfiguration.getPropertiesString());
        seqMeta.setMolaphier(graphConfiguration.getHiersString());
        seqMeta.setMolaphierColumn(graphConfiguration.getHierColumnString());
        seqMeta.setMetaMetaHeirSQLQueries(graphConfiguration.getDimensionSqlQuery());
        seqMeta.setColumnAndTableNameColumnMapForAggString(
                graphConfiguration.getColumnAndTableNameColumnMapForAgg());
        seqMeta.setForgienKeyPrimayKeyString(
                graphConfiguration.getForgienKeyAndPrimaryKeyMapString());
        seqMeta.setTableName(graphConfiguration.getTableName());
        seqMeta.setModifiedDimension(modifiedDimension);
        seqMeta.setForeignKeyHierarchyString(graphConfiguration.getForeignKeyHierarchyString());
        seqMeta.setPrimaryKeysString(graphConfiguration.getPrimaryKeyString());
        seqMeta.setMolapMeasureNames(graphConfiguration.getMeasureNamesString());
        seqMeta.setHeirNadDimsLensString(graphConfiguration.getHeirAndDimLens());
        seqMeta.setActualDimNames(graphConfiguration.getActualDimensionColumns());
        seqMeta.setNormHiers(graphConfiguration.getNormHiers());
        seqMeta.setHeirKeySize(graphConfiguration.getHeirAndKeySizeString());
        String[] aggType = graphConfiguration.getAggType();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < aggType.length; i++) {
            builder.append(aggType[i]);
            builder.append(MolapCommonConstants.SEMICOLON_SPC_CHARACTER);
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
                outputLocation + File.separator + graphConfiguration.getStoreLocation()
                        + File.separator + MolapCommonConstants.CHECKPOINT_FILE_NAME
                        + MolapCommonConstants.CHECKPOINT_EXT;
        File file = new File(checkPointFile);
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
            if (file.exists()) {
                seqMeta.setCheckPointFileExits("true");
            }
        }
        StepMeta mdkeyStepMeta = new StepMeta(GraphGeneratorConstants.MOLAP_SURROGATE_KEY_GENERATOR,
                (StepMetaInterface) seqMeta);
        mdkeyStepMeta.setStepID(GraphGeneratorConstants.MOLAP_CSV_BASED_SURROAGATEGEN_ID);
        xAxixLocation += 120;
        //
        mdkeyStepMeta.setLocation(xAxixLocation, yAxixLocation);
        mdkeyStepMeta.setDraw(true);
        mdkeyStepMeta.setDescription("Generate Surrogate For Table Data: "
                + GraphGeneratorConstants.MOLAP_SURROGATE_KEY_GENERATOR);
        return mdkeyStepMeta;
    }

    private StepMeta getMDKeyStep(GraphConfigurationInfo graphConfiguration) {
        MDKeyGenStepMeta molapMdKey = new MDKeyGenStepMeta();
        molapMdKey.setNumberOfCores(graphConfiguration.getNumberOfCores());
        molapMdKey.setTableName(graphConfiguration.getTableName());
        molapMdKey.setSchemaName(schemaName);
        molapMdKey.setCubeName(cubeName);
        molapMdKey.setComplexTypeString(graphConfiguration.getComplexTypeString());
        molapMdKey.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
        molapMdKey.setAggregateLevels(MolapDataProcessorUtil
                .getLevelCardinalitiesString(graphConfiguration.getDimCardinalities(),
                        graphConfiguration.getDimensions()));

        molapMdKey.setMeasureCount(graphConfiguration.getMeasureCount() + "");
        molapMdKey.setDimensionsStoreTypeString(graphConfiguration.getDimensionStoreTypeString());
        molapMdKey.setDimensionCount(graphConfiguration.getActualDims().length + "");
        molapMdKey.setComplexDimsCount(graphConfiguration.getComplexTypeString().isEmpty() ?
                "0" :
                graphConfiguration.getComplexTypeString().
                        split(MolapCommonConstants.SEMICOLON_SPC_CHARACTER).length + "");
        StepMeta mdkeyStepMeta = new StepMeta(
                GraphGeneratorConstants.MDKEY_GENERATOR + graphConfiguration.getTableName(),
                (StepMetaInterface) molapMdKey);
        mdkeyStepMeta.setName(
                GraphGeneratorConstants.MDKEY_GENERATOR_ID + graphConfiguration.getTableName());
        mdkeyStepMeta.setStepID(GraphGeneratorConstants.MDKEY_GENERATOR_ID);
        //
        xAxixLocation += 120;
        mdkeyStepMeta.setLocation(xAxixLocation, yAxixLocation);
        mdkeyStepMeta.setDraw(true);
        mdkeyStepMeta.setDescription(
                "Generate MDKey For Table Data: " + GraphGeneratorConstants.MDKEY_GENERATOR
                        + graphConfiguration.getTableName());
        molapMdKey.setHighCardinalityDims(graphConfiguration.getHighCardinalityDims());

        return mdkeyStepMeta;
    }

    private StepMeta getMolapDataWriter(GraphConfigurationInfo graphConfiguration,
            GraphConfigurationInfo graphjConfigurationForFact, AggregateTable[] aggTableArray,
            int[] factDimCardinality) {
        //
        MolapDataWriterStepMeta molapDataWriter = getMolapDataWriter(graphConfiguration);
        String[] factDimensions = graphjConfigurationForFact.getDimensions();
        molapDataWriter.setCurrentRestructNumber(
                MolapUtil.getRestructureNumber(this.factStoreLocation, this.factTableName));

        // getting the high cardinality string from graphjConfigurationForFact.
        String[] highCardDims = RemoveDictionaryUtil
                .extractHighCardDimsArr(graphjConfigurationForFact.getHighCardinalityDims());
        // using the string [] of high card dims , trying to get the count of the high cardinality dims in the agg query.
        molapDataWriter.sethighCardCount(
                getHighCardDimsCountInAggQuery(highCardDims, graphConfiguration.getDimensions()));

        processAutoAggRequest(graphConfiguration, graphjConfigurationForFact, molapDataWriter);

        if (null != factDimCardinality) {
            SliceMetaData sliceMetaData = MolapDataProcessorUtil
                    .readSliceMetadata(factStoreLocation, currentRestructNumber);
            int[] globalDimensioncardinality = MolapDataProcessorUtil
                    .getKeyGenerator(factDimensions, graphConfiguration.getDimensions(),
                            factDimCardinality, sliceMetaData.getNewDimensions(),
                            sliceMetaData.getNewDimLens());
            molapDataWriter.setFactDimLensString(
                    MolapDataProcessorUtil.getLevelCardinalitiesString(globalDimensioncardinality));
        }

        molapDataWriter.setIsUpdateMemberRequest(isUpdateMemberRequest + "");
        StringBuilder builder = new StringBuilder();

        String[] dimensions = graphConfiguration.getDimensions();
        for (int i = 0; i < dimensions.length - 1; i++) {
            builder.append(dimensions[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }
        builder.append(dimensions[dimensions.length - 1]);
        String mdkeySize = graphConfiguration.getMdkeySize();
        if (isAutoAggRequest) {
            mdkeySize = getMdkeySizeInCaseOfAutoAggregate(graphConfiguration.getDimensions(),
                    factDimensions, factDimCardinality);
        }
        molapDataWriter.setMdkeyLength(mdkeySize);
        molapDataWriter.setAggregateLevelsString(builder.toString());
        builder = new StringBuilder();
        for (int i = 0; i < factDimensions.length - 1; i++) {
            builder.append(factDimensions[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }
        builder.append(factDimensions[factDimensions.length - 1]);
        molapDataWriter.setFactLevelsString(builder.toString());
        //
        StepMeta molapDataWriterStep = new StepMeta(
                GraphGeneratorConstants.MOLAP_DATA_WRITER + graphConfiguration.getTableName(),
                (StepMetaInterface) molapDataWriter);
        molapDataWriterStep.setStepID(GraphGeneratorConstants.MOLAP_DATA_WRITER_ID);
        xAxixLocation += 120;
        //
        molapDataWriterStep.setLocation(xAxixLocation, yAxixLocation);
        molapDataWriterStep.setDraw(true);
        molapDataWriterStep.setName(
                GraphGeneratorConstants.MOLAP_DATA_WRITER + graphConfiguration.getTableName());
        molapDataWriterStep.setDescription(
                "Write Molap Data To File: " + GraphGeneratorConstants.MOLAP_DATA_WRITER
                        + graphConfiguration.getTableName());
        return molapDataWriterStep;
    }

    private void processAutoAggRequest(GraphConfigurationInfo graphConfiguration,
            GraphConfigurationInfo graphjConfigurationForFact,
            MolapDataWriterStepMeta molapDataWriter) {
        boolean isFactMdkeyInInputRow = false;
        if (isAutoAggRequest) {
            String[] aggType = graphConfiguration.getAggType();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < aggType.length - 1; i++) {
                builder.append(aggType[i]);
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
            }
            builder.append(aggType[aggType.length - 1]);
            molapDataWriter.setAggregatorString(builder.toString());
            String[] aggClass = graphConfiguration.getAggClass();
            builder = new StringBuilder();
            for (int i = 0; i < aggClass.length - 1; i++) {
                builder.append(aggClass[i]);
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
            }
            builder.append(aggClass[aggClass.length - 1]);
            molapDataWriter.setAggregatorClassString(builder.toString());

            for (int i = 0; i < aggType.length; i++) {
                if (aggType[i].equals(MolapCommonConstants.CUSTOM)) {
                    isFactMdkeyInInputRow = true;
                    break;
                }
            }
            if (isUpdateMemberRequest) {
                isFactMdkeyInInputRow = false;
            }
            molapDataWriter.setIsFactMDkeyInInputRow(isFactMdkeyInInputRow + "");
            if (isFactMdkeyInInputRow) {
                molapDataWriter.setFactMdkeyLength(graphjConfigurationForFact.getMdkeySize());
            } else {
                molapDataWriter.setFactMdkeyLength(0 + "");
            }
        } else {
            molapDataWriter.setAggregatorClassString(MolapCommonConstants.HASH_SPC_CHARACTER);
            molapDataWriter.setAggregatorString(MolapCommonConstants.HASH_SPC_CHARACTER);
            molapDataWriter.setIsFactMDkeyInInputRow(isFactMdkeyInInputRow + "");
            molapDataWriter.setFactMdkeyLength(0 + "");

        }
    }

    private MolapDataWriterStepMeta getMolapDataWriter(GraphConfigurationInfo graphConfiguration) {
        MolapDataWriterStepMeta molapDataWriter = new MolapDataWriterStepMeta();
        molapDataWriter.setCubeName(cubeName);
        molapDataWriter.setSchemaName(schemaName);
        molapDataWriter.setLeafNodeSize(graphConfiguration.getLeafNodeSize());
        molapDataWriter.setMaxLeafNode(graphConfiguration.getMaxLeafInFile());
        molapDataWriter.setTabelName(graphConfiguration.getTableName());
        molapDataWriter.setMeasureCount(graphConfiguration.getMeasureCount());
        molapDataWriter.setGroupByEnabled("true");
        return molapDataWriter;
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
                MolapDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
        int[] factTableDimensioncardinality = MolapDataProcessorUtil
                .getKeyGenerator(factLevels, aggreateLevels, factDimCardinality,
                        sliceMetaData.getNewDimensions(), sliceMetaData.getNewDimLens());
        KeyGenerator keyGenerator =
                KeyGeneratorFactory.getKeyGenerator(factTableDimensioncardinality);
        int[] maskedByteRanges = MolapDataProcessorUtil.getMaskedByte(surrogateIndex, keyGenerator);
        return maskedByteRanges.length + "";
    }

    private StepMeta getSelectValueToChangeTheDataType(GraphConfigurationInfo graphConfiguration,
            int counter) {
        //
        SelectValuesMeta selectValues = new SelectValuesMeta();
        selectValues.allocate(0, 0, 0);
        StepMeta selectValueMeta = new StepMeta(GraphGeneratorConstants.SELECT_REQUIRED_VALUE
                + "Change Dimension And Measure DataType" + System.currentTimeMillis() + counter,
                (StepMetaInterface) selectValues);
        xAxixLocation += 120;
        selectValueMeta.setName("SelectValueToChangeChangeData");
        selectValueMeta.setLocation(xAxixLocation, yAxixLocation);
        selectValueMeta.setDraw(true);
        selectValueMeta.setDescription("Change The Data Type For Measures: "
                + GraphGeneratorConstants.SELECT_REQUIRED_VALUE);

        String inputQuery = graphConfiguration.getTableInputSqlQuery();
        String[] columns = parseQueryAndReturnColumns(inputQuery);

        SelectMetadataChange[] changeMeta = new SelectMetadataChange[columns.length];
        Map<String, Boolean> measureDatatypeMap =
                getMeasureDatatypeMap(graphConfiguration.getMeasureDataTypeInfo());
        String[] measures = graphConfiguration.getMeasures();
        String dimensionString = graphConfiguration.getActualDimensionColumns();
        String[] dimension = dimensionString.split(MolapCommonConstants.AMPERSAND_SPC_CHARACTER);

        for (int i = 0; i < columns.length; i++) {
            changeMeta[i] = new SelectMetadataChange(selectValues);
            changeMeta[i].setName(columns[i]);
            changeMeta[i].setType(2);
            if (isMeasureColumn(measures, columns[i]) && isNotDimesnionColumn(dimension,
                    columns[i])) {
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

        String[] measures = measureDataType.split(MolapCommonConstants.AMPERSAND_SPC_CHARACTER);
        String[] measureValue = null;
        for (int i = 0; i < measures.length; i++) {
            measureValue = measures[i].split(MolapCommonConstants.COLON_SPC_CHARACTER);
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
    private StepMeta getMolapSortRowAndGroupByStep(GraphConfigurationInfo graphConfiguration,
            AggregateTable aggregateTable, String factStorePath,
            GraphConfigurationInfo configurationInfoFact, AggregateTable[] aggTableArray,
            int[] factCardinality, boolean isManualAggregationRequest)
            throws GraphGeneratorException {
        MolapSortKeyAndGroupByStepMeta sortRowsMeta = new MolapSortKeyAndGroupByStepMeta();
        sortRowsMeta.setTabelName(graphConfiguration.getTableName());
        sortRowsMeta.setCubeName(cubeName);
        sortRowsMeta.setSchemaName(schemaName);
        sortRowsMeta.setCurrentRestructNumber(
                MolapUtil.getRestructureNumber(this.factStoreLocation, this.factTableName));

        String[] highCardDims = RemoveDictionaryUtil
                .extractHighCardDimsArr(configurationInfoFact.getHighCardinalityDims());
        sortRowsMeta.setHighCardinalityCount(
                getHighCardDimsCountInAggQuery(highCardDims, graphConfiguration.getDimensions()));
        sortRowsMeta.setIsAutoAggRequest(isAutoAggRequest + "");
        boolean isFactMdKeyInInputRow = false;
        StringBuilder builder = null;
        if (isAutoAggRequest) {
            String[] aggType = graphConfiguration.getAggType();
            builder = new StringBuilder();
            for (int i = 0; i < aggType.length - 1; i++) {
                builder.append(aggType[i]);
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
                if (aggType[i].equals(MolapCommonConstants.CUSTOM)) {
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
                builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
            }
            builder.append(aggClass[aggClass.length - 1]);
            sortRowsMeta.setAggregatorClassString(builder.toString());
        } else {
            sortRowsMeta.setAggregatorClassString(MolapCommonConstants.HASH_SPC_CHARACTER);
            sortRowsMeta.setAggregatorString(MolapCommonConstants.HASH_SPC_CHARACTER);
            sortRowsMeta.setFactDimLensString(MolapCommonConstants.COMA_SPC_CHARACTER);

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
        String mdkeySize = getMdkeySizeInCaseOfAutoAggregate(graphConfiguration.getDimensions(),
                factDimensions, factCardinality);
        sortRowsMeta.setMdkeyLength(mdkeySize);
        SliceMetaData sliceMetaData =
                MolapDataProcessorUtil.readSliceMetadata(factStoreLocation, currentRestructNumber);
        int[] globalDimensioncardinality = MolapDataProcessorUtil
                .getKeyGenerator(factDimensions, graphConfiguration.getDimensions(),
                        factCardinality, sliceMetaData.getNewDimensions(),
                        sliceMetaData.getNewDimLens());
        sortRowsMeta.setFactDimLensString(
                MolapDataProcessorUtil.getLevelCardinalitiesString(globalDimensioncardinality));
        builder = new StringBuilder();
        for (int i = 0; i < factDimensions.length - 1; i++) {
            builder.append(factDimensions[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        builder.append(factDimensions[factDimensions.length - 1]);
        sortRowsMeta.setFactLevelsString(builder.toString());

        builder = new StringBuilder();
        String[] aggMeasure = aggregateTable.getAggMeasure();

        for (int i = 0; i < aggMeasure.length - 1; i++) {
            builder.append(aggMeasure[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        builder.append(aggMeasure[aggMeasure.length - 1]);
        sortRowsMeta.setAggregateMeasuresColumnNameString(builder.toString());

        builder = new StringBuilder();
        String[] aggName = aggregateTable.getAggColuName();
        for (int i = 0; i < aggName.length - 1; i++) {
            builder.append(aggName[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }
        builder.append(aggName[aggName.length - 1]);
        sortRowsMeta.setAggregateMeasuresString(builder.toString());
        sortRowsMeta.setFactStorePath(factStorePath);
        sortRowsMeta.setFactTableName(configurationInfoFact.getTableName());
        if (this.factTableName.equals(configurationInfoFact.getTableName())) {
            sortRowsMeta.setFactMeasureString(
                    configurationInfoFact.getMeasureUniqueColumnNamesString());
        } else {
            sortRowsMeta.setFactMeasureString(configurationInfoFact.getMeasureNamesString());
        }
        String[] actualAggLevelsActualName = aggregateTable.getAggLevelsActualName();
        builder = new StringBuilder();

        for (int i = 0; i < actualAggLevelsActualName.length - 1; i++) {
            builder.append(actualAggLevelsActualName[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
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
        sortRowsStep.setStepID(GraphGeneratorConstants.MOLAP_SORTKEY_AND_GROUPBY_ID);
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
        sortRowsMeta.setTabelName(graphConfiguration.getTableName());
        sortRowsMeta.setCubeName(cubeName);
        sortRowsMeta.setSchemaName(schemaName);
        sortRowsMeta.setOutputRowSize(actualMeasures.length + 1 + "");
        sortRowsMeta.setCurrentRestructNumber(graphConfiguration.getCurrentRestructNumber());
        sortRowsMeta.setDimensionCount(graphConfiguration.getDimensions().length + "");
        sortRowsMeta.setComplexDimensionCount(graphConfiguration.getComplexTypeString().isEmpty() ?
                "0" :
                graphConfiguration.getComplexTypeString().
                        split(MolapCommonConstants.SEMICOLON_SPC_CHARACTER).length + "");
        sortRowsMeta.setIsUpdateMemberRequest(isUpdateMemberRequest + "");
        sortRowsMeta.setMeasureCount(graphConfiguration.getMeasureCount() + "");
        sortRowsMeta.setHighCardinalityDims(graphConfiguration.getHighCardinalityDims());
        StepMeta sortRowsStep = new StepMeta(
                GraphGeneratorConstants.SORT_KEY_AND_GROUPBY + graphConfiguration.getTableName(),
                (StepMetaInterface) sortRowsMeta);

        xAxixLocation += 120;
        sortRowsStep.setDraw(true);
        sortRowsStep.setLocation(xAxixLocation, yAxixLocation);
        sortRowsStep.setStepID(GraphGeneratorConstants.SORTKEY_ID);
        sortRowsStep.setDescription(
                "Sort Key: " + GraphGeneratorConstants.SORT_KEY + graphConfiguration
                        .getTableName());
        sortRowsStep.setName("Sort Key: " + GraphGeneratorConstants.SORT_KEY + graphConfiguration
                .getTableName());
        return sortRowsStep;
    }

    private GraphConfigurationInfo getGraphConfigInfoForFact(Schema schema)
            throws GraphGeneratorException {
        //
        GraphConfigurationInfo graphConfiguration = new GraphConfigurationInfo();
        graphConfiguration.setCurrentRestructNumber(currentRestructNumber);
        CubeDimension[] dimensions = cube.dimensions;
        graphConfiguration.setDimensions(MolapSchemaParser.getCubeDimensions(cube, schema));
        graphConfiguration.setActualDims(MolapSchemaParser.getDimensions(cube, schema));
        graphConfiguration.setComplexTypeString(
                MolapSchemaParser.getLevelDataTypeAndParentMapString(cube, schema));
        String factTableName = MolapSchemaParser.getFactTableName(cube);
        graphConfiguration.setTableName(factTableName);
        StringBuilder dimString = new StringBuilder();
        //
        int currentCount =
                MolapSchemaParser.getDimensionString(cube, dimensions, dimString, 0, schema);
        StringBuilder highCardinalitydimString = new StringBuilder();
        MolapSchemaParser
                .getHighCardinalityDimensionString(cube, dimensions, highCardinalitydimString, 0,
                        schema);
        graphConfiguration.setHighCardinalityDims(highCardinalitydimString.toString());

        String tableString =
                MolapSchemaParser.getTableNameString(factTableName, dimensions, schema);
        graphConfiguration.setDimensionTableNames(tableString);
        graphConfiguration.setDimensionString(dimString.toString());

        StringBuilder propString = new StringBuilder();
        currentCount =
                MolapSchemaParser.getPropertyString(dimensions, propString, currentCount, schema);
        //
        graphConfiguration
                .setForignKey(MolapSchemaParser.getForeignKeyForTables(dimensions, schema));
        graphConfiguration.setPropertiesString(propString.toString());
        graphConfiguration
                .setMeasuresString(MolapSchemaParser.getMeasureString(cube.measures, currentCount));
        graphConfiguration.setHiersString(MolapSchemaParser.getHierarchyString(dimensions, schema));
        graphConfiguration.setHierColumnString(
                MolapSchemaParser.getHierarchyStringWithColumnNames(dimensions, schema));
        graphConfiguration.setMeasureUniqueColumnNamesString(
                MolapSchemaParser.getMeasuresUniqueColumnNamesString(cube));
        graphConfiguration.setForeignKeyHierarchyString(
                MolapSchemaParser.getForeignKeyHierarchyString(dimensions, schema, factTableName));
        graphConfiguration.setConnectionName("target");
        graphConfiguration.setHeirAndDimLens(
                MolapSchemaParser.getHeirAndCardinalityString(dimensions, schema));
        //setting dimension store types
        graphConfiguration.setDimensionStoreTypeString(MolapSchemaParser.getDimensionsStoreType(cube, schema));
        graphConfiguration
                .setPrimaryKeyString(MolapSchemaParser.getPrimaryKeyString(dimensions, schema));
        graphConfiguration
                .setDenormColumns(MolapSchemaParser.getDenormColNames(dimensions, schema));

        graphConfiguration.setLevelAnddataType(
                MolapSchemaParser.getLevelAndDataTypeMapString(dimensions, schema, cube));

        graphConfiguration.setForgienKeyAndPrimaryKeyMapString(MolapSchemaParser
                .getForeignKeyAndPrimaryKeyMapString(dimensions, schema, factTableName));

        graphConfiguration.setMdkeySize(MolapSchemaParser.getMdkeySizeForFact(dimensions, schema));
        Set<String> measureColumn = new HashSet<String>(cube.measures.length);
        Measure[] m = cube.measures;
        for (int i = 0; i < m.length; i++) {
            measureColumn.add(m[i].column);
        }
        char[] type = new char[measureColumn.size()];
        Arrays.fill(type, 'n');
        graphConfiguration.setType(type);
        graphConfiguration.setMeasureCount(measureColumn.size() + "");
        graphConfiguration.setHeirAndKeySizeString(
                MolapSchemaParser.getHeirAndKeySizeMapForFact(dimensions, schema));
        graphConfiguration.setAggType(MolapSchemaParser.getMeasuresAggragatorArray(cube));
        graphConfiguration.setMeasureNamesString(MolapSchemaParser.getMeasuresNamesString(cube));
        graphConfiguration.setActualDimensionColumns(
                MolapSchemaParser.getActualDimensions(schemaInfo, cube, schema));
        graphConfiguration.setNormHiers(MolapSchemaParser.getNormHiers(cube, schema));
        graphConfiguration.setMeasureDataTypeInfo(MolapSchemaParser.getMeasuresDataType(cube));

        graphConfiguration.setStoreLocation(this.schemaName + '/' + cube.name);
        graphConfiguration.setLeafNodeSize((instance
                .getProperty("com.huawei.unibi.molap.leaf.node.size", DEFAULE_LEAF_NODE_SIZE)));
        graphConfiguration.setMaxLeafInFile((instance
                .getProperty("molap.max.leafnode.in.file", DEFAULE_MAX_LEAF_NODE_IN_FILE)));
        graphConfiguration.setNumberOfCores((instance
                .getProperty(MolapCommonConstants.NUM_CORES_LOADING, DEFAULT_NUMBER_CORES)));

        // check quotes required in query or Not
        boolean isQuotesRequired = true;
        String quote = MolapSchemaParser.QUOTES;
        if (null != schemaInfo.getSrcDriverName()) {
            quote = getQuoteType(schemaInfo);
        }
        graphConfiguration.setTableInputSqlQuery(MolapSchemaParser
                .getTableInputSQLQuery(dimensions, cube.measures,
                        MolapSchemaParser.getFactTableName(cube), isQuotesRequired, schema));
        graphConfiguration
                .setBatchSize((instance.getProperty("molap.batch.size", DEFAULT_BATCH_SIZE)));
        graphConfiguration
                .setSortSize((instance.getProperty("molap.sort.size", DEFAULT_SORT_SIZE)));
        graphConfiguration.setDimensionSqlQuery(MolapSchemaParser
                .getDimensionSQLQueries(cube, dimensions, schema, isQuotesRequired, quote));
        graphConfiguration.setMetaHeirString(
                MolapSchemaParser.getMetaHeirString(dimensions, schema, factTableName));
        graphConfiguration.setDimCardinalities(
                MolapSchemaParser.getCardinalities(factTableName, dimensions, schema));

        graphConfiguration.setMeasures(MolapSchemaParser.getMeasures(cube.measures));
        graphConfiguration.setAGG(false);
        graphConfiguration.setUsername(schemaInfo.getSrcUserName());
        graphConfiguration.setPassword(schemaInfo.getSrcPwd());
        graphConfiguration.setDriverclass(schemaInfo.getSrcDriverName());
        graphConfiguration.setConnectionUrl(schemaInfo.getSrcConUrl());
        return graphConfiguration;
    }

    private GraphConfigurationInfo getGraphConfigInfoForCSVBasedAGG(AggregateTable aggtable,
            Schema schema) throws GraphGeneratorException {
        GraphConfigurationInfo graphConfiguration = new GraphConfigurationInfo();
        graphConfiguration.setCurrentRestructNumber(currentRestructNumber);
        CubeDimension[] dimensions = cube.dimensions;
        graphConfiguration.setDimensions(aggtable.getAggLevelsActualName());
        graphConfiguration.setActualDims(aggtable.getAggLevels());
        graphConfiguration.setMeasures(aggtable.getAggMeasure());
        graphConfiguration.setAggClass(aggtable.getAggregateClass());
        String factTableName = MolapSchemaParser.getFactTableName(cube);
        Map<String, String> cardinalities =
                MolapSchemaParser.getCardinalities(factTableName, dimensions, schema);
        graphConfiguration.setDimCardinalities(cardinalities);
        graphConfiguration.setDimensionTableNames(
                MolapSchemaParser.getTableNameString(factTableName, dimensions, schema));
        graphConfiguration.setTableName(aggtable.getAggregateTableName());
        StringBuilder dimString = new StringBuilder();
        graphConfiguration.setLevelAnddataType(MolapCommonConstants.HASH_SPC_CHARACTER);

        String[] levelWithTableName = aggtable.getActualAggLevels();
        String[] actualLevalName = aggtable.getAggLevels();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < levelWithTableName.length - 1; i++) {
            builder.append(actualLevalName[i]);
            builder.append(MolapCommonConstants.HYPHEN_SPC_CHARACTER);
            builder.append(levelWithTableName[i]);
            builder.append(MolapCommonConstants.HASH_SPC_CHARACTER);
        }

        builder.append(actualLevalName[actualLevalName.length - 1]);
        builder.append(MolapCommonConstants.HYPHEN_SPC_CHARACTER);
        builder.append(levelWithTableName[actualLevalName.length - 1]);
        graphConfiguration.setColumnAndTableNameColumnMapForAgg(builder.toString());

        int currentCount = MolapSchemaParser
                .getDimensionStringForAgg(aggtable.getActualAggLevels(), dimString, 0,
                        cardinalities, aggtable.getAggLevelsActualName());
        graphConfiguration.setDimensionString(dimString.toString());
        graphConfiguration.setMeasuresString(
                MolapSchemaParser.getMeasureStringForAgg(aggtable.getAggMeasure(), currentCount));

        graphConfiguration.setMeasureCount(aggtable.getAggMeasure().length + "");
        graphConfiguration.setMdkeySize(MolapSchemaParser
                .getMdkeySizeForAgg(aggtable.getAggLevelsActualName(), cardinalities));
        graphConfiguration.setHeirAndKeySizeString(
                MolapSchemaParser.getHeirAndKeySizeMapForFact(dimensions, schema));
        //
        graphConfiguration.setConnectionName("target");
        graphConfiguration.setHeirAndDimLens(
                MolapSchemaParser.getHeirAndCardinalityString(dimensions, schema));
        graphConfiguration.setMeasureNamesString(
                MolapSchemaParser.getMeasuresNamesStringForAgg(aggtable.getAggColuName()));
        graphConfiguration.setHierColumnString(
                MolapSchemaParser.getHierarchyStringWithColumnNames(dimensions, schema));
        graphConfiguration.setForeignKeyHierarchyString(
                MolapSchemaParser.getForeignKeyHierarchyString(dimensions, schema, factTableName));
        graphConfiguration.setActualDimensionColumns(
                MolapSchemaParser.getActualDimensionsForAggregate(aggtable.getAggLevels()));
        char[] type = new char[aggtable.getAggregator().length];
        Arrays.fill(type, 'n');
        for (int i = 0; i < type.length; i++) {
            if (aggtable.getAggregator()[i].equals(MolapCommonConstants.DISTINCT_COUNT) || aggtable
                    .getAggregator()[i].equals(MolapCommonConstants.CUSTOM)) {
                type[i] = 'c';
            }
        }
        graphConfiguration.setType(type);
        graphConfiguration.setAggType(aggtable.getAggregator());
        graphConfiguration.setStoreLocation(this.schemaName + '/' + cube.name);
        graphConfiguration.setLeafNodeSize((instance
                .getProperty("com.huawei.unibi.molap.leaf.node.size", DEFAULE_LEAF_NODE_SIZE)));
        graphConfiguration.setMaxLeafInFile((instance
                .getProperty("molap.max.leafnode.in.file", DEFAULE_MAX_LEAF_NODE_IN_FILE)));
        graphConfiguration.setNumberOfCores((instance
                .getProperty(MolapCommonConstants.NUM_CORES_LOADING, DEFAULT_NUMBER_CORES)));

        // check quotes required in query or Not
        boolean isQuotesRequired = false;
        if (null != schemaInfo.getSrcDriverName()) {
            isQuotesRequired = isQuotesRequired(schemaInfo);
        }

        //
        graphConfiguration.setTableInputSqlQuery(MolapSchemaParser
                .getTableInputSQLQueryForAGG(aggtable.getAggLevels(), aggtable.getAggMeasure(),
                        aggtable.getAggregateTableName(), isQuotesRequired));
        graphConfiguration
                .setBatchSize((instance.getProperty("molap.batch.size", DEFAULT_BATCH_SIZE)));
        graphConfiguration
                .setSortSize((instance.getProperty("molap.sort.size", DEFAULT_SORT_SIZE)));
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
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Driver : \"" + driverCls + " \"Not Supported.");
            throw new GraphGeneratorException("Driver : \"" + driverCls + " \"Not Supported.");
        }

        if (type.equals(MolapCommonConstants.TYPE_ORACLE) || type
                .equals(MolapCommonConstants.TYPE_MSSQL)) {
            return true;
        } else if (type.equals(MolapCommonConstants.TYPE_MYSQL)) {
            return true;
        }

        return true;
    }

    private String getQuoteType(SchemaInfo schemaInfo) throws GraphGeneratorException {
        String driverClass = schemaInfo.getSrcDriverName();
        String type = DRIVERS.get(driverClass);

        if (null == type) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Driver : \"" + driverClass + " \"Not Supported.");
            throw new GraphGeneratorException("Driver : \"" + driverClass + " \"Not Supported.");
        }

        if (type.equals(MolapCommonConstants.TYPE_ORACLE) || type
                .equals(MolapCommonConstants.TYPE_MSSQL)) {
            return MolapSchemaParser.QUOTES;
        } else if (type.equals(MolapCommonConstants.TYPE_MYSQL)) {
            return MolapSchemaParser.QUOTES;
        }

        return MolapSchemaParser.QUOTES;
    }

    public AggregateTable[] getAllAggTables() {
        return aggregateTable;
    }

    public Cube getCube() {
        return cube;
    }

    private int getHighCardDimsCountInAggQuery(String[] highCardDims, String[] actualDims) {
        int count = 0;
        for (String eachSelectedDim : actualDims) {
            for (String eachHighCardDim : highCardDims) {
                if (eachSelectedDim.equalsIgnoreCase(eachHighCardDim)) {
                    count++;
                }
            }
        }
        return count;
    }
}
