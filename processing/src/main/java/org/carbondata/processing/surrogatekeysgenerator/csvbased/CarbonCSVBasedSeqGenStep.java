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

package org.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.cache.dictionary.*;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.csvreader.checkpoint.CheckPointHanlder;
import org.carbondata.core.csvreader.checkpoint.CheckPointInterface;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonVersion;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.core.writer.ByteArrayHolder;
import org.carbondata.core.writer.HierarchyValueWriterForCSV;
import org.carbondata.processing.dataprocessor.manager.CarbonDataProcessorManager;
import org.carbondata.processing.dataprocessor.queue.impl.DataProcessorQueue;
import org.carbondata.processing.dataprocessor.queue.impl.RecordComparator;
import org.carbondata.processing.dataprocessor.record.holder.DataProcessorRecordHolder;
import org.carbondata.processing.datatypes.GenericDataType;
import org.carbondata.processing.dimension.load.command.DimensionLoadCommand;
import org.carbondata.processing.dimension.load.command.impl.CSVDimensionLoadCommand;
import org.carbondata.processing.dimension.load.command.impl.DimenionLoadCommandHelper;
import org.carbondata.processing.dimension.load.command.invoker.DimensionLoadActionInvoker;
import org.carbondata.processing.dimension.load.info.DimensionLoadInfo;
import org.carbondata.processing.schema.metadata.CarbonInfo;
import org.carbondata.processing.schema.metadata.HierarchiesInfo;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.processing.util.RemoveDictionaryUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.ConnectionPoolUtil;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.map.DatabaseConnectionMap;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class CarbonCSVBasedSeqGenStep extends BaseStep {

    /**
     * BYTE ENCODING
     */
    public static final String BYTE_ENCODING = "ISO-8859-1";
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonCSVBasedSeqGenStep.class.getName());
    /**
     * NUM_CORES_DEFAULT_VAL
     */
    private static final int NUM_CORES_DEFAULT_VAL = 2;
    /**
     * drivers
     */
    private static final Map<String, String> DRIVERS;

    static {

        DRIVERS = new HashMap<String, String>(16);
        DRIVERS.put("oracle.jdbc.OracleDriver", CarbonCommonConstants.TYPE_ORACLE);
        DRIVERS.put("com.mysql.jdbc.Driver", CarbonCommonConstants.TYPE_MYSQL);
        DRIVERS.put("org.gjt.mm.mysql.Driver", CarbonCommonConstants.TYPE_MYSQL);
        DRIVERS.put("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                CarbonCommonConstants.TYPE_MSSQL);
        DRIVERS.put("com.sybase.jdbc3.jdbc.SybDriver", CarbonCommonConstants.TYPE_SYBASE);
    }

    /**
     * ReentrantLock getRowLock
     */
    private final Object getRowLock = new Object();
    /**
     * seqGenLock
     */
    private final Object seqGenLock = new Object();
    /**
     * ReentrantLock putRowLock
     */
    private final Object putRowLock = new Object();
    /**
     * CarbonSeqGenData
     */
    private CarbonCSVBasedSeqGenData data;
    /**
     * CarbonSeqGenStepMeta1
     */
    private CarbonCSVBasedSeqGenMeta meta;
    /**
     * Map of Connection
     */
    private Map<String, Connection> cons = new HashMap<String, Connection>(16);
    /**
     * Csv file path
     */
    private String csvFilepath;
    /**
     * modifiedDimesions
     */
    private String modifiedDimesions;
    /**
     * dimTableFileLoc
     */
    private String dimTableFileLoc;
    /**
     * badRecordslogger
     */
    private BadRecordslogger badRecordslogger;
    /**
     * executorService
     */
    private ExecutorService putRowExecutorService;
    /**
     * Normalized Hier and HierWriter map
     */
    private Map<String, HierarchyValueWriterForCSV> nrmlizedHierWriterMap =
            new HashMap<String, HierarchyValueWriterForCSV>(16);
    /**
     * load Folder location
     */
    private String loadFolderLoc;
    /**
     * File manager
     */
    private IFileManagerComposite filemanager;
    /**
     * measureCol
     */
    private List<String> measureCol;
    private boolean isTerminated;
    /**
     * dimPresentCsvOrder - Dim present In CSV order
     */
    private boolean[] dimPresentCsvOrder;
    /**
     * ValueToCheckAgainst
     */
    private String valueToCheckAgainst;
    /**
     * propMap
     */
    private Map<String, int[]> propMap;
    /**
     * resultArray
     */
    private Future[] resultArray;
    /**
     * putRowFuture
     */
    private Future<Void> putRowFuture;
    /**
     * seqNumber
     */
    private int seqNumber = 1;
    /**
     * checkPointSize
     */
    private int checkPointSize = 500;
    /**
     * initialCapacity
     */
    private int initialCapacity = 25;
    /**
     * threshold
     */
    private int threshold = 20;
    /**
     * toCopy
     */
    private int toCopy = 10;
    private int outSize;
    /**
     * denormHierarchies
     */
    private List<String> denormHierarchies;
    /**
     * readCounter
     */
    private long readCounter;
    /**
     * writeCounter
     */
    private long writeCounter;
    /**
     * logCounter
     */
    private int logCounter;
    /**
     * presentColumnMapIndex
     */
    private int[] presentColumnMapIndex;
    /**
     * measurePresentMapping
     */
    private boolean[] measurePresentMapping;
    /**
     * measureSurrogateReqMapping
     */
    private boolean[] measureSurrogateReqMapping;
    /**
     * foreignKeyMappingColumns
     */
    private String[] foreignKeyMappingColumns;
    /**
     * foreignKeyMappingColumns
     */
    private String[][] foreignKeyMappingColumnsForMultiple;
    /**
     * Meta column names
     */
    private String[] metaColumnNames;
    /**
     * duplicateColMapping
     */
    private int[][] duplicateColMapping;
    private ExecutorService exec;
    /**
     * threadStatusObserver
     */
    private ThreadStatusObserver threadStatusObserver;
    /**
     * checkpoint
     */
    private CheckPointInterface checkPoint;
    /**
     * CarbonCSVBasedDimSurrogateKeyGen
     */
    private CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen;
    private DataProcessorQueue localQueue;
    private BlockingQueue<DataProcessorRecordHolder> dataQueue;
    private int buffer;
    private int processed;

    private String[] msrDataType;

    /**
     * Constructor
     *
     * @param s
     * @param stepDataInterface
     * @param c
     * @param t
     * @param dis
     */
    public CarbonCSVBasedSeqGenStep(StepMeta s, StepDataInterface stepDataInterface, int c,
            TransMeta t, Trans dis) {
        super(s, stepDataInterface, c, t, dis);
        csvFilepath = dis.getVariable("csvInputFilePath");
        modifiedDimesions = dis.getVariable("modifiedDimNames");
        dimTableFileLoc = dis.getVariable("dimFileLocDir");
    }

    /**
     * processRow
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

        try {
            meta = (CarbonCSVBasedSeqGenMeta) smi;
            StandardLogService
                    .setThreadName(StandardLogService.getPartitionID(meta.getCubeName()), null);
            data = (CarbonCSVBasedSeqGenData) sdi;

            Object[] r = getRow();  // get row, blocks when needed!
            checkPoint =
                    CheckPointHanlder.getCheckpoint(new File(getTrans().getFilename()).getName());
            if (first) {
                first = false;
                meta.initialize();
                final Object dataProcessingLockObject = CarbonDataProcessorManager.getInstance()
                        .getDataProcessingLockObject(
                                meta.getSchemaName() + '_' + meta.getCubeName());
                synchronized (dataProcessingLockObject) {
                    // observer of writing file in thread
                    this.threadStatusObserver = new ThreadStatusObserver();

                    dataQueue =
                            new PriorityBlockingQueue<DataProcessorRecordHolder>(initialCapacity,
                                    new RecordComparator());
                    if (csvFilepath == null) {
                        //                    isDBFactLoad = true;
                        csvFilepath = meta.getTableName();
                    }

                    if (null == measureCol) {
                        measureCol = Arrays.asList(meta.measureColumn);
                    }

                    buffer = Integer.parseInt(CarbonProperties.getInstance()
                            .getProperty(CarbonCommonConstants.SORT_SIZE,
                                    CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL));

                    checkPointSize = buffer / Integer.parseInt(CarbonProperties.getInstance()
                            .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
                                    CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));

                    // Update the Null value comparer and update the String against which we need
                    // to check the values coming from the previous step.

                    logCounter = Integer.parseInt(
                            CarbonCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER);
                    if (null != getInputRowMeta()) {
                        meta.updateHierMappings(getInputRowMeta());

                        meta.msrMapping = getMeasureOriginalIndexes(meta.measureColumn);

                        meta.memberMapping = getMemberMappingOriginalIndexes();

                        data.setInputSize(getInputRowMeta().size());

                        updatePropMap(meta.actualDimArray);
                        if (meta.isAggregate()) {
                            presentColumnMapIndex = createPresentColumnMapIndexForAggregate();
                        } else {
                            presentColumnMapIndex = createPresentColumnMapIndex();

                        }
                        measurePresentMapping = createMeasureMappigs(measureCol);
                        measureSurrogateReqMapping = createMeasureSurrogateReqMapping();
                        createForeignKeyMappingColumns();
                        metaColumnNames = createColumnArrayFromMeta();
                        String msrDatatypes = meta.getMeasureDataType();
                        if (msrDatatypes.length() > 0) {
                            msrDataType = msrDatatypes
                                    .split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
                        }
                    }

                    if (!meta.isAggregate()) {
                        updateHierarchyKeyGenerators(data.getKeyGenerators(), meta.hirches,
                                meta.dimLens, meta.dimColNames);
                    }

                    data.setGenerator(KeyGeneratorFactory
                            .getKeyGenerator(getUpdatedLens(meta.dimLens, meta.dimPresent)));

                    if (null != getInputRowMeta()) {
                        data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());
                    }

                    CarbonInfo carbonInfo = new CarbonInfo();
                    carbonInfo.setDims(meta.dims);
                    carbonInfo.setDimColNames(meta.dimColNames);
                    carbonInfo.setKeyGenerators(data.getKeyGenerators());
                    carbonInfo.setSchemaName(meta.getSchemaName());
                    carbonInfo.setCubeName(meta.getCubeName());
                    carbonInfo.setHierTables(meta.hirches.keySet());
                    carbonInfo.setBatchSize(meta.getBatchSize());
                    carbonInfo.setStoreType(meta.getStoreType());
                    carbonInfo.setAggregateLoad(meta.isAggregate());
                    carbonInfo.setMaxKeys(meta.dimLens);
                    carbonInfo.setPropColumns(meta.getPropertiesColumns());
                    carbonInfo.setPropIndx(meta.getPropertiesIndices());
                    carbonInfo.setTimeOrdinalCols(meta.timeOrdinalCols);
                    carbonInfo.setPropTypes(meta.getPropTypes());
                    carbonInfo.setTimDimIndex(meta.timeDimeIndex);
                    carbonInfo.setDimHierRel(meta.getDimTableArray());
                    carbonInfo.setBaseStoreLocation(updateStoreLocationAndPopulateCarbonInfo(
                            meta.getSchemaName() + '/' + meta.getCubeName()));
                    carbonInfo.setTableName(meta.getTableName());
                    carbonInfo.setPrimaryKeyMap(meta.getPrimaryKeyMap());
                    carbonInfo.setMeasureColumns(meta.measureColumn);
                    carbonInfo.setComplexTypesMap(meta.getComplexTypes());

                    updateBagLogFileName();
                    String key = meta.getSchemaName() + '/' + meta.getCubeName() + '_' + meta
                            .getTableName();
                    badRecordslogger = new BadRecordslogger(key, csvFilepath,
                            getBadLogStoreLocation(
                                    meta.getSchemaName() + '/' + meta.getCubeName()));

                    carbonInfo.setTimeOrdinalIndices(meta.timeOrdinalIndices);
                    surrogateKeyGen = new FileStoreSurrogateKeyGenForCSV(carbonInfo,
                            meta.getCurrentRestructNumber());
                    data.setSurrogateKeyGen(surrogateKeyGen);

                    updateStoreLocation();

                    // Check the insert hierarchies required or not based on that
                    // Create the list which will hold the hierarchies required to be created
                    // i.e. denormalized hierarchies.
                    if (null != getInputRowMeta()) {
                        denormHierarchies = getDenormalizedHierarchies();
                    }

                    if (null != getInputRowMeta()) {
                        // We consider that there is no time dimension,in these case
                        // the
                        // timeIndex = -1

                        ValueMetaInterface[] out = null;
                        out = new ValueMetaInterface[meta.normLength + meta.msrMapping.length
                                + checkPoint.getCheckPointInfoFieldCount()];
                        this.outSize = out.length;
                        int outCounter = 0;
                        for (int i = 0; i < meta.actualDimArray.length; i++) {
                            if (meta.dimPresent[i]) {
                                ValueMetaInterface x = new ValueMeta(meta.actualDimArray[i],
                                        ValueMetaInterface.TYPE_STRING,
                                        ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
                                x.setStorageMetadata((new ValueMeta(meta.actualDimArray[i],
                                        ValueMetaInterface.TYPE_STRING,
                                        ValueMetaInterface.STORAGE_TYPE_NORMAL)));
                                x.setStringEncoding(BYTE_ENCODING);
                                x.setStringEncoding(BYTE_ENCODING);
                                x.getStorageMetadata().setStringEncoding(BYTE_ENCODING);

                                out[outCounter] = x;
                                outCounter++;
                            }
                        }

                        for (int j = 0; j < meta.measureColumn.length; j++) {
                            for (int k = 0; k < data.getOutputRowMeta().size(); k++) {
                                if (meta.measureColumn[j].equalsIgnoreCase(
                                        data.getOutputRowMeta().getValueMeta(k).getName())) {
                                    out[outCounter] = new ValueMeta(meta.measureColumn[j],
                                            ValueMetaInterface.TYPE_NUMBER,
                                            ValueMetaInterface.STORAGE_TYPE_NORMAL);
                                    out[outCounter].setStorageMetadata(
                                            new ValueMeta(meta.measureColumn[j],
                                                    ValueMetaInterface.TYPE_NUMBER,
                                                    ValueMetaInterface.STORAGE_TYPE_NORMAL));
                                    outCounter++;
                                    break;
                                }
                            }
                        }

                        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {

                            out[outCounter++] = data.getOutputRowMeta()
                                    .getValueMeta(data.getOutputRowMeta().size() - 2);
                            out[outCounter++] = data.getOutputRowMeta()
                                    .getValueMeta(data.getOutputRowMeta().size() - 1);

                        }

                        data.getOutputRowMeta().setValueMetaList(Arrays.asList(out));

                    }

                    if (null != r) {
                        CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGenObj =
                                data.getSurrogateKeyGen();
                        if (null != surrogateKeyGenObj) {
                            int index = 0;
                            for (int j = 0; j < meta.dimColNames.length; j++) {
                                GenericDataType complexType = carbonInfo.getComplexTypesMap()
                                        .get(meta.dimColNames[j]
                                                .substring(meta.getTableName().length() + 1));
                                if (complexType != null) {
                                    List<GenericDataType> primitiveChild =
                                            new ArrayList<GenericDataType>();
                                    complexType.getAllPrimitiveChildren(primitiveChild);
                                    for (GenericDataType eachPrimitive : primitiveChild) {
                                        surrogateKeyGenObj.generateSurrogateKeys(
                                                CarbonCommonConstants.MEMBER_DEFAULT_VAL,
                                                meta.getTableName() + CarbonCommonConstants.UNDERSCORE + eachPrimitive.getName());
                                        index++;
                                    }
                                } else {
                                    surrogateKeyGenObj.generateSurrogateKeys(
                                            CarbonCommonConstants.MEMBER_DEFAULT_VAL,
                                            meta.dimColNames[j]);
                                    index++;
                                }
                            }
                        }
                    }
                }
            }

            // no more input to be expected...
            if (r == null) {
                return processWhenRowIsNull();
            }
            // proecess the first
            Object[] out = process(r);
            readCounter++;
            if (null != out) {
                writeCounter++;
                putRow(data.getOutputRowMeta(), out);
            }
            localQueue = new DataProcessorQueue(initialCapacity);

            // start multi-thread to process
            int numberOfNodes;
            try {
                numberOfNodes = Integer.parseInt(CarbonProperties.getInstance()
                        .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
                                CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
            } catch (NumberFormatException exc) {
                numberOfNodes = NUM_CORES_DEFAULT_VAL;
            }

            startReadingProcess(numberOfNodes);
            updateAndWriteSliceMetadataFile();
            CarbonUtil.writeLevelCardinalityFile(loadFolderLoc, meta.getTableName(),
                    getUpdatedCardinality(data.getSurrogateKeyGen().max));
            writeDataFileVersion();
            badRecordslogger.closeStreams();
            if (!meta.isAggregate()) {
                closeNormalizedHierFiles();
            }
            if (writeCounter == 0) {
                putRow(data.getOutputRowMeta(), new Object[outSize]);
            }
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTableName());
            String logMessage =
                    "Summary: Carbon CSV Based Seq Gen Step : " + readCounter + ": Write: "
                            + writeCounter;
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
            setOutputDone();

        } catch (Exception ex) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, ex);
            throw new RuntimeException(ex);
        }

        return false;
    }

    private void startReadingProcess(int numberOfNodes)
            throws KettleException, InterruptedException {
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
            this.putRowExecutorService = Executors.newFixedThreadPool(1);

            startProcess(numberOfNodes);

            if (resultArray != null) {
                int futureTaskSize = resultArray.length;
                boolean done = false;
                while (!done) {
                    done = true;
                    for (int i = 0; i < futureTaskSize; i++) {

                        if (!resultArray[i].isDone()) {
                            done = false;
                        }

                    }
                    Thread.sleep(200);
                }
            }

            if (!localQueue.isEmpty()) {
                while (!localQueue.isEmpty()) {
                    dataQueue.offer(localQueue.poll());
                }
                putRowInSeqence();
            }

            while (true) {
                if (putRowFuture.isDone()) {
                    break;
                }
            }

            this.putRowExecutorService.shutdown();

        } else {

            startProcess(numberOfNodes);
        }
    }

    private boolean processWhenRowIsNull() throws KettleException {
        // If first request itself is null then It will not enter the first block and
        // in data surrogatekeygen will not be initialized so it can throw NPE.
        if (data.getSurrogateKeyGen() == null) {
            setOutputDone();
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTableName());
            String logMessage =
                    "Summary: Carbon CSV Based Seq Gen Step:  Read: " + readCounter + ": Write: "
                            + writeCounter;
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
            return false;
        }
        try {
            updateAndWriteSliceMetadataFile();
        } catch (Exception e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                    "Not able to get Key genrator");
            throw new KettleException();
        }

        setOutputDone();
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "Record Procerssed For table: " + meta.getTableName());
        String logMessage =
                "Summary: Carbon CSV Based Seq Gen Step:  Read: " + readCounter + ": Write: "
                        + writeCounter;
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
        return false;
    }

    private List<String> getDenormalizedHierarchies() {
        List<String> hierList = Arrays.asList(meta.hierNames);
        List<String> denormHiers = new ArrayList<String>(10);
        for (Iterator<Entry<String, int[]>> iterator = meta.hirches.entrySet().iterator(); iterator
                .hasNext(); ) {
            Entry<String, int[]> entry = iterator.next();
            String name = entry.getKey();

            if (hierList.contains(name)) {
                continue;
            } else if (entry.getValue().length > 1) {
                denormHiers.add(name);
            }
        }

        return denormHiers;
    }

    private void updatePropMap(String[] actualDimArray) {
        if (null == propMap) {
            propMap = new HashMap<String, int[]>(actualDimArray.length);
        }
        List<String> currentColNames = new ArrayList<String>(10);
        for (int i = 0; i < getInputRowMeta().size(); i++) {
            currentColNames.add(getInputRowMeta().getValueMeta(i).getName());
        }

        List<String> currentColName = new ArrayList<String>(actualDimArray.length);

        for (int i = 0; i < getInputRowMeta().size(); i++) {
            String columnName = getInputRowMeta().getValueMeta(i).getName();
            String hier = meta.foreignKeyHierarchyMap.get(columnName);
            if (null != hier) {
                if (hier.indexOf(CarbonCommonConstants.COMA_SPC_CHARACTER) > -1) {
                    String[] splittedHiers = hier.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
                    for (String hierName : splittedHiers) {
                        String tableName = meta.getHierDimTableMap().get(hier);
                        String[] cols = meta.hierColumnMap.get(hierName);
                        if (null != cols) {
                            for (String column : cols) {
                                currentColName.add(tableName + '_' + column);
                            }
                        }
                    }
                } else {
                    String tableName = meta.getHierDimTableMap().get(hier);

                    String[] columns = meta.hierColumnMap.get(hier);

                    if (null != columns) {
                        for (String column : columns) {
                            currentColName.add(tableName + '_' + column);
                        }
                    }
                }
            } else
            // then it can be direct column name if not foreign key.
            {
                currentColName.add(meta.getTableName() + '_' + columnName);
            }
        }

        String[] currentColNamesArray = currentColName.toArray(new String[currentColName.size()]);

        List<HierarchiesInfo> metahierVoList = meta.getMetahierVoList();

        if (null == metahierVoList) {
            return;
        }
        for (HierarchiesInfo hierInfo : metahierVoList) {

            Map<String, String[]> columnPropMap = hierInfo.getColumnPropMap();

            Set<Entry<String, String[]>> entrySet = columnPropMap.entrySet();

            for (Entry<String, String[]> entry : entrySet) {
                String[] propColmns = entry.getValue();
                int[] index = getIndex(currentColNamesArray, propColmns);
                propMap.put(entry.getKey(), index);
            }
        }

    }

    private int[] getIndex(String[] currentColNamesArray, String[] propColmns) {
        int[] resultIndex = new int[propColmns.length];

        for (int i = 0; i < propColmns.length; i++) {
            for (int j = 0; j < currentColNamesArray.length; j++) {
                if (propColmns[i].equalsIgnoreCase(currentColNamesArray[j])) {
                    resultIndex[i] = j;
                    break;
                }
            }
        }

        return resultIndex;
    }

    private void closeNormalizedHierFiles() throws KettleException {
        if (null == filemanager) {
            return;
        }
        int hierLen = filemanager.size();

        for (int i = 0; i < hierLen; i++) {
            FileData hierFileData = (FileData) filemanager.get(i);
            String hierInProgressFileName = hierFileData.getFileName();
            HierarchyValueWriterForCSV hierarchyValueWriter =
                    nrmlizedHierWriterMap.get(hierInProgressFileName);
            if (null == hierarchyValueWriter) {
                continue;
            }

            List<ByteArrayHolder> holders = hierarchyValueWriter.getByteArrayList();
            Collections.sort(holders);

            for (ByteArrayHolder holder : holders) {
                hierarchyValueWriter
                        .writeIntoHierarchyFile(holder.getMdKey(), holder.getPrimaryKey());
            }

            // now write the byte array in the file.
            FileChannel bufferedOutStream = hierarchyValueWriter.getBufferedOutStream();
            if (null == bufferedOutStream) {
                continue;
            }
            CarbonUtil.closeStreams(bufferedOutStream);

            hierInProgressFileName = hierFileData.getFileName();
            int counter = hierarchyValueWriter.getCounter();
            String storePath = hierFileData.getStorePath();
            String changedFileName = hierInProgressFileName + (counter - 1);
            hierInProgressFileName = changedFileName + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

            File currentFile = new File(storePath + File.separator + hierInProgressFileName);
            File destFile = new File(storePath + File.separator + changedFileName);
            if (currentFile.exists()) {
                boolean renameTo = currentFile.renameTo(destFile);

                if (!renameTo) {
                    LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                            "Not Able to Rename File : " + currentFile.getName());
                }
            }

        }

    }

    /**
     * Load Store location
     */
    private void updateStoreLocation() {
        String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
        String store = CarbonProperties.getInstance()
                .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        store = store + File.separator + meta.getSchemaName() + '/' + meta.getCubeName();

        int rsCounter = meta.getCurrentRestructNumber()/*CarbonUtil.checkAndReturnNextRestructFolderNumber(store,"RS_")*/;

        store = store + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCounter
                + File.separator + meta.getTableName();

        int loadCounter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(store);

        loadFolderLoc = store + File.separator + CarbonCommonConstants.LOAD_FOLDER + loadCounter
                + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

    }

    private String getBadLogStoreLocation(String storeLocation) {
        String badLogStoreLocation = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
        badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

        return badLogStoreLocation;
    }

    private void updateBagLogFileName() {
        csvFilepath = new File(csvFilepath).getName();
        if (csvFilepath.indexOf(".") > -1) {
            csvFilepath = csvFilepath.substring(0, csvFilepath.indexOf("."));
        }

        csvFilepath = csvFilepath + '_' + System.currentTimeMillis() + ".log";

    }

    private void startProcess(final int numberOfNodes) throws RuntimeException {
        exec = Executors.newFixedThreadPool(numberOfNodes);

        Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws RuntimeException {
                StandardLogService
                        .setThreadName(StandardLogService.getPartitionID(meta.getCubeName()), null);
                try {
                    if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
                        doProcessWithCheckPoint();
                    } else {
                        doProcess();
                    }
                } catch (Throwable e) {
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                            "Thread is terminated due to error");
                    threadStatusObserver.notifyFailed(e);
                }
                return null;
            }
        };
        List<Future<Void>> results = new ArrayList<Future<Void>>(10);
        for (int i = 0; i < numberOfNodes; i++) {
            results.add(exec.submit(callable));
        }

        resultArray = results.toArray(new Future[results.size()]);
        boolean completed = false;
        try {
            while (!completed) {
                completed = true;
                for (int j = 0; j < resultArray.length; j++) {
                    if (!resultArray[j].isDone()) {
                        completed = false;
                    }

                }
                if (isTerminated) {
                    exec.shutdownNow();
                    throw new RuntimeException("Interrupted due to failing of other threads");
                }
                Thread.sleep(100);

            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread InterruptedException", e);
        }
        exec.shutdown();
    }

    private int[] getUpdatedLens(int[] lens, boolean[] presentDims) {
        int k = 0;
        int[] integers = new int[meta.normLength];
        for (int i = 0; i < lens.length; i++) {
            if (presentDims[i]) {
                integers[k] = lens[i];
                k++;
            }
        }
        return integers;
    }

    private String[] getUpdatedDims(String[] dims, String[] NoDictionaryCols, boolean[] presentDims) {
        int k = 0;
        String[] normDims = null;
        if (null != meta.NoDictionaryCols) {
            normDims = new String[meta.normLength + meta.NoDictionaryCols.length];
        } else {
            normDims = new String[meta.normLength];
        }
        for (int i = 0; i < dims.length; i++) {
            if (presentDims[i]) {
                normDims[k] = dims[i];
                k++;
            }
        }
        if (null != NoDictionaryCols) {
            for (int j = 0; j < NoDictionaryCols.length; j++) {
                normDims[k++] = NoDictionaryCols[j];
            }
        }
        return normDims;
    }

    /**
     * updateAndWriteSliceMetadataFile
     *
     * @throws KettleException
     */
    private void updateAndWriteSliceMetadataFile() throws KettleException {
        String storeLocation = updateStoreLocationAndPopulateCarbonInfo(
                meta.getSchemaName() + '/' + meta.getCubeName());
        //

        int restructFolderNumber = meta.getCurrentRestructNumber()/*CarbonUtil.checkAndReturnNextRestructFolderNumber(storeLocation,"RS_")*/;

        String sliceMetaDataFilePath =
                storeLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
                        + restructFolderNumber + File.separator + meta.getTableName()
                        + File.separator + CarbonUtil
                        .getSliceMetaDataFileName(meta.getCurrentRestructNumber());

        SliceMetaData sliceMetaData = new SliceMetaData();
        //
        sliceMetaData.setDimensions(
                getUpdatedDims(meta.dimColNames, meta.NoDictionaryCols, meta.dimPresent));
        sliceMetaData.setActualDimensions(meta.dimColNames);
        sliceMetaData.setMeasures(meta.measureColumn);
        sliceMetaData.setActualDimLens(getUpdatedCardinality(meta.dimLens));
        sliceMetaData.setDimLens(getUpdatedLens(meta.dimLens, meta.dimPresent));
        sliceMetaData.setMeasuresAggregator(meta.msrAggregators);
        sliceMetaData.setHeirAnKeySize(meta.getHeirKeySize());
        sliceMetaData.setTableNamesToLoadMandatory(null);
        sliceMetaData.setComplexTypeString(meta.getComplexTypeString());
        sliceMetaData.setKeyGenerator(
                KeyGeneratorFactory.getKeyGenerator(getUpdatedLens(meta.dimLens, meta.dimPresent)));
        CarbonDataProcessorUtil.writeFileAsObjectStream(sliceMetaDataFilePath, sliceMetaData);
    }

    /**
     * @param dimCardinality
     * @return
     */
    private int[] getUpdatedCardinality(int[] dimCardinality) {
        int[] maxSurrogateKeyArray = data.getSurrogateKeyGen().max;

        List<Integer> dimCardWithComplex = new ArrayList<Integer>();

        for (int i = 0; i < meta.dimColNames.length; i++) {
            GenericDataType complexDataType = meta.complexTypes
                    .get(meta.dimColNames[i].substring(meta.getTableName().length() + 1));
            if (complexDataType != null) {
                complexDataType
                        .fillCardinalityAfterDataLoad(dimCardWithComplex, maxSurrogateKeyArray);
            } else {
                dimCardWithComplex.add(maxSurrogateKeyArray[i]);
            }
        }

        int[] complexDimCardinality = new int[dimCardWithComplex.size()];
        for (int i = 0; i < dimCardWithComplex.size(); i++) {
            complexDimCardinality[i] = dimCardWithComplex.get(i);
        }
        return complexDimCardinality;
    }

    private void doProcess() throws RuntimeException {
        try {
            while (true) {
                Object[] r = null;
                synchronized (getRowLock) {

                    r = getRow();
                    readCounter++;
                }

                // no more input to be expected...
                if (r == null) {
                    readCounter--;
                    break;
                }
                Object[] out = process(r);
                if (null == out) {
                    continue;
                }

                synchronized (putRowLock) {
                    putRow(data.getOutputRowMeta(), out);
                    processRecord();
                    writeCounter++;
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processRecord() {
        if (readCounter % logCounter == 0) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTableName());
            String logMessage = "Carbon Csv Based Seq Gen Step: Record Read : " + readCounter;
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
        }
    }

    private void doProcessWithCheckPoint() throws RuntimeException {
        try {

            while (true) {
                Object[] r = null;
                DataProcessorRecordHolder oriRecords = null;
                synchronized (getRowLock) {
                    oriRecords = new DataProcessorRecordHolder(checkPointSize, seqNumber++);
                    for (int i = 0; i < checkPointSize; i++) {
                        r = getRow();
                        if (r == null) {
                            break;
                        }

                        oriRecords.addRow(r);
                        readCounter++;
                    }
                }

                processRows(oriRecords);

                // no more input to be expected...
                if (r == null) {
                    readCounter--;
                    break;
                }

                if (localQueue.size() > threshold) {
                    synchronized (putRowLock) {
                        if (localQueue.size() > threshold) {
                            for (int i = 0; i < toCopy; i++) {
                                dataQueue.offer(localQueue.poll());
                            }
                            putRowInSeqence();
                        }

                    }
                }

            }
        } catch (RuntimeException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Exception happened while processing rows in Do Process", e);
            throw e;
        } catch (Exception e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Exception happened while processing rows in Do Process", e);
            throw new RuntimeException(e);
        } catch (Throwable t) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Exception happened while processing rows in Do Process", t);
            throw new RuntimeException(t);
        }

    }

    private void processRows(DataProcessorRecordHolder oriRecordHolders)
            throws KettleStepException {
        Object[][] originalRow = oriRecordHolders.getOriginalRow();

        for (int i = 0; i < checkPointSize; i++) {
            if (null == originalRow[i]) {
                break;
            }
            Object[] process = process(originalRow[i]);
            originalRow[i] = null;
            if (process == null) {
                continue;
            }
            oriRecordHolders.addProcessedRows(process);
        }

        synchronized (putRowLock) {
            while (!localQueue.offer(oriRecordHolders)) {
                putRowInSeqence();
            }
        }
    }

    private void putRowInSeqence() throws KettleStepException {

        putRowFuture = putRowExecutorService.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                try {
                    while (!dataQueue.isEmpty()) {
                        DataProcessorRecordHolder recordHolders = dataQueue.poll();
                        if (null == recordHolders) {
                            return null;
                        }

                        Object[][] processedRow = recordHolders.getProcessedRow();
                        // while(!processedRecords.isEmpty())
                        for (int i = 0; i < checkPointSize; i++) {
                            if (processedRow[i] == null) {
                                break;
                            }

                            putRow(data.getOutputRowMeta(), processedRow[i]);
                            processedRow[i] = null;
                            processRecord();
                            writeCounter++;
                        }
                    }

                } catch (Throwable t) {
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                            "Not able to process rows to next step");
                    throw new KettleException(t);
                }
                return null;
            }
        });
    }

    private String updateStoreLocationAndPopulateCarbonInfo(String schemaCubeName) {
        //
        String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
        String strLoc = CarbonProperties.getInstance()
                .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        File f = new File(strLoc);
        String absoluteStorePath = f.getAbsolutePath();
        //
        if (absoluteStorePath.length() > 0
                && absoluteStorePath.charAt(absoluteStorePath.length() - 1) == '/') {
            absoluteStorePath = absoluteStorePath + schemaCubeName;
        } else {
            absoluteStorePath =
                    absoluteStorePath + System.getProperty("file.separator") + schemaCubeName;
        }
        return absoluteStorePath;
    }

    private Object[] process(Object[] r) throws RuntimeException {
        try {
            r = changeNullValueToNullString(r);
            Object[] out = populateOutputRow(r);
            if (out != null) {
                checkPoint.updateInfoFields(r, out);
                if (CheckPointHanlder.IS_CHECK_POINT_NEEDED) {
                    synchronized (seqGenLock) {
                        if (processed++ % buffer == 0) {
                            data.getSurrogateKeyGen().writeDataToFileAndCloseStreams();
                        }
                    }
                }

                for (int i = 0; i < meta.normLength; i++) {
                    if (null == RemoveDictionaryUtil.getDimension(i, out)) {
                        RemoveDictionaryUtil.setDimension(i, 1, out);
                    }
                }
            }
            return out;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] changeNullValueToNullString(Object[] rowValue) {
        int i = 0;
        for (Object obj : rowValue) {
            if (obj != null) {
                if (obj.equals(valueToCheckAgainst)) {
                    rowValue[i] = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
                }
            } else if (i > meta.memberMapping.length) {
                return rowValue;
            } else {
                rowValue[i] = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
            }

            i++;
        }

        return rowValue;
    }

    private Object[] populateOutputRow(Object[] r) throws KettleException {

        // Copy the dimension String values to output
        int[] memberMapping = meta.memberMapping;
        int inputColumnsSize = metaColumnNames.length;
        boolean isGenerated = false;
        int generatedSurrogate = -1;

        //If CSV Exported from DB and we enter one row down then that row become empty.
        // In that case it will have first value empty and other values will be null
        // So If records is coming like this then we need to write this records as a bad Record.

        if (null == r[1]) {
            badRecordslogger
                    .addBadRecordsToBilder(r, inputColumnsSize, "Column Names are coming NULL",
                            "null");
            return null;
        }

        Map<String, Dictionary> dictionaryCaches = surrogateKeyGen.getDictionaryCaches();
        Object[] out = new Object[meta.normLength + meta.msrs.length + checkPoint
                .getCheckPointInfoFieldCount()];
        int dimLen = meta.dims.length;

        Object[] newArray = new Object[CarbonCommonConstants.ARRAYSIZE];

        ByteBuffer[] byteBufferArr = null;
        if (null != meta.NoDictionaryCols) {
            byteBufferArr = new ByteBuffer[meta.NoDictionaryCols.length + meta.complexTypes.size()];
        }
        int i = 0;
        int n = 0;
        int index = 0;
        int l = 0;
        int msrCount = 0;
        boolean isNull = false;
        int complexIndex = meta.NoDictionaryCols.length;
        for (int j = 0; j < inputColumnsSize; j++) {
            String columnName = metaColumnNames[j];
            String foreignKeyColumnName = foreignKeyMappingColumns[j];

            // TODO check if it is ignore dictionary dimension or not . if yes directly write byte buffer

            if (null != meta.NoDictionaryCols && isDimensionNoDictionary(meta.NoDictionaryCols,
                    columnName) && !measurePresentMapping[j]) {
                processnoDictionaryDim(getIndexOfNoDictionaryDims(meta.NoDictionaryCols, columnName),
                        (String) r[j], byteBufferArr);
                continue;
            }

            // There is a possibility that measure can be referred as dimensions also
            // so in that case we need to just copy the value into the measure column index.
            //if it enters here means 3 possibility
            //1) this is not foreign key it can be direct columns
            //2) This column present in the csv file but in the schema it is not present.
            //3) This column can be measure column

            if (measurePresentMapping[j]) {
                String msr = r[j] == null ? null : r[j].toString();
                isNull = CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(msr);
                if (measureSurrogateReqMapping[j] && !isNull) {
                    Integer surrogate = 0;
                    if (null == foreignKeyColumnName) {
                        // If foreignKeyColumnName is null till here that means this
                        // measure column is of type count and data type may be string
                        // so we have to create the surrogate key for the values.
                        surrogate = createSurrogateForMeasure(msr, columnName,
                                presentColumnMapIndex[j]);
                        if (presentColumnMapIndex[j] > -1) {
                            isGenerated = true;
                            generatedSurrogate = surrogate;
                        }
                    } else {
                        surrogate = surrogateKeyGen
                                .generateSurrogateKeys(msr, foreignKeyColumnName);
                    }

                    out[memberMapping[dimLen + index]] = surrogate.doubleValue();
                } else {

                    try {
                        out[memberMapping[dimLen + index]] =
                                (isNull || msr == null || msr.length() == 0) ?
                                        null :
                                        DataTypeUtil.getMeasureValueBasedOnDataType(msr,
                                                msrDataType[meta.msrMapping[msrCount]]);
                    } catch (NumberFormatException e) {
                        try {
                            msr = msr.replaceAll(",", "");
                            out[memberMapping[dimLen + index]] = DataTypeUtil
                                    .getMeasureValueBasedOnDataType(msr,
                                            msrDataType[meta.msrMapping[msrCount]]);
                        } catch (NumberFormatException ex) {
                            badRecordslogger.addBadRecordsToBilder(r, inputColumnsSize,
                                    "Measure should be number", valueToCheckAgainst);
                            return null;
                        }
                    }
                }

                index++;
                msrCount++;
                if (presentColumnMapIndex[j] < 0 && null == foreignKeyColumnName) {
                    continue;
                }
            }

            boolean isPresentInSchema = false;
            if (null == foreignKeyColumnName) {
                //if it enters here means 3 possibility
                //1) this is not foreign key it can be direct columns
                //2) This column present in the csv file but in the schema it is not present.
                //3) This column can be measure column
                int m = presentColumnMapIndex[j];
                if (m >= 0) {
                    isPresentInSchema = true;
                }

                if (isPresentInSchema) {
                    foreignKeyColumnName = meta.dimColNames[m];
                    n = m;
                } else {
                    continue;
                }
            }

            //If it refers to multiple hierarchy by same foreign key
            if (foreignKeyMappingColumnsForMultiple[j] != null) {
                String[] splittedHiers = foreignKeyMappingColumnsForMultiple[j];

                for (String hierForignKey : splittedHiers) {
                    Dictionary dicCache = dictionaryCaches.get(hierForignKey);

                    String actualHierName = null;
                    if (!isPresentInSchema) {
                        actualHierName = meta.hierNames[l++];

                    }

                    Int2ObjectMap<int[]> cache = surrogateKeyGen.getHierCache().get(actualHierName);
                    int[] surrogateKeyForHierarchy = null;
                    if (null != cache) {

                        Integer keyFromCsv = dicCache.getSurrogateKey(((String) r[j]));

                        if (null != keyFromCsv) {
                            surrogateKeyForHierarchy = cache.get(keyFromCsv);
                        } else {
                            addMemberNotExistEntry(r, inputColumnsSize, j, columnName);
                            return null;
                        }
                        // If cardinality exceeded for some levels then
                        // for that hierarchy will not be their
                        // so while joining with fact table if we are
                        // getting this scenerio we will log it
                        // in bad records
                        if (null == surrogateKeyForHierarchy) {
                            addCardinalityExcededEntry(r, inputColumnsSize, j, columnName);
                            return null;

                        }
                    } else {
                        int[] propIndex = propMap.get(foreignKeyColumnName);
                        Object[] props;
                        if (null == propIndex) {
                            props = new Object[0];
                        } else {
                            props = new Object[propIndex.length];
                            for (int ind = 0; ind < propIndex.length; ind++) {
                                props[ind] = r[propIndex[ind]];
                            }
                        }
                        surrogateKeyForHierarchy = new int[1];
                        surrogateKeyForHierarchy[0] = surrogateKeyGen
                                .generateSurrogateKeys((String) r[j], foreignKeyColumnName);
                    }
                    for (int k = 0; k < surrogateKeyForHierarchy.length; k++) {
                        if (dimPresentCsvOrder[i]) {
                            out[memberMapping[i]] = Integer.valueOf(surrogateKeyForHierarchy[k]);
                        }

                        i++;
                    }

                }

            }
            //If it refers to single hierarchy
            else {
                String complexDataTypeName =
                        foreignKeyColumnName.substring(meta.getTableName().length() + 1);
                GenericDataType complexType = meta.getComplexTypes().get(complexDataTypeName);
                if (complexType != null) {
                    try {
                        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(byteArray);
                        complexType.parseStringAndWriteByteArray(meta.getTableName(), (String) r[j],
                                new String[] { meta.getComplexDelimiterLevel1(),
                                        meta.getComplexDelimiterLevel2() }, 0, dataOutputStream,
                                surrogateKeyGen);
                        byteBufferArr[complexIndex++] = ByteBuffer.wrap(byteArray.toByteArray());
                        if (null != byteArray) {
                            byteArray.close();
                        }
                    } catch (IOException e1) {
                        throw new KettleException(
                                "Parsing complex string and generating surrogates/ByteArray failed. ",
                                e1);
                    }
                    i++;
                } else {
                    Dictionary dicCache = dictionaryCaches.get(foreignKeyColumnName);

                    String actualHierName = null;
                    if (!isPresentInSchema) {
                        actualHierName = meta.hierNames[l++];

                    }

                    Int2ObjectMap<int[]> cache = surrogateKeyGen.getHierCache().get(actualHierName);
                    int[] surrogateKeyForHrrchy = null;
                    if (null != cache) {
                        Integer keyFromCsv = dicCache.getSurrogateKey(((String) r[j]));

                        if (null != keyFromCsv) {
                            surrogateKeyForHrrchy = cache.get(keyFromCsv);
                        } else {
                            addMemberNotExistEntry(r, inputColumnsSize, j, columnName);
                            return null;
                        }
                        // If cardinality exceeded for some levels then for that hierarchy will not be their
                        // so while joining with fact table if we are getting this scenerio we will log it
                        // in bad records
                        if (null == surrogateKeyForHrrchy) {
                            addCardinalityExcededEntry(r, inputColumnsSize, j, columnName);
                            return null;

                        }
                    } else {
                        int[] propIndex = propMap.get(foreignKeyColumnName);
                        Object[] properties;
                        if (null == propIndex) {
                            properties = new Object[0];
                        } else {
                            properties = new Object[propIndex.length];
                            for (int ind = 0; ind < propIndex.length; ind++) {
                                properties[ind] = r[propIndex[ind]];
                            }
                        }
                        surrogateKeyForHrrchy = new int[1];
                        if (isGenerated && !isNull) {
                            surrogateKeyForHrrchy[0] = generatedSurrogate;
                            isGenerated = false;
                            generatedSurrogate = -1;
                        } else {
                            surrogateKeyForHrrchy[0] = surrogateKeyGen
                                    .generateSurrogateKeys(((String) r[j]), foreignKeyColumnName);
                        }
                        if (surrogateKeyForHrrchy[0] == -1) {
                            addCardinalityExcededEntry(r, inputColumnsSize, j, columnName);
                            return null;
                        }
                    }
                    for (int k = 0; k < surrogateKeyForHrrchy.length; k++) {
                        if (dimPresentCsvOrder[i]) {
                            if (duplicateColMapping[j] != null) {
                                for (int m = 0; m < duplicateColMapping[j].length; m++) {
                                    out[duplicateColMapping[j][m]] =
                                            Integer.valueOf(surrogateKeyForHrrchy[k]);
                                }
                            } else {
                                out[memberMapping[i]] = Integer.valueOf(surrogateKeyForHrrchy[k]);
                            }
                        }

                        i++;
                    }

                }
            }
        }

        insertHierIfRequired(out);

        RemoveDictionaryUtil.prepareOut(newArray, byteBufferArr, out, dimLen);

        return newArray;
    }

    private void addCardinalityExcededEntry(Object[] r, int inputRowSize, int j,
            String columnName) {
        badRecordslogger.addBadRecordsToBilder(r, inputRowSize,
                "For Coulmn " + columnName + " \"" + r[j] + "\""
                        + " members hierarchy not loaded as the "
                        + "cardinality exceeded while loading dimension table.",
                valueToCheckAgainst);
    }

    private void addMemberNotExistEntry(Object[] r, int inputRowSize, int j, String columnName) {
        badRecordslogger.addBadRecordsToBilder(r, inputRowSize,
                "For Coulmn " + columnName + " \"" + r[j] + "\""
                        + " member not exist in the dimension table ", valueToCheckAgainst);
    }

    private void insertHierIfRequired(Object[] out) throws KettleException {
        if (denormHierarchies.size() > 0) {
            insertHierarichies(out);
        }
    }

    private int[] createPresentColumnMapIndex() {
        int[] presentColumnMapIndex = new int[getInputRowMeta().size()];
        duplicateColMapping = new int[getInputRowMeta().size()][];
        Arrays.fill(presentColumnMapIndex, -1);
        for (int j = 0; j < getInputRowMeta().size(); j++) {
            String columnName = getInputRowMeta().getValueMeta(j).getName();

            int m = 0;

            String foreignKey = meta.foreignKeyHierarchyMap.get(columnName);
            if (foreignKey == null) {
                List<Integer> repeats = new ArrayList<Integer>(10);
                for (String col : meta.dimColNames) {
                    if (col.equalsIgnoreCase(meta.getTableName() + '_' + columnName)) {
                        presentColumnMapIndex[j] = m;
                        repeats.add(m);
                    }
                    m++;
                }
                if (repeats.size() > 1) {
                    int[] dims = new int[repeats.size()];
                    for (int i = 0; i < dims.length; i++) {
                        dims[i] = repeats.get(i);
                    }
                    duplicateColMapping[j] = dims;
                }

            } else {
                for (String col : meta.actualDimArray) {
                    if (col.equalsIgnoreCase(columnName)) {
                        presentColumnMapIndex[j] = m;
                        break;
                    }
                    m++;
                }

            }
        }
        return presentColumnMapIndex;
    }

    private int[] createPresentColumnMapIndexForAggregate() {
        int[] presentColumnMapIndex = new int[getInputRowMeta().size()];
        duplicateColMapping = new int[getInputRowMeta().size()][];
        Arrays.fill(presentColumnMapIndex, -1);
        for (int j = 0; j < getInputRowMeta().size(); j++) {
            String columnName = getInputRowMeta().getValueMeta(j).getName();

            int m = 0;

            String foreignKey = meta.foreignKeyHierarchyMap.get(columnName);
            if (foreignKey == null) {
                for (String col : meta.actualDimArray) {
                    if (col.equalsIgnoreCase(columnName)) {
                        presentColumnMapIndex[j] = m;
                        break;
                    }
                    m++;
                }
            }
        }
        return presentColumnMapIndex;
    }

    private String[] createColumnArrayFromMeta() {
        String[] metaColumnNames = new String[getInputRowMeta().size()];
        for (int j = 0; j < getInputRowMeta().size(); j++) {
            metaColumnNames[j] = getInputRowMeta().getValueMeta(j).getName();
        }
        return metaColumnNames;
    }

    private boolean[] createMeasureMappigs(List<String> measureCol) {
        int size = getInputRowMeta().size();
        boolean[] measurePresentMapping = new boolean[size];
        for (int j = 0; j < size; j++) {
            String columnName = getInputRowMeta().getValueMeta(j).getName();
            if (measureCol.contains(columnName)) {
                measurePresentMapping[j] = true;
            }
        }
        return measurePresentMapping;

    }

    private boolean[] createMeasureSurrogateReqMapping() {
        int size = getInputRowMeta().size();
        boolean[] measureSuurogateReqMapping = new boolean[size];
        for (int j = 0; j < size; j++) {
            String columnName = getInputRowMeta().getValueMeta(j).getName();
            Boolean isPresent = meta.getMeasureSurrogateRequired().get(columnName);
            if (null != isPresent && isPresent) {
                measureSuurogateReqMapping[j] = true;
            }
        }
        return measureSuurogateReqMapping;
    }

    private void createForeignKeyMappingColumns() {
        int size = getInputRowMeta().size();
        foreignKeyMappingColumns = new String[size];
        foreignKeyMappingColumnsForMultiple = new String[size][];
        for (int j = 0; j < size; j++) {
            String columnName = getInputRowMeta().getValueMeta(j).getName();
            String foreignKeyColumnName = meta.foreignKeyPrimaryKeyMap.get(columnName);
            if (foreignKeyColumnName != null) {
                if (foreignKeyColumnName.indexOf(CarbonCommonConstants.COMA_SPC_CHARACTER) > -1) {
                    String[] splittedHiers =
                            foreignKeyColumnName.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
                    foreignKeyMappingColumnsForMultiple[j] = splittedHiers;
                    foreignKeyMappingColumns[j] = foreignKeyColumnName;
                } else {
                    foreignKeyMappingColumns[j] = foreignKeyColumnName;
                }
            }
        }
    }

    private int createSurrogateForMeasure(String member, String columnName, int index)
            throws KettleException {
        String colName = meta.getTableName() + '_' + columnName;

        return data.getSurrogateKeyGen().getSurrogateForMeasure(member, colName);

    }

    private void insertHierarichies(Object[] rowWithKeys) throws KettleException {

        try {
            for (String hierName : denormHierarchies) {

                String storeLocation = "";
                String hierInprogName = hierName + CarbonCommonConstants.HIERARCHY_FILE_EXTENSION;
                HierarchyValueWriterForCSV hierWriter = nrmlizedHierWriterMap.get(hierInprogName);
                storeLocation = loadFolderLoc;
                if (null == filemanager) {
                    filemanager = new LoadFolderData();
                    filemanager.setName(storeLocation);
                }
                if (null == hierWriter) {
                    FileData fileData = new FileData(hierInprogName, storeLocation);
                    hierWriter = new HierarchyValueWriterForCSV(hierInprogName, storeLocation);
                    filemanager.add(fileData);
                    nrmlizedHierWriterMap.put(hierInprogName, hierWriter);
                }

                int[] levelsIndxs = meta.hirches.get(hierName);
                int[] levelSKeys = new int[levelsIndxs.length];

                if (meta.complexTypes.get(meta.hierColumnMap.get(hierName)[0]) == null) {
                    for (int i = 0; i < levelSKeys.length; i++) {
                        levelSKeys[i] = (Integer) rowWithKeys[levelsIndxs[i]];
                    }

                    if (levelSKeys.length > 1) {
                        data.getSurrogateKeyGen()
                                .checkNormalizedHierExists(levelSKeys, hierName, hierWriter);
                    }
                }
            }
        } catch (Exception e) {
            throw new KettleException(e.getMessage(), e);
        }
    }

    private boolean isMeasureColumn(String msName, boolean compareWithTable) {
        String msrNameTemp;
        for (String msrName : meta.measureColumn) {
            msrNameTemp = msrName;
            if (compareWithTable) {
                msrNameTemp = meta.getTableName() + '_' + msrNameTemp;
            }
            if (msrNameTemp.equalsIgnoreCase(msName)) {
                return true;
            }
        }
        return false;
    }

    private int[] getMeasureOriginalIndexes(String[] originalMsrCols) {
        List<String> currMsrCol = new ArrayList<String>(10);
        for (int i = 0; i < getInputRowMeta().size(); i++) {
            String columnName = getInputRowMeta().getValueMeta(i).getName();
            for (String measureCol : originalMsrCols) {
                if (measureCol.equalsIgnoreCase(columnName)) {
                    currMsrCol.add(columnName);
                    break;
                }
            }
        }
        String[] currentMsrCols = currMsrCol.toArray(new String[currMsrCol.size()]);

        int[] indexs = new int[currentMsrCols.length];

        for (int i = 0; i < currentMsrCols.length; i++) {
            for (int j = 0; j < originalMsrCols.length; j++) {
                if (currentMsrCols[i].equalsIgnoreCase(originalMsrCols[j])) {
                    indexs[i] = j;
                    break;
                }
            }
        }

        return indexs;
    }

    private int[] getMemberMappingOriginalIndexes() {
        int[] memIndexes = new int[meta.dimLens.length + meta.msrs.length];
        Arrays.fill(memIndexes, -1);
        String actualColumnName = null;
        List<String> allColumnsNamesFromCSV = new ArrayList<String>(10);
        for (int i = 0; i < getInputRowMeta().size(); i++) {
            allColumnsNamesFromCSV.add(getInputRowMeta().getValueMeta(i).getName());
        }

        List<String> currentColName = new ArrayList<String>(meta.actualDimArray.length);
        List<String> duplicateNames = new ArrayList<String>(10);
        for (int i = 0; i < getInputRowMeta().size(); i++) {
            String columnName = getInputRowMeta().getValueMeta(i).getName();
            String hier = meta.foreignKeyHierarchyMap.get(columnName);

            String uniqueName = meta.getTableName() + '_' + columnName;
            if (null != hier) {

                if (hier.indexOf(CarbonCommonConstants.COMA_SPC_CHARACTER) > -1) {
                    getCurrenColForMultiHier(currentColName, hier);
                } else {
                    String tableName = meta.getHierDimTableMap().get(hier);

                    String[] columns = meta.hierColumnMap.get(hier);

                    if (null != columns) {
                        for (String column : columns) {
                            //currentColumnNames[k++] = column;
                            currentColName.add(tableName + '_' + column);
                        }
                    }
                }

                if (isMeasureColumn(columnName, false)) {
                    currentColName.add(uniqueName);
                }

            } else // then it can be direct column name if not foreign key.
            {
                if (!meta.isAggregate()) {
                    currentColName.add(uniqueName);
                    //add to duplicate column list if it is a repeated column. it is required since the member mapping is 1 to 1 mapping
                    //of csv columns and schema columns. so if schema columns are repeated then we have to handle it in special way.
                    checkAndAddDuplicateCols(duplicateNames, uniqueName);
                } else {
                    actualColumnName = meta.columnAndTableNameColumnMapForAggMap.get(columnName);
                    if (actualColumnName != null) {
                        currentColName
                                .add(meta.columnAndTableNameColumnMapForAggMap.get(columnName));
                    } else {
                        currentColName.add(uniqueName);
                    }
                }
            }
        }
        //Add the duplicate columns at the end so that it won't create any problem with current mapping.
        currentColName.addAll(duplicateNames);
        String[] currentColNamesArray = currentColName.toArray(new String[currentColName.size()]);

        // We will use same array for dimensions and measures
        // First create the mapping for dimensions.
        int dimIndex = 0;
        Map<String, Boolean> counterMap = new HashMap<String, Boolean>(16);
        // Setting dimPresent value in CSV order as we need it later
        dimPresentCsvOrder = new boolean[meta.dimPresent.length];
        // var used to set measures value (in next loop)
        int toAddInIndex = 0;
        int tmpIndex = 0;
        for (int i = 0; i < currentColNamesArray.length; i++) {
            if (isMeasureColumn(currentColNamesArray[i], true) && isNotInDims(
                    currentColNamesArray[i])) {
                continue;
            }
            int n = 0;
            for (int j = 0; j < meta.actualDimArray.length; j++) {

                if (currentColNamesArray[i].equalsIgnoreCase(meta.dimColNames[j])) {

                    String mapKey = currentColNamesArray[i] + "__" + j;
                    if (null == counterMap.get(mapKey)) {
                        dimPresentCsvOrder[tmpIndex] = meta.dimPresent[j];//CHECKSTYLE:ON
                        tmpIndex++;
                        counterMap.put(mapKey, true);
                        if (!meta.dimPresent[j]) {
                            dimIndex++;
                            continue;
                        }
                        memIndexes[dimIndex++] = n;
                        // Added one more value to memIndexes, increase counter
                        toAddInIndex++;
                        break;
                    } else {
                        n++;
                        continue;
                    }
                }
                if (meta.dimPresent[j]) {
                    n++;
                }
            }
        }

        for (int actDimLen = 0; actDimLen < meta.actualDimArray.length; actDimLen++) {
            boolean found = false;
            for (int csvHeadLen = 0; csvHeadLen < currentColNamesArray.length; csvHeadLen++) {
                if (meta.dimColNames[actDimLen]
                        .equalsIgnoreCase(currentColNamesArray[csvHeadLen])) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                dimIndex++;
                toAddInIndex++;
            }
        }

        // Now create the mapping of measures
        // There may be case when measure column is present in the CSV file
        // but not present in the schema , in that case we need to skip that column while
        // sending the output to next step.
        // Or Measure can be in any ordinal in the csv

        int k = 0;
        Map<String, Boolean> existsMap = new HashMap<String, Boolean>(16);

        for (int i = 0; i < currentColNamesArray.length; i++) {
            k = calculateMeasureOriginalIndexes(memIndexes, currentColNamesArray, dimIndex,
                    toAddInIndex, k, existsMap, i);
        }

        return memIndexes;
    }

    private void getCurrenColForMultiHier(List<String> currentColName, String hier) {
        String[] splittedHiers = hier.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
        for (String hierName : splittedHiers) {
            String tableName = meta.getHierDimTableMap().get(hierName);

            String[] cols = meta.hierColumnMap.get(hierName);
            if (null != cols) {
                for (String column : cols) {
                    currentColName.add(tableName + '_' + column);
                }
            }
        }
    }

    private void checkAndAddDuplicateCols(List<String> duplicateNames, String uniqueName) {
        boolean exists = false;
        for (int i = 0; i < meta.dimColNames.length; i++) {
            if (uniqueName.equals(meta.dimColNames[i])) {
                if (exists) {
                    duplicateNames.add(uniqueName);
                }
                exists = true;
            }
        }
    }

    /**
     * calculateMeasureOriginalIndexes
     *
     * @param memIndexes
     * @param currentColNamesArray
     * @param dimIndex
     * @param toAddInIndex
     * @param k
     * @param existsMap
     * @param i
     * @return
     */
    public int calculateMeasureOriginalIndexes(int[] memIndexes, String[] currentColNamesArray,
            int dimIndex, int toAddInIndex, int k, Map<String, Boolean> existsMap, int i) {
        for (int j = 0; j < meta.measureColumn.length; j++) {
            if (currentColNamesArray[i]
                    .equalsIgnoreCase(meta.getTableName() + '_' + meta.measureColumn[j])) {
                if (existsMap.get(meta.measureColumn[j]) == null) {
                    memIndexes[k + dimIndex] = toAddInIndex + j;
                    k++;
                    existsMap.put(meta.measureColumn[j], true);
                    break;
                }
            }
        }
        return k;
    }

    private boolean isNotInDims(String columnName) {
        for (String dimName : meta.dimColNames) {
            if (dimName.equalsIgnoreCase(columnName)) {
                return false;
            }
        }
        return true;
    }

    private void closeConnections() throws KettleException {
        try {
            for (Entry<String, Connection> entry : cons.entrySet()) {
                entry.getValue().close();
            }
            cons.clear();
        } catch (Exception ex) {
            throw new KettleException(ex.getMessage(), ex);
        }
    }

    public synchronized void connect(String group, String partitionId, Database database)
            throws Exception {
        // Before anything else, let's see if we already have a connection
        // defined for this group/partition!
        // The group is called after the thread-name of the transformation or
        // job that is running
        // The name of that threadname is expected to be unique (it is in
        // Kettle)
        // So the deal is that if there is another thread using that, we go for
        // it.
        //
        Connection conn = null;
        if (!Const.isEmpty(group)) {

            DatabaseConnectionMap map = DatabaseConnectionMap.getInstance();

            // Try to find the connection for the group
            Database lookup = map.getDatabase(group, partitionId, database);
            if (lookup == null) // We already opened this connection for the
            // partition & database in this group
            {
                // Do a normal connect and then store this database object for
                // later re-use.
                conn = ConnectionPoolUtil
                        .getConnection(log, database.getDatabaseMeta(), partitionId);

                map.storeDatabase(group, partitionId, database);
            } else {
                conn = lookup.getConnection();
                lookup.setOpened(lookup.getOpened() + 1); // if this counter
                // hits 0 again, close
                // the connection.
            }
        } else {
            // Proceed with a normal connect
            conn = ConnectionPoolUtil.getConnection(log, database.getDatabaseMeta(), partitionId);
        }
        database.setConnection(conn);
    }

    /**
     * According to the hierarchies,generate the varLengthKeyGenerator
     *
     * @param keyGenerators
     * @param hirches
     * @param dimLens
     */
    private void updateHierarchyKeyGenerators(Map<String, KeyGenerator> keyGenerators,
            Map<String, int[]> hirches, int[] dimLens, String[] dimCols) {
        //
        String timeHierNameVal = "";
        if (meta.getCarbonTime() == null || "".equals(meta.getCarbonTime())) {
            timeHierNameVal = "";
        } else {
            String[] hies = meta.getCarbonTime().split(":");
            timeHierNameVal = hies[1];
        }

        // Set<Entry<String,int[]>> hierSet = hirches.entrySet();
        Iterator<Entry<String, int[]>> itr = hirches.entrySet().iterator();

        while (itr.hasNext()) {
            Entry<String, int[]> hieEntry = itr.next();

            int[] a = hieEntry.getValue();
            int[] lens = new int[a.length];
            String name = hieEntry.getKey();
            //
            if (name.equalsIgnoreCase(timeHierNameVal)) {
                for (int i = 0; i < a.length; i++) {//CHECKSTYLE:OFF
                    lens[i] = dimLens[a[i]];
                }//CHECKSTYLE:ON
            } else {
                String[] columns = meta.hierColumnMap.get(name);

                if (meta.getComplexTypes().get(columns[0]) != null) {
                    continue;
                }
                boolean isNoDictionary = false;
                for (int i = 0; i < a.length; i++) {
                    if (null != meta.NoDictionaryCols && isDimensionNoDictionary(
                            meta.NoDictionaryCols, columns[i])) {
                        isNoDictionary = true;
                        break;
                    }
                }
                //if no dictionary column then do not populate the dim lens
                if (isNoDictionary) {
                    continue;
                }
                //
                for (int i = 0; i < a.length; i++) {
                    int newIndex = -1;
                    for (int j = 0; j < dimCols.length; j++) {
                        //
                        if (checkDimensionColName(dimCols[j], columns[i])) {
                            newIndex = j;
                            break;
                        }
                    }//CHECKSTYLE:OFF
                    lens[i] = dimLens[newIndex];
                }//CHECKSTYLE:ON
            }
            //
            KeyGenerator generator = KeyGeneratorFactory.getKeyGenerator(lens);
            keyGenerators.put(name, generator);

        }

        Iterator<Entry<String, GenericDataType>> complexMap =
                meta.getComplexTypes().entrySet().iterator();
        while (complexMap.hasNext()) {
            Entry<String, GenericDataType> complexDataType = complexMap.next();
            List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
            complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
            for (GenericDataType eachPrimitive : primitiveTypes) {
                KeyGenerator generator = KeyGeneratorFactory.getKeyGenerator(new int[] { -1 });
                keyGenerators.put(eachPrimitive.getName(), generator);
            }
        }
    }

    private boolean checkDimensionColName(String dimColName, String hierName) {
        String[] tables = meta.getDimTableArray();

        for (String table : tables) {
            String hierWithTableName = table + '_' + hierName;
            if (hierWithTableName.equalsIgnoreCase(dimColName)) {
                return true;
            }
        }

        return false;
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonCSVBasedSeqGenMeta) smi;
        data = (CarbonCSVBasedSeqGenData) sdi;
        return super.init(smi, sdi);
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        /**
         * Fortify Fix: FORWARD_NULL
         * Changed to ||
         * previously there was && but actully in case any one the object being null can through the nullpointer exception 
         *
         */
        if (null == smi || null == sdi) {
            return;
        }

        meta = (CarbonCSVBasedSeqGenMeta) smi;
        data = (CarbonCSVBasedSeqGenData) sdi;
        CarbonCSVBasedDimSurrogateKeyGen surKeyGen = data.getSurrogateKeyGen();

        try {
            closeConnections();
            boolean isCacheEnabled = Boolean.parseBoolean(CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED,
                            CarbonCommonConstants.CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE));
            if (null != surKeyGen) {
                surKeyGen.setDictionaryCaches(null);
                surKeyGen.setHierCache(null);
                surKeyGen.setHierCacheReverse(null);
                surKeyGen.setTimeDimCache(null);
                surKeyGen.setMax(null);
                surKeyGen.setTimDimMax(null);
                surKeyGen.close();
            }
        } catch (Exception e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
        }
        nrmlizedHierWriterMap = null;
        data.clean();
        super.dispose(smi, sdi);
        meta = null;
        data = null;
    }

    private void writeDataFileVersion() throws KettleException {
        FileWriter versionWriter = null;
        try {
            versionWriter = new FileWriter(loadFolderLoc + File.separator + ".version");
            versionWriter
                    .write(CarbonCommonConstants.DATA_VERSION + '=' + CarbonVersion.getDataVersion());
            versionWriter.flush();
        } catch (IOException e) {
            throw new KettleException("Not able to write version File", e);
        } finally {
            try {
                if (null != versionWriter) {
                    versionWriter.close();
                }
            } catch (IOException e) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
            }
        }
    }

    private void processnoDictionaryDim(int index, String dimension, ByteBuffer[] out) {

        String dimensionValue = dimension;
        ByteBuffer buffer = ByteBuffer.allocate(dimensionValue.length());
        buffer.put(dimensionValue.getBytes(Charset.forName("UTF-8")));
        buffer.rewind();
        out[index] = buffer;

    }

    /**
     * @param NoDictionaryDims
     * @param columnName
     * @return true if the dimension is high cardinality.
     */
    private boolean isDimensionNoDictionary(String[] NoDictionaryDims, String columnName) {
        for (String colName : NoDictionaryDims) {
            if (colName.equalsIgnoreCase(
                    meta.getTableName() + CarbonCommonConstants.UNDERSCORE + columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method will give the correct index where to place the high card dims
     *
     * @param NoDictionaryCols
     * @param columnName
     * @return
     */
    private int getIndexOfNoDictionaryDims(String[] NoDictionaryCols, String columnName) {
        for (int i = 0; i < NoDictionaryCols.length; i++) {
            if (NoDictionaryCols[i].equalsIgnoreCase(
                    meta.getTableName() + CarbonCommonConstants.UNDERSCORE + columnName)) {
                // if found return index of high card dims
                return i;
            }
        }

        // this case will not occur as the check is done earlier.
        return -1;
    }

    /**
     * Observer class for thread execution
     * In case of any failure we need stop all the running thread
     */
    private class ThreadStatusObserver {
        /**
         * Below method will be called if any thread fails during execution
         *
         * @param exception
         * @throws CarbonSortKeyAndGroupByException
         */
        public void notifyFailed(Throwable exception) throws RuntimeException {
            exec.shutdownNow();
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, exception);
            throw new RuntimeException(exception);
        }
    }

}

