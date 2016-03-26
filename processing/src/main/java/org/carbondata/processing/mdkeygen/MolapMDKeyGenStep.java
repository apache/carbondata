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

package org.carbondata.processing.mdkeygen;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.csvreader.checkpoint.CheckPointHanlder;
import org.carbondata.core.csvreader.checkpoint.CheckPointInterface;
import org.carbondata.core.util.*;
import org.carbondata.processing.dataprocessor.queue.impl.DataProcessorQueue;
import org.carbondata.processing.dataprocessor.queue.impl.RecordComparator;
import org.carbondata.processing.dataprocessor.record.holder.DataProcessorRecordHolder;
import org.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;
import org.carbondata.processing.util.MolapDataProcessorUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

//import org.carbondata.core.sortandgroupby.exception.MolapSortKeyAndGroupByException;

public class MolapMDKeyGenStep extends BaseStep implements StepInterface {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapMDKeyGenStep.class.getName());
    /**
     * default number of cores
     */
    private static final short DEFAULT_NUMBER_CORES = 2;

    /**
     * will be used to get the lock on get row method
     */
    private final Object getRowLock = new Object();

    /**
     * will be used to get the lock on put row method
     */
    private final Object putRowLock = new Object();

    /**
     * writeMsrMetaDataFileLock
     */
    private final Object writeMsrMetaDataFileLock = new Object();
    /**
     * maxMinLock
     */
    private final Object maxMinLock = new Object();
    /**
     * decimalPointers
     */
    private final byte decimalPointers = Byte.parseByte(MolapProperties.getInstance()
            .getProperty(MolapCommonConstants.MOLAP_DECIMAL_POINTERS,
                    MolapCommonConstants.MOLAP_DECIMAL_POINTERS_DEFAULT));
    /**
     * molap mdkey generator step data class
     */

    private MolapMDKeyGenStepData data;
    /**
     * molap mdkey generator step meta
     */
    private MolapMDKeyGenStepMeta meta;
    /**
     * dimension length
     */
    private int dimensionLength;
    /**
     * number of cores
     */
    private int numberOfCores;
    /**
     * table name
     */
    private String tableName;
    /**
     * measure meta data file location
     */
    private String measureMetaDataFileLocation;
    /**
     * max value for each measure
     */
    private double[] maxValue;
    /**
     * min value for each measure
     */
    private double[] minValue;
    /**
     * decimal length of each measure
     */
    private int[] decimalLength;
    /**
     * uniqueValue
     */
    private double[] uniqueValue;
    /**
     * aggType
     */
    private char[] type;
    /**
     * isTerminated
     */
    private boolean isTerminated;
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;
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
     * threadStatusObserver
     */
    private ThreadStatusObserver threadStatusObserver;
    /**
     * checkpoint
     */
    private CheckPointInterface checkPoint;

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

    /**
     * executorService
     */
    private ExecutorService putRowExecutorService;

    private DataProcessorQueue localDataProcessorQueue;

    private int counterToFlush;

    private int processed;

    private BlockingQueue<DataProcessorRecordHolder> dataQueue;

    private ExecutorService exec;

    private int measureCount;

    /**
     * MolapMDKeyGenStep
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public MolapMDKeyGenStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Perform the equivalent of processing one row. Typically this means
     * reading a row from input (getRow()) and passing a row to output
     * (putRow)).
     *
     * @param smi The steps metadata to work with
     * @param sdi The steps temporary working data to work with (database
     *            connections, result sets, caches, temporary variables, etc.)
     * @return false if no more rows can be processed or an error occurred.
     * @throws KettleException
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        try {
            meta = (MolapMDKeyGenStepMeta) smi;
            data = (MolapMDKeyGenStepData) sdi;

            if (!meta.isAutoAggRequest()) {
                checkPoint = CheckPointHanlder
                        .getCheckpoint(new File(getTrans().getFilename()).getName());
            } else {
                checkPoint = CheckPointHanlder.getDummyCheckPoint();
            }
            // get the row from previous step
            Object[] r = getRow();

            // if row is null then we can assume there wont be any incoming row,
            // so
            // finish this step
            if (r == null) {

                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + this.tableName);
                String logMessage =
                        "MolapSortKeyStep: Read: " + readCounter + ": Write: " + writeCounter;
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
                setOutputDone();
                return false;
            }

            if (first) {
                first = false;

                threadStatusObserver = new ThreadStatusObserver();

                dataQueue = new PriorityBlockingQueue<DataProcessorRecordHolder>(initialCapacity,
                        new RecordComparator());

                if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isAutoAggRequest()) {
                    updateCounter();
                    MolapProperties instance = MolapProperties.getInstance();

                    checkPointSize = Integer.parseInt(
                            instance.getProperty(MolapCommonConstants.SORT_SIZE,
                                    MolapCommonConstants.SORT_SIZE_DEFAULT_VAL)) / Integer
                            .parseInt(meta.getNumberOfCores());

                    this.threshold = Integer.parseInt(instance.getProperty(
                            MolapCommonConstants.MOLAP_CHECKPOINT_QUEUE_THRESHOLD,
                            MolapCommonConstants.MOLAP_CHECKPOINT_QUEUE_THRESHOLD_DEFAULT_VAL));
                    this.checkPointSize = Integer.parseInt(
                            instance.getProperty(MolapCommonConstants.MOLAP_CHECKPOINT_CHUNK_SIZE,
                                    MolapCommonConstants.MOLAP_CHECKPOINT_CHUNK_SIZE_DEFAULT_VAL));
                    this.initialCapacity = Integer.parseInt(instance.getProperty(
                            MolapCommonConstants.MOLAP_CHECKPOINT_QUEUE_INITIAL_CAPACITY,
                            MolapCommonConstants.MOLAP_CHECKPOINT_QUEUE_INITIAL_CAPACITY_DEFAULT_VAL));
                    this.toCopy = Integer.parseInt(instance.getProperty(
                            MolapCommonConstants.MOLAP_CHECKPOINT_TOCOPY_FROM_QUEUE,
                            MolapCommonConstants.MOLAP_CHECKPOINT_TOCOPY_FROM_QUEUE_DEFAULT_VAL));
                }

                data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
                meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
                int[] dimLens = MolapDataProcessorUtil.getDimLens(meta.getAggregateLevels());
                setStepConfiguration(dimLens);
                setStepOutputInterface(dimLens);
            }
            readCounter++;
            if (checkAllRowValuesAreNull(r)) {
                putRow(data.outputRowMeta, new Object[data.outputRowMeta.size()]);

                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + this.tableName);
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Form Previous Step was null");
                String logMessage =
                        "MolapSortKeyStep: Read: " + readCounter + ": Write: " + writeCounter;
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
                return true;
            }
            // create the measure model while will hold max,min and decimal
            // length of all the measures
            // process first row
            Object[] outputRow = new Object[data.outputRowMeta.size()];
            double[] msrs = new double[this.measureCount];
            process(r, outputRow, msrs);
            calculateMaxMinUnique(msrs);
            // add model in model list
            // send the transformed row to next step for processing
            writeCounter++;
            putRow(data.outputRowMeta, outputRow);

            localDataProcessorQueue = new DataProcessorQueue(initialCapacity);

            //Start the reading records process.
            startReadingProcess();
                FileData fileData = (FileData) fileManager.get(0);
                String storePath = fileData.getStorePath();
                String inProgFileName = fileData.getFileName();
                String changedFileName = measureMetaDataFileLocation
                        .substring(0, measureMetaDataFileLocation.lastIndexOf('.'));
                File currentFile = new File(storePath + File.separator + inProgFileName);
                File destFile = new File(changedFileName);
                currentFile.renameTo(destFile);
                fileData.setName(changedFileName);
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + this.tableName);
            String logMessage =
                    "Finished Molap Mdkey Generation Step: Read: " + readCounter + ": Write: "
                            + writeCounter;
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
            setOutputDone();
        } catch (Exception ex) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
            throw new RuntimeException(ex);
        }
        return false;
    }

    /**
     * @throws KettleException
     * @throws InterruptedException
     */
    private void startReadingProcess() throws KettleException, InterruptedException {
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isAutoAggRequest()) {
            this.putRowExecutorService = Executors.newFixedThreadPool(1);

            startProcesses();

            if (resultArray != null) {
                int futureTaskSize = resultArray.length;
                boolean complete = false;
                while (!complete) {
                    complete = true;
                    for (int i = 0; i < futureTaskSize; i++) {

                        if (!resultArray[i].isDone()) {
                            complete = false;
                        }

                    }
                    Thread.sleep(200);
                }
            }

            if (!localDataProcessorQueue.isEmpty()) {
                while (!localDataProcessorQueue.isEmpty()) {
                    dataQueue.offer(localDataProcessorQueue.poll());
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
            startProcesses();
        }
    }

    private void updateCounter() {
        MolapProperties instance = MolapProperties.getInstance();

        String rowSetSizeStr = instance.getProperty(MolapCommonConstants.GRAPH_ROWSET_SIZE,
                MolapCommonConstants.GRAPH_ROWSET_SIZE_DEFAULT);
        int rowSetSize = Integer.parseInt(rowSetSizeStr);

        String sortSizeStr = instance.getProperty(MolapCommonConstants.SORT_SIZE,
                MolapCommonConstants.SORT_SIZE_DEFAULT_VAL);
        int sortSize = Integer.parseInt(sortSizeStr);

        if (sortSize > rowSetSize) {
            counterToFlush = rowSetSize;
        } else {
            counterToFlush = sortSize;
        }
    }

    private void startProcesses() throws KettleException {
        // create the thread poll
        exec = Executors.newFixedThreadPool(numberOfCores);
        List<Future<Void>> results =
                new ArrayList<Future<Void>>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        // submit process
        for (int i = 0; i < numberOfCores; i++) {
            results.add(exec.submit(new DoProcess()));
        }

        resultArray = results.toArray(new Future[results.size()]);
        boolean completed = false;
        try {
            while (!completed) {
                completed = true;
                for (int i = 0; i < resultArray.length; i++) {
                    if (!resultArray[i].isDone()) {
                        completed = false;

                    }

                }
                if (isTerminated) {
                    exec.shutdownNow();
                    throw new KettleException("Interrupted due to failing of other threads");
                }
                Thread.sleep(100);

            }
        } catch (InterruptedException e) {
            throw new KettleException("Thread InterruptedException", e);
        }
        exec.shutdown();
    }

    /**
     * @param r
     * @return
     */
    private boolean checkAllRowValuesAreNull(Object[] r) {
        for (int i = 0; i < r.length; i++) {
            if (null != r[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method will be used to get and update the step properties which will
     * required to run this step
     */
    private void setStepConfiguration(int[] dimLens) {
        data.generator = KeyGeneratorFactory.getKeyGenerator(dimLens);
        this.dimensionLength = dimLens.length;
        this.measureCount = meta.getMeasureCount();

        try {
            numberOfCores = Integer.parseInt(meta.getNumberOfCores());
        } catch (NumberFormatException e) {
            numberOfCores = DEFAULT_NUMBER_CORES;
        }
        this.tableName = meta.getTableName();
        MolapProperties instance = MolapProperties.getInstance();
        String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
        String baseStorelocation = instance.getProperty(tempLocationKey,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL) + File.separator + meta
                .getSchemaName() + File.separator + meta.getCubeName();

        int restructFolderNumber = meta.getCurrentRestructNumber()/*MolapUtil.checkAndReturnNextRestructFolderNumber(baseStorelocation,"RS_")*/;

        baseStorelocation =
                baseStorelocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + restructFolderNumber + File.separator + this.tableName;

        int counter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(baseStorelocation);
        // This check is just to get the absolute path because from the property file Relative path
        // will come and sometimes FileOutPutstream was not able to Create the file.
        File file = new File(baseStorelocation);
        String storeLocation =
                file.getAbsolutePath() + File.separator + MolapCommonConstants.LOAD_FOLDER
                        + counter;

        fileManager = new LoadFolderData();
        fileManager.setName(MolapCommonConstants.LOAD_FOLDER + counter
                + MolapCommonConstants.FILE_INPROGRESS_STATUS);

        storeLocation = storeLocation + MolapCommonConstants.FILE_INPROGRESS_STATUS;

        String metaDataFileName = MolapCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;

        this.measureMetaDataFileLocation = storeLocation + metaDataFileName;

        if (!(new File(storeLocation).exists())) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Load Folder Not Present for writing measure metadata  : " + storeLocation);
            return;

        }

        FileData fileData = new FileData(metaDataFileName, storeLocation);
        fileManager.add(fileData);

        maxValue = new double[measureCount];
        minValue = new double[measureCount];
        decimalLength = new int[measureCount];
        uniqueValue = new double[measureCount];
        type = new char[measureCount];
        for (int i = 0; i < maxValue.length; i++) {
            maxValue[i] = -Double.MAX_VALUE;
        }
        for (int i = 0; i < minValue.length; i++) {
            minValue[i] = Double.MAX_VALUE;
        }

        for (int i = 0; i < decimalLength.length; i++) {
            decimalLength[i] = 0;
        }

        Arrays.fill(type, 'n');
        //If check point is enabled then we need to check if it was failed and
        // we are resuming it then we have to initialize the max min , decimal
        // and unique value with the previously saved values.
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isAutoAggRequest()) {
            String measureMetaDataTempFile = instance.getProperty(tempLocationKey,
                    MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL) + File.separator + meta
                    .getSchemaName() + File.separator + meta.getCubeName() + File.separator
                    + MolapCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + this.tableName
                    + File.separator + MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                    + this.tableName + MolapCommonConstants.MEASUREMETADATA_FILE_EXT;

            if (new File(measureMetaDataTempFile).exists()) {
                MeasureMetaDataModel measureMetadataModel = ValueCompressionUtil
                        .readMeasureMetaDataFile(measureMetaDataTempFile, measureCount);

                System.arraycopy(measureMetadataModel.getMaxValue(), 0, maxValue, 0, measureCount);
                System.arraycopy(measureMetadataModel.getMinValue(), 0, minValue, 0, measureCount);
                System.arraycopy(measureMetadataModel.getDecimal(), 0, decimalLength, 0,
                        measureCount);
                System.arraycopy(measureMetadataModel.getUniqueValue(), 0, uniqueValue, 0,
                        measureCount);
                System.arraycopy(measureMetadataModel.getType(), 0, type, 0, measureCount);

            }

        }

        logCounter = Integer.parseInt(MolapCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER);
    }

    /**
     * This method will be used for setting the output interface.
     * Output interface is how this step will process the row to next step
     *
     * @param dimLens number of dimensions
     */
    private void setStepOutputInterface(int[] dimLens) {
        ValueMetaInterface[] out =
                new ValueMetaInterface[data.outputRowMeta.size() - dimLens.length + 1];
        int l = 0;
        int measureSize = dimLens.length + measureCount;
        for (int i = dimLens.length; i < measureSize; i++) {
            out[l] = data.outputRowMeta.getValueMeta(i);
            l++;
        }
        if (meta.isAutoAggRequest() && meta.isFactMdKeyInInputRow()) {
            out[out.length - 2 - checkPoint.getCheckPointInfoFieldCount()] =
                    new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
                            ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
            out[out.length - 2 - checkPoint.getCheckPointInfoFieldCount()].setStorageMetadata(
                    new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
                            ValueMetaInterface.STORAGE_TYPE_NORMAL));
            out[out.length - 2 - checkPoint.getCheckPointInfoFieldCount()].setLength(256);
            out[out.length - 2 - checkPoint.getCheckPointInfoFieldCount()]
                    .setStringEncoding(MolapCommonConstants.BYTE_ENCODING);
            out[out.length - 2 - checkPoint.getCheckPointInfoFieldCount()].getStorageMetadata()
                    .setStringEncoding(MolapCommonConstants.BYTE_ENCODING);

            out[out.length - 1] = data.outputRowMeta.getValueMeta(data.outputRowMeta.size() - 1);
        } else {
            out[out.length - 1 - checkPoint.getCheckPointInfoFieldCount()] =
                    new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
                            ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
            out[out.length - 1 - checkPoint.getCheckPointInfoFieldCount()].setStorageMetadata(
                    new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
                            ValueMetaInterface.STORAGE_TYPE_NORMAL));
            out[out.length - 1 - checkPoint.getCheckPointInfoFieldCount()].setLength(256);
            out[out.length - 1 - checkPoint.getCheckPointInfoFieldCount()]
                    .setStringEncoding(MolapCommonConstants.BYTE_ENCODING);
            out[out.length - 1 - checkPoint.getCheckPointInfoFieldCount()].getStorageMetadata()
                    .setStringEncoding(MolapCommonConstants.BYTE_ENCODING);
        }

        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isAutoAggRequest()) {

            out[out.length - 2] = data.outputRowMeta.getValueMeta(data.outputRowMeta.size() - 2);
            out[out.length - 1] = data.outputRowMeta.getValueMeta(data.outputRowMeta.size() - 1);

        }
        data.outputRowMeta.setValueMetaList(Arrays.asList(out));
    }

    /**
     * This method will be used to get the row from previous step and then it
     * will generate the mdkey and then send the mdkey to next step
     *
     * @throws KettleException
     */
    private void doProcess() throws KettleException {
        double[] msrs = new double[this.measureCount];
        while (true) {
            Object[] r = null;
            synchronized (getRowLock) {
                readCounter++;
                if (readCounter % logCounter == 0) {
                    String logMessage =
                            "Molap Mdkey Generation Step: Record Read for table : " + this.tableName
                                    + " is : " + readCounter;
                    LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            logMessage);
                }
                r = getRow();
            }
            // no more input to be expected...
            if (r == null) {
                readCounter--;
                break;
            }
            Object[] outputRow = new Object[data.outputRowMeta.size()];
            process(r, outputRow, msrs);
            synchronized (putRowLock) {
                writeCounter++;
                putRow(data.outputRowMeta, outputRow);
            }
            calculateMaxMinUnique(msrs);

        }
    }

    private void doProcessWithCheckPoint() throws KettleException {
        try {

            double[] msrs = new double[this.measureCount];
            while (true) {

                Object[] r = null;
                DataProcessorRecordHolder oriRecords = null;
                synchronized (getRowLock) {
                    oriRecords = new DataProcessorRecordHolder(checkPointSize, seqNumber++);
                    for (int i = 0; i < checkPointSize; i++) {
                        readCounter++;
                        r = getRow();
                        if (r == null) {
                            break;
                        }

                        oriRecords.addRow(r);
                    }
                }

                processRows(oriRecords, msrs);

                // no more input to be expected...
                if (r == null) {
                    readCounter--;
                    break;
                }

                if (localDataProcessorQueue.size() > threshold) {
                    synchronized (putRowLock) {
                        if (localDataProcessorQueue.size() > threshold) {
                            for (int i = 0; i < toCopy; i++) {
                                dataQueue.offer(localDataProcessorQueue.poll());
                            }

                            putRowInSeqence();
                        }

                    }
                }

            }

        } catch (Throwable t) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Error While Processing the rows in the doprocess");
            throw new KettleException(t);
        }
    }

    /**
     * @param oriRecords
     * @param msrs
     * @throws KettleException
     */
    private void processRows(DataProcessorRecordHolder oriRecords, double[] msrs)
            throws KettleException {
        Object[][] originalRow = oriRecords.getOriginalRow();

        for (int i = 0; i < checkPointSize; i++) {
            if (null == originalRow[i]) {
                break;
            }
            Object[] outputRow = new Object[data.outputRowMeta.size()];
            process(originalRow[i], outputRow, msrs);

            originalRow[i] = null;

            calculateMaxMinUnique(msrs);

            if (processed % counterToFlush == 0) {
                // Write the measureMetadata details into the File, as it will
                // be required if the
                // data loading is failed as we will not start reading fact csv
                // from beginning.
                synchronized (writeMsrMetaDataFileLock) {
                    writeMeasureMetadataFileToTempLocation();
                }
            }
            ++processed;
            oriRecords.addProcessedRows(outputRow);
        }

        synchronized (putRowLock) {
            while (!localDataProcessorQueue.offer(oriRecords)) {
                putRowInSeqence();
            }
        }
    }

    private void putRowInSeqence() throws KettleStepException {
        putRowFuture = putRowExecutorService.submit(new Callable<Void>() {

            @Override public Void call() throws Exception {
                try {

                    while (!dataQueue.isEmpty()) {

                        DataProcessorRecordHolder records = dataQueue.poll();

                        if (null == records) {
                            return null;
                        }

                        Object[][] processedRow = records.getProcessedRow();
                        for (int i = 0; i < checkPointSize; i++) {
                            if (processedRow[i] == null) {
                                break;
                            }
                            writeCounter++;
                            putRow(data.outputRowMeta, processedRow[i]);
                            processedRow[i] = null;
                        }

                    }
                } catch (Throwable t) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Not Able to process records to Next step.");
                    throw new KettleException(t);
                }
                return null;
            }
        });
    }

    private void writeMeasureMetadataFileToTempLocation() {
        MolapProperties molapPropInstance = MolapProperties.getInstance();
        String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
        String sortTempFileLoc = molapPropInstance
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
                + File.separator + meta.getSchemaName() + File.separator + meta.getCubeName()
                + File.separator + MolapCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator
                + this.tableName;

        String metaDataFileName = MolapCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;

        String measuremetaDataFilepath = sortTempFileLoc + File.separator + metaDataFileName;

        // first check if the metadata file already present the take backup and
        // rename inprofress file to
        // measure metadata and delete the bak file. else rename bak bak to
        // original file.

        File inprogress = new File(measuremetaDataFilepath);
        String inprogressFileName = inprogress.getName();
        String originalFileName =
                inprogressFileName.substring(0, inprogressFileName.lastIndexOf('.'));

        File orgFile = new File(sortTempFileLoc + File.separator + originalFileName);
        File bakupFile = new File(sortTempFileLoc + File.separator + originalFileName + ".bak");

        if (orgFile.exists()) {
            if (!orgFile.renameTo(bakupFile)) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "not able to rename original measure metadata file to bak fiel");
            }

        }

        if (!inprogress.renameTo(orgFile)) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Not able to rename inprogress File to original file in the sort temp folder.");
        } else {
            //delete the bak file.
            if (bakupFile.exists()) {
                if (!bakupFile.delete()) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Not able to delete backup file " + bakupFile.getName());
                }
            }

        }

    }

    /**
     * This method will be used to get the row from previous step and then it
     * will generate the mdkey and then send the mdkey to next step
     *
     * @param row input row
     * @throws KettleException
     */
    private void process(Object[] row, Object[] outputRow, double[] measures)
            throws KettleException {
        int[] keys = new int[this.dimensionLength];
        int l = 0;
        // copy all the measures to output row;
        int measureSize = this.dimensionLength + measureCount;
        for (int i = this.dimensionLength;
             i < measureSize; i++) {//CHECKSTYLE:OFF    Approval No:Approval-377
            outputRow[l] = row[i];//CHECKSTYLE:ON
            if (null == row[i]) {
                measures[l] = 0;
            } else {
                measures[l] = (Double) row[i];
            }
            l++;
        }
        // copy all the dimension to keys Array. This key array will be used to
        // generate id
        for (int i = 0; i < this.dimensionLength; i++) {
            Object key = row[i];
            keys[i] = (Integer) key;
        }
        try {
            // generate byte array from id.
            byte[] k = data.generator.generateKey(keys);
            if (meta.isFactMdKeyInInputRow() && meta.isAutoAggRequest()) {
                outputRow[outputRow.length - 2] = k;
                outputRow[outputRow.length - 1] = row[row.length - 1];
            } else {
                outputRow[outputRow.length - 1 - checkPoint.getCheckPointInfoFieldCount()] = k;
            }
        } catch (KeyGenException e) {
            throw new KettleException("Unbale to generate the mdkey", e);
        }
        checkPoint.updateInfoFields(row, outputRow);
    }

    /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param smi The metadata to work with
     * @param sdi The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (MolapMDKeyGenStepMeta) smi;
        data = (MolapMDKeyGenStepData) sdi;

        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (MolapMDKeyGenStepMeta) smi;
        data = (MolapMDKeyGenStepData) sdi;
        super.dispose(smi, sdi);
    }

    /**
     * This method will be used to update the max value for each measure
     *
     * @param currentMeasures
     */
    private void calculateMaxMinUnique(double[] currentMeasures) {

        synchronized (maxMinLock) {
            for (int i = 0; i < currentMeasures.length; i++) {
                double value = currentMeasures[i];
                maxValue[i] = (maxValue[i] > value ? maxValue[i] : value);
                minValue[i] = (minValue[i] < value ? minValue[i] : value);
                uniqueValue[i] = minValue[i] - 1;
                int num = (value % 1 == 0) ? 0 : decimalPointers;
                decimalLength[i] = (decimalLength[i] > num ? decimalLength[i] : num);
            }
        }
    }

    private class DoProcess implements Callable<Void> {
        @Override public Void call() throws Exception {
            try {
                if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isAutoAggRequest()) {
                    doProcessWithCheckPoint();

                } else {
                    doProcess();
                }
            } catch (Throwable e) {
                threadStatusObserver.notifyFailed(e);
            }
            return null;
        }

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
         */
        public void notifyFailed(Throwable exception) throws RuntimeException {
            exec.shutdownNow();
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, exception);
            throw new RuntimeException(exception);
        }
    }
}
