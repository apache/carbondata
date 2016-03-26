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

package org.carbondata.processing.sortandgroupby.sortKey;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.csvreader.checkpoint.CheckPointHanlder;
import org.carbondata.core.csvreader.checkpoint.CheckPointInterface;
import org.carbondata.core.csvreader.checkpoint.exception.CheckPointException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.util.MolapUtilException;
import org.carbondata.processing.groupby.exception.MolapGroupByException;
import org.carbondata.processing.schema.metadata.SortObserver;
import org.carbondata.processing.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;
import org.carbondata.processing.util.MolapDataProcessorUtil;
import org.carbondata.query.aggregator.MeasureAggregator;

public class MolapSortKeys {

    /**
     * LOGGER
     */
    private static final LogService SORTKEYLOGGER =
            LogServiceFactory.getLogService(MolapSortKeys.class.getName());

    /**
     * CONSTANT_SIZE_TEN
     */
    private static final int CONSTANT_SIZE_TEN = 10;
    /**
     * lockObject
     */
    private final Object lockObject = new Object();
    /**
     * tempFileLocation
     */
    private String tempFileLocation;
    /**
     * mdKeyIndex
     */
    private int mdKeyIndex;
    /**
     * entryCount
     */
    private int entryCount;
    /**
     * tableName
     */
    private String tableName;
    /**
     * sortBufferSize
     */
    private int sortBufferSize;
    /**
     * record holder array
     */
    private Object[][] recordHolderList;
    /**
     * measure count
     */
    private int measureCount;
    /**
     * mdKeyLenght
     */
    private int mdKeyLength;
    /**
     * fileBufferSize
     */
    private int fileBufferSize;
    /**
     * numberOfIntermediateFileToBeMerged
     */
    private int numberOfIntermediateFileToBeMerged;
    /**
     * executorService
     */
    private ExecutorService executorService;
    /**
     * executorService
     */
    private ExecutorService writerExecutorService;
    /**
     * fileWriteBufferSize
     */
    private int fileWriteBufferSize;
    /**
     * procFiles
     */
    private List<File> procFiles;
    /**
     * checkpoint
     */
    private CheckPointInterface checkpoint;

    /**
     * map for maintaining the map
     */
    private Map<String, Long> checkPointMap;

    /**
     * observer
     */
    private SortObserver observer;

    /**
     * threadStatusObserver
     */
    private ThreadStatusObserver threadStatusObserver;

    /**
     * isAutoAggRequest
     */
    private boolean isAutoAggRequest;

    /**
     * isFactMdkeyInInputRow
     */
    private boolean isFactMdkeyInInputRow;

    /**
     * factMdkeyLength
     */
    private int factMdkeyLength;

    /**
     * sortTempFileNoOFRecordsInCompression
     */
    private int sortTempFileNoOFRecordsInCompression;

    /**
     * isSortTempFileCompressionEnabled
     */
    private boolean isSortTempFileCompressionEnabled;

    /**
     * aggregators
     */
    private String[] aggregators;

    /**
     * aggregatorClass
     */
    private String[] aggregatorClass;

    /**
     * factKetGenerator
     */
    private KeyGenerator factKetGenerator;

    /**
     * type
     */
    private char[] type;

    /**
     * prefetch
     */
    private boolean prefetch;

    /**
     * bufferSize
     */
    private int bufferSize;

    /**
     * isGroupByInSort
     */
    private boolean isGroupByInSort;

    /**
     * isUpdateMemberRequest
     */
    private boolean isUpdateMemberRequest;

    private MolapSortKeyHashbasedAggregator hashedBasedAgg;

    private int highCardinalityCount;

    private Object[] mergedMinValue;

    public MolapSortKeys(String tabelName, int measureCount, int mdkeyIndex, int mdkeylength,
            CheckPointInterface checkpoint, SortObserver observer, boolean autoAggRequest,
            boolean isFactMdkeyInInputRow, int factMdkeyLength, String[] aggregators,
            String[] aggregatorClass, int[] factDims, String schemaName, String cubeName,
            boolean isUpdateMemberRequest, int highCardinalityCount, char[] type) {
        // set table name
        this.tableName = tabelName;
        // set measure count
        this.measureCount = measureCount;

        this.highCardinalityCount = highCardinalityCount;
        // set mdkey index
        this.mdKeyIndex = mdkeyIndex;
        // set mdkey length
        this.mdKeyLength = mdkeylength;
        // processed file list
        this.procFiles = new ArrayList<File>(CONSTANT_SIZE_TEN);
        // check point
        this.checkpoint = checkpoint;
        // observer for main sorting
        this.observer = observer;
        // observer of writing file in thread
        this.threadStatusObserver = new ThreadStatusObserver();
        this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
        this.isAutoAggRequest = autoAggRequest;
        this.factMdkeyLength = factMdkeyLength;
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !isAutoAggRequest) {
            checkPointMap = new HashMap<String, Long>(1);
        }

        this.aggregators = aggregators;
        this.type = type;
        this.aggregatorClass = aggregatorClass;
        this.factKetGenerator = KeyGeneratorFactory.getKeyGenerator(factDims);
        this.isUpdateMemberRequest = isUpdateMemberRequest;
        if (isFactMdkeyInInputRow && isUpdateMemberRequest) {
            this.isFactMdkeyInInputRow = false;
        }
    }

    /**
     * This method will check of sort resume is required or not in case of check point
     *
     * @param tableName
     * @return SortingResumeRequired
     */
    public static boolean isSortingResumeRequired(String schemaName, String cubeName,
            final String tableName, int currentRestructNumber) {
        // get the base location
        String tempLocationKey = schemaName + '_' + cubeName;
        String baseLocation = MolapProperties.getInstance()
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        // get the temp file location
        String schemaCubeName = schemaName + File.separator + cubeName;
        baseLocation = baseLocation + File.separator + schemaCubeName + File.separator
                + MolapCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + tableName;
        File file = new File(baseLocation);
        File[] tempFiles = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                String name = pathname.getName();
                return name.startsWith(tableName) && (
                        name.endsWith(MolapCommonConstants.SORT_TEMP_FILE_EXT) || name
                                .endsWith(MolapCommonConstants.MERGERD_EXTENSION));
            }
        });
        if (null == tempFiles || tempFiles.length < 1) {
            return false;
        }

        //Delete the fact files created before server was killed/stopped
        return deleteFactFiles(schemaName, cubeName, tableName, currentRestructNumber);

    }

    private static boolean deleteFactFiles(String schemaName, String cubeName,
            final String tableName, int currentRestructNumber) {
        // get the base location
        String tempLocationKey = schemaName + '_' + cubeName;
        String baseLocation = MolapProperties.getInstance()
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        // get the temp file location
        String schemaCubeName = schemaName + File.separator + cubeName;
        baseLocation = baseLocation + File.separator + schemaCubeName;

        int restructFolderNumber = currentRestructNumber/*MolapUtil.checkAndReturnNextRestructFolderNumber(baseLocation,"RS_")*/;

        baseLocation = baseLocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                + restructFolderNumber + File.separator + tableName;

        int counter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(baseLocation);
        // This check is just to get the absolute path because from the property file Relative path
        // will come and sometimes FileOutPutstream was not able to Create the file.
        File file = new File(baseLocation);
        String storeLocation =
                file.getAbsolutePath() + File.separator + MolapCommonConstants.LOAD_FOLDER
                        + counter;

        storeLocation = storeLocation + MolapCommonConstants.FILE_INPROGRESS_STATUS;

        File loadFolder = new File(storeLocation);
        File[] factFiles = loadFolder.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                if (pathname.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT) || pathname
                        .getName().endsWith(MolapCommonConstants.FACT_FILE_EXT
                                + MolapCommonConstants.FILE_INPROGRESS_STATUS)) {
                    return true;
                }
                return false;
            }
        });

        //If Directory does not exist then list file will return null
        // in that case no need to return without deleting the files
        // as files will not be present.
        if (factFiles == null) {
            return false;
        }

        try {
            MolapUtil.deleteFiles(factFiles);
        } catch (MolapUtilException e) {
            SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Not Able to delete fact files while resuming the data loading");
        }

        return true;
    }

    /**
     * This method will be used to initialize
     */
    public void initialize(String schemaName, String cubeName, int currentRestructNumber)
            throws MolapSortKeyAndGroupByException {
        MolapProperties molapProperties = MolapProperties.getInstance();
        setSortConfiguration(molapProperties);
        // create holde list which will hold incoming rows
        // size of list will be sort buffer size + 1 to avoid creation of new
        // array in list array
        this.recordHolderList = new Object[this.sortBufferSize][];
        updateSortTempFileLocation(schemaName, cubeName, molapProperties);
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(isAutoAggRequest
                || isUpdateMemberRequest)) {
            // else resume the sorting
            resumeSortingForCheckPoint();
        } else {
            // if check point is not enabled then delete if any older file exists in sort temp folder
            deleteSortLocationIfExists();
            // create new sort temp directory
            if (!new File(this.tempFileLocation).mkdirs()) {
                SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Sort Temp Location Already Exists");
            }
        }
        this.writerExecutorService = Executors.newFixedThreadPool(3);
        this.executorService = Executors.newFixedThreadPool(10);
        this.isSortTempFileCompressionEnabled = Boolean.parseBoolean(molapProperties
                .getProperty(MolapCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
                        MolapCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));

        this.fileWriteBufferSize = Integer.parseInt(molapProperties
                .getProperty(MolapCommonConstants.MOLAP_SORT_FILE_WRITE_BUFFER_SIZE,
                        MolapCommonConstants.MOLAP_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE));

        try {
            this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(molapProperties
                    .getProperty(MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
                            MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
            if (this.sortTempFileNoOFRecordsInCompression < 1) {
                SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Invalid value for: "
                                + MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                                + ": Only Positive Integer value(greater than zero) is allowed.Default value will be used");

                this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                        MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
            }
        } catch (NumberFormatException e) {
            SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Invalid value for: "
                            + MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                            + ": Only Positive Integer value(greater than zero) is allowed.Default value will be used");
            this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                    MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
        }
        if (isSortTempFileCompressionEnabled) {
            SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Compression will be used for writing the sort temp File");
        }

        prefetch = MolapCommonConstants.MOLAP_PREFETCH_IN_MERGE_VALUE;
        bufferSize = MolapCommonConstants.MOLAP_PREFETCH_BUFFERSIZE;
        isGroupByInSort = false;

        boolean useHashBasedAggWhileSorting = Boolean.parseBoolean(molapProperties
                .getProperty(MolapCommonConstants.MOLAP_USE_HASHBASED_AGG_INSORT,
                        MolapCommonConstants.MOLAP_USE_HASHBASED_AGG_INSORT_DEFAULTVALUE));
        checkGroupByInSortIfValid();
        updateAggTypeForDistinctCount(isGroupByInSort);
        updateAggTypeForDistinctCount(useHashBasedAggWhileSorting);
        updateAggTypeForCustom();

        if (isAutoAggRequest && !isUpdateMemberRequest) {
            mergedMinValue = MolapDataProcessorUtil
                    .updateMergedMinValue(schemaName, cubeName, tableName, this.aggregators.length,
                            MolapCommonConstants.FILE_INPROGRESS_STATUS, currentRestructNumber);
        }
        if (isAutoAggRequest && useHashBasedAggWhileSorting && !isUpdateMemberRequest) {
            SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "************************ Hased based aggaregator is being used");
            hashedBasedAgg = new MolapSortKeyHashbasedAggregator(aggregators, aggregatorClass,
                    factKetGenerator, type, this.sortBufferSize, mergedMinValue);
        }

        boolean useXXHASH = Boolean.valueOf(
                MolapProperties.getInstance().getProperty("molap.enableXXHash", "false"));
        if (useXXHASH) {
            SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "************************ XXHASH is being used");
        }
    }

    private void updateAggTypeForCustom() {
        if (isAutoAggRequest && isUpdateMemberRequest) {
            for (int i = 0; i < type.length; i++) {
                if (aggregators[i].equals(MolapCommonConstants.CUSTOM)) {
                    type[i] = MolapCommonConstants.BYTE_VALUE_MEASURE;
                }
            }
        }
    }

    private void updateAggTypeForDistinctCount(boolean groupByInSort) {
        if (isAutoAggRequest && (groupByInSort || isUpdateMemberRequest)) {
            for (int i = 0; i < type.length; i++) {
                if (aggregators[i].equals(MolapCommonConstants.DISTINCT_COUNT)) {
                    type[i] = MolapCommonConstants.BYTE_VALUE_MEASURE;
                }
            }
        }
    }

    private void checkGroupByInSortIfValid() {
        if (isAutoAggRequest && (isGroupByInSort || isUpdateMemberRequest)) {
            for (int i = 0; i < aggregators.length; i++) {
                if (aggregators[i].equals(MolapCommonConstants.CUSTOM)) {
                    SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Group By In Sort cannot be used as custom measures are present");
                    isGroupByInSort = false;
                    break;
                }
            }
        }
    }

    /**
     * This method will be called if check point is enabled and will be used to restore the state
     */
    private void resumeSortingForCheckPoint() {
        // first delete all the file with check point extension
        File file = new File(tempFileLocation);
        // if directory not present then there were not failure in previous run
        if (!file.exists()) {
            // create new sort temp directory
            if (!file.mkdirs()) {
                SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Sort Temp Location Already Exists");
            }
            return;
        }
        deleteAllBakFile();
        // get the all the files with only ".sorttemp" extension which are not merger by intermediate merger
        File[] listFiles = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(tableName) && pathname.getAbsolutePath()
                        .endsWith(MolapCommonConstants.SORT_TEMP_FILE_EXT);
            }
        });
        if (null != listFiles) {
            this.procFiles = new ArrayList<File>(Arrays.asList(listFiles));
        } else {
            this.procFiles = new ArrayList<File>(1);
        }
    }

    /**
     * Below method will be used to delete all the files is sort temp folder with check point extension
     */
    private void deleteAllBakFile() {
        File file = new File(tempFileLocation);
        // get all the check point files
        File[] checkPointFiles = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(tableName) && pathname.getName()
                        .endsWith(MolapCommonConstants.BAK_EXT);
            }
        });
        // if check point files are present the delete
        if (null != checkPointFiles) {
            try {
                MolapUtil.deleteFiles(checkPointFiles);
            } catch (MolapUtilException e) {
                SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Problem while deleting the check point files");
            }
        }
    }

    /**
     * This method will be used to get the sort configuration
     *
     * @param instance
     */
    private void setSortConfiguration(MolapProperties instance) {
        // get sort buffer size 
        this.sortBufferSize = Integer.parseInt(instance.getProperty(MolapCommonConstants.SORT_SIZE,
                MolapCommonConstants.SORT_SIZE_DEFAULT_VAL));
        SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Sort buffer size for cube: " + this.sortBufferSize);
        // set number of intermedaite file to merge
        this.numberOfIntermediateFileToBeMerged = Integer.parseInt(
                instance.getProperty(MolapCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
                        MolapCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE));
        SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Number of intermediate files to be merged: "
                        + this.numberOfIntermediateFileToBeMerged);
        // get file buffer size 
        this.fileBufferSize = MolapDataProcessorUtil
                .getFileBufferSize(this.numberOfIntermediateFileToBeMerged,
                        MolapProperties.getInstance(), CONSTANT_SIZE_TEN);
        SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "File Buffer Size: " + this.fileBufferSize);
    }

    /**
     * This will be used to get the sort temo location
     *
     * @param instance
     */
    private void updateSortTempFileLocation(String schemaName, String cubeName,
            MolapProperties instance) {
        // get the base location
        String tempLocationKey = schemaName + '_' + cubeName;
        String baseLocation = instance.getProperty(tempLocationKey,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        // get the temp file location
        this.tempFileLocation =
                baseLocation + File.separator + schemaName + File.separator + cubeName
                        + File.separator + MolapCommonConstants.SORT_TEMP_FILE_LOCATION
                        + File.separator + this.tableName;
        SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "temp file location" + this.tempFileLocation);
    }

    /**
     * This method will be used to delete sort temp location is it is exites
     *
     * @throws MolapSortKeyAndGroupByException
     */
    public void deleteSortLocationIfExists() throws MolapSortKeyAndGroupByException {
        MolapDataProcessorUtil.deleteSortLocationIfExists(this.tempFileLocation);
    }

    /**
     * Below method will be used to start storing process This method will get
     * all the temp files present in sort temp folder then it will create the
     * record holder heap and then it will read first record from each file and
     * initialize the heap
     *
     * @throws MolapSortKeyAndGroupByException
     */
    public void startSorting() throws MolapSortKeyAndGroupByException {
        SORTKEYLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "File based sorting will be used");
        if (this.entryCount > 0) {
            Object[][] toSort = null;
            if (null != hashedBasedAgg) {
                toSort = hashedBasedAgg.getResult();
                entryCount = toSort.length;
            } else {
                toSort = new Object[entryCount][];
                System.arraycopy(recordHolderList, 0, toSort, 0, entryCount);
            }
            Arrays.sort(toSort, new MolapRowComparator(this.mdKeyIndex));
            recordHolderList = toSort;

            // create new file
            File file = new File(getFileName(
                    this.tempFileLocation + File.separator + this.tableName + System.nanoTime(),
                    MolapCommonConstants.SORT_TEMP_FILE_EXT));
            writeDataTofile(recordHolderList, this.entryCount, file);

            if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(isAutoAggRequest
                    || isUpdateMemberRequest)) {
                String destFileName = file.getAbsolutePath();
                String[] split = destFileName.split(MolapCommonConstants.BAK_EXT);
                if (!file.renameTo(new File(split[0]))) {
                    SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while renaming the checkpoint file");
                }

                try {
                    checkpoint.saveCheckPointCache(checkPointMap);
                } catch (CheckPointException e) {
                    SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
                }

            }
        }
        procFiles = null;
        this.recordHolderList = null;
        startFileBasedMerge();
    }

    /**
     * Below method will be used to start file based merge
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void startFileBasedMerge() throws MolapSortKeyAndGroupByException {
        try {
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
            writerExecutorService.shutdown();
            writerExecutorService.awaitTermination(2, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new MolapSortKeyAndGroupByException("Problem while shutdown the server ", e);
        }
    }

    /**
     * This method will be used to add new row
     *
     * @param row new row
     * @throws MolapSortKeyAndGroupByException problem while writing
     */
    public void addRow(Object[] row) throws MolapSortKeyAndGroupByException {
        // if record holder list size is equal to sort buffer size then it will
        // sort the list and then write current list data to file
        int currentSize = entryCount;
        if (null != hashedBasedAgg) {
            currentSize = hashedBasedAgg.getSize();
        }
        if (sortBufferSize == currentSize) {
            if (SORTKEYLOGGER.isDebugEnabled()) {
                SORTKEYLOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "************ Writing to temp file ********** ");
            }
            File[] fileList;
            if (procFiles.size() >= numberOfIntermediateFileToBeMerged) {
                synchronized (lockObject) {
                    fileList = procFiles.toArray(new File[procFiles.size()]);
                    this.procFiles = new ArrayList<File>(1);
                }
                SORTKEYLOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Sumitting request for intermediate merging no of files: "
                                + fileList.length);
                startIntermediateMerging(fileList);
            }
            if (null != hashedBasedAgg) {
                this.recordHolderList = hashedBasedAgg.getResult();
                this.hashedBasedAgg.reset();
            }
            // create new file
            File destFile = new File(getFileName(
                    this.tempFileLocation + File.separator + this.tableName + System.nanoTime(),
                    MolapCommonConstants.SORT_TEMP_FILE_EXT));
            Object[][] recordHolderListLocal = recordHolderList;
            // / create the new holder Array
            this.recordHolderList = new Object[this.sortBufferSize][];
            Map<String, Long> localCheckPointMap = null;
            if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(isAutoAggRequest
                    || isUpdateMemberRequest)) {
                localCheckPointMap = new HashMap<String, Long>(checkPointMap);
            }
            sortAndWriteToFile(destFile, recordHolderListLocal, sortBufferSize, mdKeyIndex,
                    localCheckPointMap);
            this.entryCount = 0;
        }
        addRecord(row);
    }

    private void addRecord(Object[] row) {
        if (null != hashedBasedAgg) {
            entryCount++;
            hashedBasedAgg.addData(row);
        } else {
            recordHolderList[entryCount++] = row;
        }
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !isAutoAggRequest) {
            updateCheckPointInfo(row);
        }
    }

    /**
     * Below method will be used to save the checkpoint details to check point map
     *
     * @param row
     */
    private void updateCheckPointInfo(Object row) {
        Object[] actualRow = (Object[]) row;
        String filename = (String) actualRow[actualRow.length - 2];
        Long newValue = (Long) actualRow[actualRow.length - 1];
        Long value = checkPointMap.get(filename);
        if (null == value || value < newValue) {
            checkPointMap.put(filename, newValue);
        } else {
            SORTKEYLOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "False means data in wrong order also coming:" + newValue);
        }
    }

    /**
     * sortAndWriteToFile to write data to temp file
     *
     * @param destFile
     * @throws MolapSortKeyAndGroupByException
     */
    private void sortAndWriteToFile(final File destFile, final Object[][] recordHolderListLocal,
            final int entryCountLocal, final int mdKeyIndexLocal,
            final Map<String, Long> checkPointMapLocal) throws MolapSortKeyAndGroupByException {
        writerExecutorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String newFileName = "";
                File finalFile = null;
                try {
                    // sort the record holder list
                    Arrays.sort(recordHolderListLocal, new MolapRowComparator(mdKeyIndexLocal));
                    // write data to file
                    writeDataTofile(recordHolderListLocal, entryCountLocal, destFile);
                    newFileName = destFile.getAbsolutePath();
                    if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(isAutoAggRequest
                            || isUpdateMemberRequest)) {
                        String destFileName = destFile.getAbsolutePath();
                        String[] split = destFileName.split(MolapCommonConstants.BAK_EXT);
                        newFileName = split[0];
                        checkpoint.saveCheckPointCache(checkPointMapLocal);
                        if (!destFile.renameTo(new File(newFileName))) {
                            SORTKEYLOGGER
                                    .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                            "Problem while renaming the checkpoint file");
                        }
                    }
                    finalFile = new File(newFileName);
                } catch (Throwable e) {
                    threadStatusObserver.notifyFailed(e);
                }
                synchronized (lockObject) {
                    procFiles.add(finalFile);
                }
                return null;
            }
        });
    }

    /**
     * Below method will be used to get the sort buffer size
     *
     * @return
     */
    public int getSortBufferSize() {
        return sortBufferSize;
    }

    /**
     * Below method will be used to start the intermediate file merging
     *
     * @param intermediateFiles
     */
    private void startIntermediateMerging(File[] intermediateFiles) {
        File file = new File(getFileName(
                this.tempFileLocation + File.separator + this.tableName + System.nanoTime(),
                MolapCommonConstants.MERGERD_EXTENSION));
        IntermediateFileMerger merger =
                new IntermediateFileMerger(intermediateFiles, fileBufferSize, measureCount,
                        mdKeyLength, file, mdKeyIndex, this.fileBufferSize,
                        CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(isAutoAggRequest
                                || isUpdateMemberRequest), this.isFactMdkeyInInputRow,
                        this.factMdkeyLength, sortTempFileNoOFRecordsInCompression,
                        isSortTempFileCompressionEnabled, type, prefetch, bufferSize,
                        this.aggregators, this.highCardinalityCount);
        executorService.submit(merger);
    }

    /**
     * Below method will return file name base of check required or not
     *
     * @param prefix
     * @param ext
     * @return file name
     */
    private String getFileName(String prefix, String ext) {
        StringBuilder fileName = new StringBuilder();
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(isAutoAggRequest
                || isUpdateMemberRequest)) {
            fileName.append(prefix);
            fileName.append(ext);
            fileName.append(MolapCommonConstants.BAK_EXT);
        } else {
            fileName.append(prefix);
            fileName.append(ext);
        }
        return fileName.toString();
    }

    /**
     * Below method will be used to write data to file
     *
     * @throws MolapSortKeyAndGroupByException problem while writing
     */
    private void writeDataTofile(Object[][] recordHolderList, int entryCountLocal, File file)
            throws MolapSortKeyAndGroupByException {
        if (isAutoAggRequest && isGroupByInSort && !isUpdateMemberRequest) {
            MolapSortKeyAggregator sortAggrgateKey =
                    new MolapSortKeyAggregator(aggregators, aggregatorClass, factKetGenerator, type,
                            mergedMinValue);
            try {
                recordHolderList = sortAggrgateKey.getAggregatedData(recordHolderList);
            } catch (MolapGroupByException ex) {
                SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex,
                        "Problem while writing the sort temp file");
                throw new MolapSortKeyAndGroupByException(ex);
            }
            entryCountLocal = recordHolderList.length;
        }
        // stream
        if (isSortTempFileCompressionEnabled || prefetch) {
            writeSortTempFile(recordHolderList, entryCountLocal, file, type);
            return;
        }
        writeData(recordHolderList, entryCountLocal, file, type);
    }

    private void writeData(Object[][] recordHolderList, int entryCountLocal, File file, char[] type)
            throws MolapSortKeyAndGroupByException {
        DataOutputStream stream = null;
        try {

            // open stream
            stream = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(file), fileWriteBufferSize));
            // increment file counter
            // write number of entries to the file
            stream.writeInt(entryCountLocal);
            Object[] row = null;
            int aggregatorIndexInRowObject = 0;
            for (int i = 0; i < entryCountLocal; i++) {
                // get row from record holder list
                row = recordHolderList[i];
                MeasureAggregator[] aggregator =
                        (MeasureAggregator[]) row[aggregatorIndexInRowObject];
                MolapDataProcessorUtil
                        .writeMeasureAggregatorsToSortTempFile(type, stream, aggregator);
                stream.writeDouble((Double) row[aggregatorIndexInRowObject + 1]);

                // write the high cardinality if present.

                int highCardIndex = this.mdKeyIndex - 1;
                if (null != row[highCardIndex]) {
                    byte[] singleHighCardArr = (byte[]) row[highCardIndex];
                    stream.write(singleHighCardArr);
                }

                // write mdkey
                stream.write((byte[]) row[this.mdKeyIndex]);
                if (isAutoAggRequest && isFactMdkeyInInputRow) {
                    stream.write((byte[]) row[row.length - 1]);
                }
            }
        } catch (IOException e) {
            throw new MolapSortKeyAndGroupByException("Problem while writing the file", e);
        } finally {
            // close streams
            MolapUtil.closeStreams(stream);
        }
    }

    private void writeSortTempFile(Object[][] recordHolderList, int entryCountLocal, File file,
            char[] type) throws MolapSortKeyAndGroupByException {
        MolapSortTempFileWriter writer = null;

        try {
            writer = getWriter();
            writer.initiaize(file, entryCountLocal);
            writer.writeSortTempFile(recordHolderList);
        } catch (MolapSortKeyAndGroupByException e) {
            SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Problem while writing the sort temp file");
            throw e;
        } finally {
            writer.finish();
        }
    }

    private MolapSortTempFileWriter getWriter() {
        MolapSortTempFileWriter writer = null;
        if (prefetch && !isSortTempFileCompressionEnabled) {
            writer = new MolapSortTempFileChunkWriter(
                    new MolapUnCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
                            isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type),
                    bufferSize);
        } else {
            writer = new MolapSortTempFileChunkWriter(
                    new MolapCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
                            isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type),
                    sortTempFileNoOFRecordsInCompression);
        }

        return writer;
    }

    /**
     * Below method will be used to delete the
     * temp files and folder
     */
    public void deleteTmpFiles() {
        try {
            deleteSortLocationIfExists();
        } catch (MolapSortKeyAndGroupByException e) {
            SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Problem while deleting the temp folder and files");
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
         * @throws MolapSortKeyAndGroupByException
         */
        public void notifyFailed(Throwable exception) throws MolapSortKeyAndGroupByException {
            writerExecutorService.shutdownNow();
            executorService.shutdownNow();
            observer.setFailed(true);
            SORTKEYLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, exception);
            throw new MolapSortKeyAndGroupByException(exception);
        }
    }
}
