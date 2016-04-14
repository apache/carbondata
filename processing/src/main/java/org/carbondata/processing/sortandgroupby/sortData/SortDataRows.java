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

package org.carbondata.processing.sortandgroupby.sortData;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.exception.CarbonDataProcessorException;
import org.carbondata.processing.schema.metadata.SortObserver;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.processing.util.RemoveDictionaryUtil;

public class SortDataRows {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(SortDataRows.class.getName());
    /**
     * lockObject
     */
    private final Object lockObject = new Object();
    /**
     * maxMinLock
     */
    private final Object maxMinLock = new Object();
    /**
     * decimalPointers
     */
    private final byte decimalPointers = Byte.parseByte(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS,
                    CarbonCommonConstants.CARBON_DECIMAL_POINTERS_DEFAULT));
    /**
     * tempFileLocation
     */
    private String tempFileLocation;
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
    private int measureColCount;
    /**
     * measure count
     */
    private int dimColCount;
    /**
     * measure count
     */
    private int complexDimColCount;
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
     * observer
     */
    private SortObserver observer;
    /**
     * threadStatusObserver
     */
    private ThreadStatusObserver threadStatusObserver;
    /**
     * sortTempFileNoOFRecordsInCompression
     */
    private int sortTempFileNoOFRecordsInCompression;
    /**
     * isSortTempFileCompressionEnabled
     */
    private boolean isSortFileCompressionEnabled;
    /**
     * prefetch
     */
    private boolean prefetch;
    /**
     * bufferSize
     */
    private int bufferSize;
    private String schemaName;
    private String cubeName;
    /**
     * max value for each measure
     */
    private Object[] maxValue;

    /**
     * min value for each measure
     */
    private Object[] minValue;

    /**
     * decimal length of each measure
     */
    private int[] decimalLength;
    /**
     * uniqueValue
     */
    private Object[] uniqueValue;

    private int currentRestructNumber;

    private String[] measureDatatype;

    private char[] aggType;

    /**
     * To know how many columns are of high cardinality.
     */
    private int noDictionaryCount;

    public SortDataRows(String tabelName, int dimColCount, int complexDimColCount,
            int measureColCount, SortObserver observer, int currentRestructNum,
            int noDictionaryCount, String[] measureDatatype) {
        // set table name
        this.tableName = tabelName;

        // set measure count
        this.measureColCount = measureColCount;

        this.dimColCount = dimColCount;

        this.noDictionaryCount = noDictionaryCount;
        this.complexDimColCount = complexDimColCount;

        // processed file list
        this.procFiles = new ArrayList<File>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

        // observer for main sorting
        this.observer = observer;

        // observer of writing file in thread
        this.threadStatusObserver = new ThreadStatusObserver();

        this.currentRestructNumber = currentRestructNum;

        this.measureDatatype = measureDatatype;
        this.aggType = new char[measureColCount];
    }

    /**
     * This method will be used to initialize
     */
    public void initialize(String schemaName, String cubeName)
            throws CarbonSortKeyAndGroupByException {
        this.schemaName = schemaName;
        this.cubeName = cubeName;

        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        setSortConfiguration(carbonProperties);

        // create holder list which will hold incoming rows
        // size of list will be sort buffer size + 1 to avoid creation of new
        // array in list array
        this.recordHolderList = new Object[this.sortBufferSize][];
        updateSortTempFileLocation(carbonProperties);

        // Delete if any older file exists in sort temp folder
        deleteSortLocationIfExists();

        // create new sort temp directory
        if (!new File(this.tempFileLocation).mkdirs()) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Sort Temp Location Already Exists");
        }

        this.executorService = Executors.newFixedThreadPool(10);
        this.writerExecutorService = Executors.newFixedThreadPool(3);
        this.fileWriteBufferSize = Integer.parseInt(carbonProperties
                .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
                        CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE));

        this.isSortFileCompressionEnabled = Boolean.parseBoolean(carbonProperties
                .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
                        CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));

        try {
            this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(carbonProperties
                    .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
                            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
            if (this.sortTempFileNoOFRecordsInCompression < 1) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Invalid value for: "
                                + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                                + ":Only Positive Integer value(greater than zero) is allowed.Default value will be used");

                this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                        CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
            }
        } catch (NumberFormatException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Invalid value for: "
                            + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                            + ":Only Positive Integer value(greater than zero) is allowed.Default value will be used");

            this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                    CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
        }

        if (isSortFileCompressionEnabled) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Compression will be used for writing the sort temp File");
        }

        prefetch = CarbonCommonConstants.CARBON_PREFETCH_IN_MERGE_VALUE;
        bufferSize = CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE;

        initAggType();
        maxValue = new Object[measureColCount];
        minValue = new Object[measureColCount];
        decimalLength = new int[measureColCount];
        uniqueValue = new Object[measureColCount];

        for (int i = 0; i < maxValue.length; i++) {
            if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                maxValue[i] = Long.MIN_VALUE;
            } else if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                maxValue[i] = -Double.MAX_VALUE;
            } else if (aggType[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                maxValue[i] = new BigDecimal(0.0);
            } else {
                maxValue[i] = 0.0;
            }
        }

        for (int i = 0; i < minValue.length; i++) {
            if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                minValue[i] = Long.MAX_VALUE;
            } else if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                minValue[i] = Double.MAX_VALUE;
            } else if (aggType[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                minValue[i] = new BigDecimal(Double.MAX_VALUE);
            } else {
                minValue[i] = 0.0;
            }
        }

        for (int i = 0; i < decimalLength.length; i++) {
            decimalLength[i] = 0;
        }
    }

    private void initAggType() {
        Arrays.fill(aggType, 'n');
        for (int i = 0; i < measureColCount; i++) {
            aggType[i] = DataTypeUtil.getAggType(measureDatatype[i]);
        }
    }

    /**
     * This method will be used to add new row
     *
     * @param row new row
     * @throws CarbonSortKeyAndGroupByException problem while writing
     */
    public void addRow(Object[] row) throws CarbonSortKeyAndGroupByException {
        // if record holder list size is equal to sort buffer size then it will
        // sort the list and then write current list data to file
        int currentSize = entryCount;

        if (sortBufferSize == currentSize) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "************ Writing to temp file ********** ");
            }

            File[] fileList;
            if (procFiles.size() >= numberOfIntermediateFileToBeMerged) {
                synchronized (lockObject) {
                    fileList = procFiles.toArray(new File[procFiles.size()]);
                    this.procFiles = new ArrayList<File>(1);
                }

                LOGGER.debug(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Sumitting request for intermediate merging no of files: "
                                + fileList.length);

                startIntermediateMerging(fileList);
            }

            // create new file
            File destFile = new File(
                    this.tempFileLocation + File.separator + this.tableName + System.nanoTime()
                            + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
            Object[][] recordHolderListLocal = recordHolderList;

            // create the new holder Array
            this.recordHolderList = new Object[this.sortBufferSize][];

            sortAndWriteToFile(destFile, recordHolderListLocal, sortBufferSize);
            this.entryCount = 0;

        }

        recordHolderList[entryCount++] = row;
    }

    /**
     * Below method will be used to start storing process This method will get
     * all the temp files present in sort temp folder then it will create the
     * record holder heap and then it will read first record from each file and
     * initialize the heap
     *
     * @throws CarbonSortKeyAndGroupByException
     */
    public void startSorting() throws CarbonSortKeyAndGroupByException {
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "File based sorting will be used");
        if (this.entryCount > 0) {
            Object[][] toSort;// = null;
            toSort = new Object[entryCount][];
            System.arraycopy(recordHolderList, 0, toSort, 0, entryCount);

            Arrays.sort(toSort, new RowComparator(this.dimColCount));
            recordHolderList = toSort;

            // create new file
            File file = new File(
                    this.tempFileLocation + File.separator + this.tableName + System.nanoTime() +
                            CarbonCommonConstants.SORT_TEMP_FILE_EXT);
            writeDataTofile(recordHolderList, this.entryCount, file);

        }

        procFiles = null;
        this.recordHolderList = null;
        startFileBasedMerge();
    }

    /**
     * sortAndWriteToFile to write data to temp file
     *
     * @param destFile
     * @throws CarbonSortKeyAndGroupByException
     */
    private void sortAndWriteToFile(final File destFile, final Object[][] recordHolderListLocal,
            final int entryCountLocal) throws CarbonSortKeyAndGroupByException {
        writerExecutorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String newFileName = "";
                File finalFile = null;
                try {
                    // sort the record holder list
                    Arrays.sort(recordHolderListLocal, new RowComparator(dimColCount));

                    // write data to file
                    writeDataTofile(recordHolderListLocal, entryCountLocal, destFile);

                    newFileName = destFile.getAbsolutePath();
                    finalFile = new File(newFileName);
                } catch (Throwable e) {
                    threadStatusObserver.notifyFailed(e);
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                            e.getMessage());
                }
                synchronized (lockObject) {
                    procFiles.add(finalFile);
                }
                return null;
            }
        });
    }

    /**
     * Below method will be used to write data to file
     *
     * @throws CarbonSortKeyAndGroupByException problem while writing
     */
    private void writeDataTofile(Object[][] recordHolderList, int entryCountLocal, File file)
            throws CarbonSortKeyAndGroupByException {
        // stream
        if (isSortFileCompressionEnabled || prefetch) {
            writeSortTempFile(recordHolderList, entryCountLocal, file);
            return;
        }
        writeData(recordHolderList, entryCountLocal, file);
    }

    private void writeSortTempFile(Object[][] recordHolderList, int entryCountLocal, File file)
            throws CarbonSortKeyAndGroupByException {
        TempSortFileWriter writer = null;

        try {
            writer = getWriter();
            writer.initiaize(file, entryCountLocal);
            writer.writeSortTempFile(recordHolderList);
        } catch (CarbonSortKeyAndGroupByException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                    "Problem while writing the sort temp file");
            throw e;
        } finally {
            writer.finish();
        }
    }

    private void writeData(Object[][] recordHolderList, int entryCountLocal, File file)
            throws CarbonSortKeyAndGroupByException {
        DataOutputStream stream = null;
        try {
            // open stream
            stream = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(file), fileWriteBufferSize));

            // write number of entries to the file
            stream.writeInt(entryCountLocal);
            Object[] row = null;
            Object[] measures = null;

            // Row level min max
            Object[] max = new Object[measureColCount];
            Object[] min = new Object[measureColCount];
            int[] decimal = new int[measureColCount];

            for (int i = 0; i < max.length; i++) {
                if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                    max[i] = Long.MIN_VALUE;
                } else if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                    max[i] = -Double.MAX_VALUE;
                } else {
                    max[i] = 0.0;
                }
            }

            for (int i = 0; i < min.length; i++) {
                if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                    min[i] = Long.MAX_VALUE;
                } else if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                    min[i] = Double.MAX_VALUE;
                } else if (aggType[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                    min[i] = new BigDecimal(Double.MAX_VALUE);
                } else {
                    min[i] = 0.0;
                }
            }

            for (int i = 0; i < decimal.length; i++) {
                decimal[i] = 0;
            }

            for (int i = 0; i < entryCountLocal; i++) {
                // get row from record holder list
                row = recordHolderList[i];
                measures = new Object[measureColCount];

                int fieldIndex = 0;

                for (int dimCount = 0; dimCount < this.dimColCount; dimCount++) {
                    stream.writeInt(RemoveDictionaryUtil.getDimension(fieldIndex++, row));
                    //                	stream.writeInt((Integer)row[fieldIndex++]);
                }

               /* for (int dimCount = 0; dimCount < this.complexDimColCount; dimCount++) {
                    int complexByteArrayLength = ((byte[]) row[fieldIndex]).length;
                    stream.writeInt(complexByteArrayLength);
                    stream.write(((byte[]) row[fieldIndex++]));
                }*/

                // if any high cardinality dims are present then write it to the file.
                if (this.noDictionaryCount > 0) {
                    stream.write(RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row));
                }

                // as measures are stored in separate array.
                fieldIndex = 0;
                for (int mesCount = 0; mesCount < this.measureColCount; mesCount++) {
                    if (null != RemoveDictionaryUtil.getMeasure(fieldIndex, row)) {
                        stream.write((byte) 1);
                        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                            Double val = (Double) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
                            stream.writeDouble(val);
                            measures[mesCount] = val;
                        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
                            Long val = (Long) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
                            stream.writeLong(val);
                            measures[mesCount] = val;
                        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                            BigDecimal val =
                                    (BigDecimal) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
                            byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
                            stream.writeInt(bigDecimalInBytes.length);
                            stream.write(bigDecimalInBytes);
                            measures[mesCount] = val;
                        }
                    } else {
                        stream.write((byte) 0);
                        if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
                            measures[mesCount] = 0L;
                        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                            measures[mesCount] = new BigDecimal(0.0);
                        } else {
                            measures[mesCount] = 0.0;
                        }
                    }
                    fieldIndex++;
                }

                // Update row level min max
                for (int count = 0; count < measures.length; count++) {
                    if (aggType[count] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                        double value = (double) measures[count];
                        double maxVal = (double) max[count];
                        double minVal = (double) min[count];
                        max[count] = (maxVal > value ? max[count] : value);
                        min[count] = (minVal < value ? min[count] : value);
                        int num = (value % 1 == 0) ? 0 : decimalPointers;
                        decimal[count] = (decimal[count] > num ? decimal[count] : num);
                    } else if (aggType[count] == CarbonCommonConstants.BIG_INT_MEASURE) {
                        long value = (long) measures[count];
                        long maxVal = (long) max[count];
                        long minVal = (long) min[count];
                        max[count] = (maxVal > value ? max[count] : value);
                        min[count] = (minVal < value ? min[count] : value);
                        int num = (value % 1 == 0) ? 0 : decimalPointers;
                        decimal[count] = (decimal[count] > num ? decimal[count] : num);
                    } else if (aggType[count] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                        BigDecimal value = (BigDecimal) measures[count];
                        BigDecimal minVal = (BigDecimal) min[count];
                        min[count] = minVal.min(value);
                    }
                }
            }

            // Update file level min max
            calculateMaxMinUnique(max, min, decimal, measures.length);
        } catch (IOException e) {
            throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
        } finally {
            // close streams
            CarbonUtil.closeStreams(stream);
        }
    }

    private TempSortFileWriter getWriter() {
        TempSortFileWriter chunkWriter = null;
        TempSortFileWriter writer = TempSortFileWriterFactory.getInstance()
                .getTempSortFileWriter(isSortFileCompressionEnabled, dimColCount,
                        complexDimColCount, measureColCount, noDictionaryCount,
                        fileWriteBufferSize);

        if (prefetch && !isSortFileCompressionEnabled) {
            chunkWriter = new SortTempFileChunkWriter(writer, bufferSize);
        } else {
            chunkWriter = new SortTempFileChunkWriter(writer, sortTempFileNoOFRecordsInCompression);
        }

        return chunkWriter;
    }

    /**
     * Below method will be used to start the intermediate file merging
     *
     * @param intermediateFiles
     */
    private void startIntermediateMerging(File[] intermediateFiles) {
        File file = new File(
                this.tempFileLocation + File.separator + this.tableName + System.nanoTime()
                        + CarbonCommonConstants.MERGERD_EXTENSION);

        FileMergerParameters parameters = new FileMergerParameters();

        parameters.setDimColCount(dimColCount);
        parameters.setComplexDimColCount(complexDimColCount);
        parameters.setMeasureColCount(measureColCount);
        parameters.setIntermediateFiles(intermediateFiles);
        parameters.setFileReadBufferSize(fileBufferSize);
        parameters.setFileWriteBufferSize(fileBufferSize);
        parameters.setOutFile(file);
        parameters.setCompressionEnabled(isSortFileCompressionEnabled);
        parameters.setNoOfRecordsInCompression(sortTempFileNoOFRecordsInCompression);
        parameters.setPrefetch(prefetch);
        parameters.setPrefetchBufferSize(bufferSize);
        parameters.setAggType(aggType);
        parameters.setNoDictionaryCount(noDictionaryCount);

        IntermediateFileMerger merger = new IntermediateFileMerger(parameters);
        executorService.submit(merger);
    }

    /**
     * This method will be used to get the sort configuration
     *
     * @param instance
     */
    private void setSortConfiguration(CarbonProperties instance) {
        // get sort buffer size 
        this.sortBufferSize = Integer.parseInt(instance.getProperty(CarbonCommonConstants.SORT_SIZE,
                CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL));

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "Sort size for cube: " + this.sortBufferSize);

        // set number of intermedaite file to merge
        this.numberOfIntermediateFileToBeMerged = Integer.parseInt(
                instance.getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
                        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE));

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "Number of intermediate file to be merged: "
                        + this.numberOfIntermediateFileToBeMerged);

        // get file buffer size 
        this.fileBufferSize = CarbonDataProcessorUtil
                .getFileBufferSize(this.numberOfIntermediateFileToBeMerged,
                        CarbonProperties.getInstance(), CarbonCommonConstants.CONSTANT_SIZE_TEN);

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "File Buffer Size: " + this.fileBufferSize);
    }

    /**
     * This will be used to get the sort temo location
     *
     * @param instance
     */
    private void updateSortTempFileLocation(CarbonProperties instance) {
        // get the base location
        String tempLocationKey = schemaName + '_' + cubeName;
        String baseLocation = instance.getProperty(tempLocationKey,
                CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);

        // get the temp file location
        this.tempFileLocation =
                baseLocation + File.separator + schemaName + File.separator + cubeName
                        + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION
                        + File.separator + this.tableName;

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "temp file location" + this.tempFileLocation);
    }

    /**
     * This method will be used to delete sort temp location is it is exites
     *
     * @throws CarbonSortKeyAndGroupByException
     */
    public void deleteSortLocationIfExists() throws CarbonSortKeyAndGroupByException {
        CarbonDataProcessorUtil.deleteSortLocationIfExists(this.tempFileLocation);
    }

    /**
     * Below method will be used to start file based merge
     *
     * @throws CarbonSortKeyAndGroupByException
     */
    private void startFileBasedMerge() throws CarbonSortKeyAndGroupByException {
        try {
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
            writerExecutorService.shutdown();
            writerExecutorService.awaitTermination(2, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new CarbonSortKeyAndGroupByException("Problem while shutdown the server ", e);
        }
    }

    /**
     * This method will be used to update the max value for each measure
     */
    private void calculateMaxMinUnique(Object[] max, Object[] min, int[] decimal, int length) {
        synchronized (maxMinLock) {
            for (int i = 0; i < length; i++) {
                if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                    double prevMaxVal = (double) maxValue[i];
                    double prevMinVal = (double) minValue[i];
                    double curMaxVal = (double) max[i];
                    double curMinVal = (double) min[i];
                    maxValue[i] = (prevMaxVal > curMaxVal ? maxValue[i] : max[i]);
                    minValue[i] = (prevMinVal < curMinVal ? minValue[i] : min[i]);
                    uniqueValue[i] = (double) minValue[i] - 1;
                    decimalLength[i] =
                            (decimalLength[i] > decimal[i] ? decimalLength[i] : decimal[i]);
                } else if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                    long prevMaxVal = (long) maxValue[i];
                    long prevMinVal = (long) minValue[i];
                    long curMaxVal = (long) max[i];
                    long curMinVal = (long) min[i];
                    maxValue[i] = (prevMaxVal > curMaxVal ? maxValue[i] : max[i]);
                    minValue[i] = (prevMinVal < curMinVal ? minValue[i] : min[i]);
                    uniqueValue[i] = (long) minValue[i] - 1;
                    decimalLength[i] =
                            (decimalLength[i] > decimal[i] ? decimalLength[i] : decimal[i]);
                } else if (aggType[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                    BigDecimal val = (BigDecimal) minValue[i];
                    BigDecimal newVal = (BigDecimal) min[i];
                    val = val.min(newVal);
                    minValue[i] = val;
                    uniqueValue[i] = (val.subtract(new BigDecimal(1.0)));
                }
            }
        }
    }

    /**
     * Writes the measure metadata to a file
     */
    public void writeMeasureMetadataFile() {
        CarbonProperties instance = CarbonProperties.getInstance();

        // get the base store location
        String tempLocationKey = schemaName + '_' + cubeName;
        String baseStorelocation = instance.getProperty(tempLocationKey,
                CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL) + File.separator + schemaName
                + File.separator + cubeName;

        int restructFolderNumber = currentRestructNumber;

        baseStorelocation =
                baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
                        + restructFolderNumber + File.separator + this.tableName;

        // get the current folder sequence
        int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseStorelocation);

        File file = new File(baseStorelocation);

        // get the store location
        String storeLocation =
                file.getAbsolutePath() + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter
                        + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

        String metaDataFileName = CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
                + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT
                + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

        String measuremetaDataFilepath = storeLocation + File.separator + metaDataFileName;

        try {
            CarbonDataProcessorUtil
                    .writeMeasureMetaDataToFile(this.maxValue, this.minValue, this.decimalLength,
                            this.uniqueValue, aggType, new byte[this.maxValue.length],
                            measuremetaDataFilepath);
        } catch (CarbonDataProcessorException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Not able to write temp measure metadatafile.");
        }

        // first check if the metadata file already present the take backup and
        // rename inprofress file to
        // measure metadata and delete the bak file. else rename bak bak to
        // original file.

        File inprogress = new File(measuremetaDataFilepath);
        String inprogressFileName = inprogress.getName();
        String originalFileName =
                inprogressFileName.substring(0, inprogressFileName.lastIndexOf('.'));

        File originalFile = new File(storeLocation + File.separator + originalFileName);
        File bakFile = new File(storeLocation + File.separator + originalFileName + ".bak");

        if (originalFile.exists()) {
            if (!originalFile.renameTo(bakFile)) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "not able to rename original measure metadata file to bak fiel");
            }

        }

        if (!inprogress.renameTo(originalFile)) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Not able to rename inprogress File to original file in the sort temp folder.");
        } else {
            //delete the bak file.
            if (bakFile.exists()) {
                if (!bakFile.delete()) {
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                            "Not able to delete backup file " + bakFile.getName());
                }
            }
        }
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
        public void notifyFailed(Throwable exception) throws CarbonSortKeyAndGroupByException {
            writerExecutorService.shutdownNow();
            executorService.shutdownNow();
            observer.setFailed(true);
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, exception);
            throw new CarbonSortKeyAndGroupByException(exception);
        }
    }
}
