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

package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.*;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;

public class IntermediateFileMerger implements Callable<Void> {

    /**
     * LOGGER
     */
    private static final LogService FILEMERGERLOGGER =
            LogServiceFactory.getLogService(IntermediateFileMerger.class.getName());

    /**
     * recordHolderHeap
     */
    private AbstractQueue<MolapSortTempFileChunkHolder> recordHolderHeap;

    /**
     * measure count
     */
    private int measureCount;

    /**
     * mdKeyLenght
     */
    private int mdKeyLength;

    /**
     * intermediateFiles
     */
    private File[] intermediateFiles;

    /**
     * outFile
     */
    private File outFile;

    /**
     * fileCounter
     */
    private int fileCounter;

    /**
     * mdkeyIndex
     */
    private int mdKeyIndex;

    /**
     * fileBufferSize
     */
    private int fileReadBufferSize;

    /**
     * fileWriteSize
     */
    private int fileWriteBufferSize;

    /**
     * stream
     */
    private DataOutputStream stream;

    /**
     * totalNumberOfRecords
     */
    private int totalNumberOfRecords;

    /**
     * isRenamingRequired
     */
    private boolean isRenamingRequired;

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
     * records
     */
    private Object[][] records;

    /**
     * entryCount
     */
    private int entryCount;

    /**
     * writer
     */
    private MolapSortTempFileWriter writer;

    /**
     * type
     */
    private char[] type;

    /**
     * prefetch
     */
    private boolean prefetch;

    /**
     * prefetchBufferSize
     */
    private int prefetchBufferSize;

    /**
     * totalSize
     */
    private int totalSize;

    private String[] aggregator;

    /**
     * highCardinalityCount
     */
    private int highCardinalityCount;

    /**
     * IntermediateFileMerger Constructor
     *
     * @param intermediateFiles intermediateFiles
     * @param measureCount      measureCount
     * @param mdKeyLength       mdKeyLength
     * @param outFile           outFile
     */
    public IntermediateFileMerger(File[] intermediateFiles, int fileReadBufferSize,
            int measureCount, int mdKeyLength, File outFile, int mdkeyIndex,
            int fileWriteBufferSize, boolean isRenamingRequired, boolean isFactMdkeyInInputRow,
            int factMdkeyLength, int sortTempFileNoOFRecordsInCompression,
            boolean isSortTempFileCompressionEnabled, char[] type, boolean prefetch,
            int prefetchBufferSize, String[] aggregator, int highCardinalityCount) {
        this.intermediateFiles = intermediateFiles;
        this.measureCount = measureCount;
        this.mdKeyLength = mdKeyLength;
        this.outFile = outFile;
        this.fileCounter = intermediateFiles.length;
        this.mdKeyIndex = mdkeyIndex;
        this.fileReadBufferSize = fileReadBufferSize;
        this.fileWriteBufferSize = fileWriteBufferSize;
        this.isRenamingRequired = isRenamingRequired;
        this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
        this.factMdkeyLength = factMdkeyLength;
        this.sortTempFileNoOFRecordsInCompression = sortTempFileNoOFRecordsInCompression;
        this.isSortTempFileCompressionEnabled = isSortTempFileCompressionEnabled;
        this.type = type;
        this.prefetch = prefetch;
        this.prefetchBufferSize = prefetchBufferSize;
        this.aggregator = aggregator;
        this.highCardinalityCount = highCardinalityCount;
    }

    @Override public Void call() throws Exception {
        boolean isFailed = false;
        try {
            startSorting();
            initialize();
            while (hasNext()) {
                writeDataTofile(next());
            }
            if (isSortTempFileCompressionEnabled || prefetch) {
                if (entryCount > 0) {
                    if (entryCount < totalSize) {
                        Object[][] tempArr = new Object[entryCount][];
                        System.arraycopy(records, 0, tempArr, 0, entryCount);
                        records = tempArr;
                        this.writer.writeSortTempFile(tempArr);
                    } else {
                        this.writer.writeSortTempFile(records);
                    }
                }
            }
        } catch (Exception ex) {
            FILEMERGERLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex,
                    "Problem while intermediate merging");
            isFailed = true;
        } finally {
            MolapUtil.closeStreams(this.stream);
            records = null;
            if (null != writer) {
                writer.finish();
            }
            if (!isFailed) {
                try {
                    finish();
                } catch (MolapSortKeyAndGroupByException e) {
                    FILEMERGERLOGGER
                            .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                                    "Problem while deleting the merge file");
                }
            } else {
                if (this.outFile.delete()) {
                    FILEMERGERLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while deleting the merge file");
                }
            }

        }

        if (this.isRenamingRequired) {
            String destFileName = this.outFile.getAbsolutePath();
            String[] split = destFileName.split(MolapCommonConstants.BAK_EXT);
            File renamed = new File(split[0]);
            if (!this.outFile.renameTo(renamed)) {
                FILEMERGERLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Problem while renaming the checkpoint file");
            }
        }
        return null;
    }

    /**
     * This method is responsible for initialising the out stream
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void initialize() throws MolapSortKeyAndGroupByException {
        if (!isSortTempFileCompressionEnabled && !prefetch) {
            try {
                this.stream = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(this.outFile),
                                this.fileWriteBufferSize));
                this.stream.writeInt(this.totalNumberOfRecords);
            } catch (FileNotFoundException e) {
                throw new MolapSortKeyAndGroupByException("Problem while getting the file", e);
            } catch (IOException e) {
                throw new MolapSortKeyAndGroupByException("Problem while writing the data to file",
                        e);
            }
        } else if (prefetch && !isSortTempFileCompressionEnabled) {
            writer = new MolapUnCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
                    isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type);
            totalSize = prefetchBufferSize;
            writer.initiaize(outFile, totalNumberOfRecords);
        } else {
            writer = new MolapCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
                    isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type);
            totalSize = sortTempFileNoOFRecordsInCompression;
            writer.initiaize(outFile, totalNumberOfRecords);
        }

    }

    /**
     * This method will be used to get the sorted record from file
     *
     * @return sorted record sorted record
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[] getSortedRecordFromFile() throws MolapSortKeyAndGroupByException {
        Object[] row = null;
        // poll the top object from heap
        // heap maintains binary tree which is based on heap condition that will
        // be based on comparator we are passing the heap
        // when will call poll it will always delete root of the tree and then
        // it does trickel down operation complexity is log(n)
        MolapSortTempFileChunkHolder chunkHolderPoll = this.recordHolderHeap.poll();
        // get the row from chunk
        row = chunkHolderPoll.getRow();
        // check if there no entry present
        if (!chunkHolderPoll.hasNext()) {
            // if chunk is empty then close the stream
            chunkHolderPoll.closeStream();
            // change the file counter
            --this.fileCounter;
            // reaturn row
            return row;
        }
        // read new row
        chunkHolderPoll.readRow();
        // add to heap
        this.recordHolderHeap.add(chunkHolderPoll);
        // return row
        return row;
    }

    /**
     * Below method will be used to start storing process This method will get
     * all the temp files present in sort temp folder then it will create the
     * record holder heap and then it will read first record from each file and
     * initialize the heap
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void startSorting() throws MolapSortKeyAndGroupByException {
        FILEMERGERLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Number of temp file: " + this.fileCounter);
        // create record holder heap
        createRecordHolderQueue(this.intermediateFiles);
        // iterate over file list and create chunk holder and add to heap
        FILEMERGERLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Started adding first record from each file");
        MolapSortTempFileChunkHolder molapSortTempFileChunkHolder = null;
        for (File tempFile : this.intermediateFiles) {
            // create chunk holder
            molapSortTempFileChunkHolder =
                    new MolapSortTempFileChunkHolder(tempFile, this.measureCount, this.mdKeyLength,
                            this.fileReadBufferSize, this.isFactMdkeyInInputRow,
                            this.factMdkeyLength, this.aggregator, this.highCardinalityCount,
                            this.type);
            // initialize
            molapSortTempFileChunkHolder.initialize();
            molapSortTempFileChunkHolder.readRow();
            this.totalNumberOfRecords += molapSortTempFileChunkHolder.getEntryCount();
            // add to heap
            this.recordHolderHeap.add(molapSortTempFileChunkHolder);
        }
        FILEMERGERLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Heap Size" + this.recordHolderHeap.size());
    }

    /**
     * This method will be used to create the heap which will be used to hold
     * the chunk of data
     *
     * @param listFiles list of temp files
     */
    private void createRecordHolderQueue(File[] listFiles) {
        // creating record holder heap
        this.recordHolderHeap = new PriorityQueue<MolapSortTempFileChunkHolder>(listFiles.length,
                new Comparator<MolapSortTempFileChunkHolder>() {
                    public int compare(MolapSortTempFileChunkHolder r1,
                            MolapSortTempFileChunkHolder r2) {
                        byte[] b1 = (byte[]) r1.getRow()[mdKeyIndex];
                        byte[] b2 = (byte[]) r2.getRow()[mdKeyIndex];
                        int cmp = 0;

                        for (int i = 0; i < mdKeyLength; i++) {
                            int a = b1[i] & 0xFF;
                            int b = b2[i] & 0xFF;
                            cmp = a - b;
                            if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return cmp;
                    }
                });
    }

    /**
     * This method will be used to get the sorted row
     *
     * @return sorted row
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[] next() throws MolapSortKeyAndGroupByException {
        return getSortedRecordFromFile();
    }

    /**
     * This method will be used to check whether any more element is present or
     * not
     *
     * @return more element is present
     */
    private boolean hasNext() {
        return this.fileCounter > 0;
    }

    /**
     * Below method will be used to write data to file
     *
     * @throws MolapSortKeyAndGroupByException problem while writing
     */
    private void writeDataTofile(Object[] row) throws MolapSortKeyAndGroupByException {
        if (isSortTempFileCompressionEnabled || prefetch) {
            if (entryCount == 0) {
                records = new Object[totalSize][];
                records[entryCount++] = row;
                return;
            }
            records[entryCount++] = row;
            if (entryCount == totalSize) {
                entryCount = 0;
                this.writer.writeSortTempFile(records);
                records = new Object[totalSize][];
            }
            return;
        }
        try {
            int aggregatorIndexInRowObject = 0;
            // get row from record holder list
            MeasureAggregator[] aggregator = (MeasureAggregator[]) row[aggregatorIndexInRowObject];
            MolapDataProcessorUtil.writeMeasureAggregatorsToSortTempFile(type, stream, aggregator);
            stream.writeDouble((Double) row[aggregatorIndexInRowObject + 1]);

            // writing the high cardinality data.
            if (highCardinalityCount > 0) {
                int highCardIndex = this.mdKeyIndex - 1;
                byte[] singleHighCardArr = (byte[]) row[highCardIndex];
                stream.write(singleHighCardArr);
            }

            // write mdkye
            stream.write((byte[]) row[this.mdKeyIndex]);
            if (this.isFactMdkeyInInputRow) {
                stream.write((byte[]) row[row.length - 1]);
            }
        } catch (IOException e) {
            throw new MolapSortKeyAndGroupByException("Problem while writing the file", e);
        }
    }

    private void finish() throws MolapSortKeyAndGroupByException {
        if (recordHolderHeap != null) {
            int size = recordHolderHeap.size();
            for (int i = 0; i < size; i++) {
                recordHolderHeap.poll().closeStream();
            }
        }
        try {
            MolapUtil.deleteFiles(this.intermediateFiles);
        } catch (MolapUtilException e) {
            throw new MolapSortKeyAndGroupByException(
                    "Problem while deleting the intermediate files");
        }
    }
}
