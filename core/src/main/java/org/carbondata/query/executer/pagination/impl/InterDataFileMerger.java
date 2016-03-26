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

package org.carbondata.query.executer.pagination.impl;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.CalculatedMeasure;
import org.carbondata.core.metadata.MolapMetadata.Measure;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.util.MolapUtilException;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.AggUtil;
import org.carbondata.query.datastorage.InMemoryCube;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.executer.pagination.lru.LRUCacheKey;
import org.carbondata.query.util.MolapEngineLogEvent;

//import java.lang.reflect.Constructor;

/**
 * Intermediate file merging based on file sizes
 */
public class InterDataFileMerger implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(InterDataFileMerger.class.getName());
    /**
     * dataOutFile
     */
    private File[] dataOutFiles;
    /**
     * outLocation
     */
    private String outLocation;
    /**
     * recordHolderHeap
     */
    private AbstractQueue<DataFileChunkHolder> recordHolderHeap;
    /**
     * keySize
     */
    private int keySize;
    /**
     * fileBufferSize
     */
    private int fileBufferSize;
    /**
     * fileCounter
     */
    private int fileCounter;
    /**
     * measureAggregators
     */
    private MeasureAggregator[] measureAggregators;
    /**
     * entryCount
     */
    private int entryCount;
    /**
     * prevMsrs
     */
    private MeasureAggregator[] prevMsrs;
    /**
     * prevKey
     */
    private byte[] prevKey;
    private BufferedOutputStream bout;
    private DataOutputStream dataOutput;
    private LRUCacheKey holder;
    private File file;
    private Measure[] measures;
    private InMemoryCube slice;
    private KeyGenerator keyGenerator;
    /**
     *
     */
    private CalculatedMeasure[] calculatedMeasures;

    /**
     * DataFileMerger {@link Constructor}
     *
     * @param keySize      Masked key size
     * @param recordSize   record Size
     * @param dataOutFiles out Files to be merged
     * @param outLocation  output location
     * @param queryId      query id
     * @throws Exception
     */
    public InterDataFileMerger(int keySize, String outLocation, String queryId, int fileBufferSize,
            MeasureAggregator[] measureAggregators, File[] dataOutFiles, LRUCacheKey holder,
            Comparator<DataFileChunkHolder> heapComparator, Measure[] measures,
            CalculatedMeasure[] calculatedMeasures, KeyGenerator keyGenerator, InMemoryCube slice)
            throws Exception {
        this.keySize = keySize;
        this.outLocation = outLocation;
        this.dataOutFiles = dataOutFiles;
        this.holder = holder;

        this.fileCounter = this.dataOutFiles.length;
        if (this.fileCounter == 0) {
            return;
        }
        this.fileBufferSize = fileBufferSize;
        this.measureAggregators = measureAggregators;
        this.measures = measures;
        this.calculatedMeasures = calculatedMeasures;
        this.slice = slice;
        this.keyGenerator = keyGenerator;
        this.recordHolderHeap =
                new PriorityQueue<DataFileChunkHolder>(this.fileCounter, heapComparator);

        file = new File(
                this.outLocation + File.separator + queryId + File.separator + System.nanoTime()
                        + MolapCommonConstants.QUERY_MERGED_FILE_EXT);
        bout = new BufferedOutputStream(new FileOutputStream(file),
                MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                        * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR);
        dataOutput = new DataOutputStream(bout);

    }

    /**
     * @see Callable#call()
     */
    @Override public Void call() throws Exception {
        try {
            for (File fileInfo : dataOutFiles) {
                // create chunk holder
                DataFileChunkHolder molapSortTempFileChunkHolder =
                        new DataFileChunkHolder(fileInfo, this.keySize, this.measures,
                                AggUtil.getAggregators(this.measures, calculatedMeasures, false,
                                        keyGenerator, slice.getCubeUniqueName()),
                                this.fileBufferSize);
                // initialize
                molapSortTempFileChunkHolder.initialize();
                // add to heap
                this.recordHolderHeap.add(molapSortTempFileChunkHolder);
            }
            while (hasNext()) {
                writeSortedRecordToFile();
            }
            writeRow();
            dataOutput.writeInt(entryCount);
        } catch (MolapPaginationException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw e;
        } catch (Exception e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new MolapPaginationException("Problem while deleting the query part file", e);
        } finally {
            MolapUtil.closeStreams(bout, dataOutput);
        }
        try {
            holder.setIncrementalSize(file.length());

            for (int i = 0; i < dataOutFiles.length; i++) {
                holder.setDecrementalSize(dataOutFiles[i].length());
            }
            MolapUtil.deleteFoldersAndFiles(dataOutFiles);

        } catch (MolapUtilException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new MolapPaginationException("Problem while deleting the query part file", e);
        }

        return null;
    }

    //TODO SIMIAN

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
     * This method will be used to get the sorted record from file
     *
     * @return sorted record
     * sorted record
     * @throws MolapSortKeyAndGroupByException
     */
    private void writeSortedRecordToFile() throws MolapPaginationException {
        // poll the top object from heap
        // heap maintains binary tree which is based on heap condition that will
        // be based on comparator we are passing the heap
        // when will call poll it will always delete root of the tree and then
        // it does trickel down operation complexity is log(n)
        DataFileChunkHolder poll = this.recordHolderHeap.poll();
        // check if there no entry present
        //        poll.readRow();
        if (!poll.hasNext()) {
            // if chunk is empty then close the stream
            poll.closeStream();
            // change the file counter
            --this.fileCounter;
            // reaturn row
            return;
        }
        addRow(poll);
        // read new row
        poll.readRow();
        // add to heap
        this.recordHolderHeap.add(poll);
        // return row
    }

    private void addRow(DataFileChunkHolder poll) throws MolapPaginationException {
        byte[] key = poll.getKey();
        MeasureAggregator[] measures = poll.getMeasures();
        if (prevKey != null) {
            if (ByteUtil.compare(key, prevKey) == 0) {
                aggregateData(prevMsrs, measures);
            } else {
                writeRow();
            }
        }
        poll.setMeasureAggs(
                AggUtil.getAggregators(this.measures, calculatedMeasures, false, keyGenerator,
                        slice.getCubeUniqueName()));
        key = key.clone();
        prevKey = key;
        prevMsrs = measures;

    }

    /**
     *
     */
    private void writeRow() {
        try {
            dataOutput.write(prevKey);

            for (int i = 0; i < this.measureAggregators.length; i++) {
                prevMsrs[i].writeData(dataOutput);
            }
        } catch (Exception e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
        entryCount++;
    }

    private void aggregateData(MeasureAggregator[] src, MeasureAggregator[] dest) {
        for (int i = 0; i < dest.length; i++) {
            dest[i].merge(src[i]);
        }
    }

    /**
     * This method will be used to create the heap which will be used to hold
     * the chunk of data
     *
     * @param listFiles list of temp files
     */

    private class HeapComparator implements Comparator<DataFileChunkHolder> {
        /**
         * key size
         */
        private int keySize;

        /**
         * HeapComparator {@link Constructor}
         *
         * @param keySize
         */
        public HeapComparator(int keySize) {
            this.keySize = keySize;
        }

        /**
         * Below method will be used to compare result
         *
         * @param r1 MolapSortTempFileChunkHolder
         * @param r2 MolapSortTempFileChunkHolder
         */
        public int compare(DataFileChunkHolder r1, DataFileChunkHolder r2) {
            byte[] b1 = r1.getRow();
            byte[] b2 = r2.getRow();
            int cmp = 0;

            for (int i = 0; i < this.keySize; i++) {
                int a = b1[i] & 0xFF;
                int b = b2[i] & 0xFF;
                cmp = a - b;
                if (cmp == 0) {
                    continue;
                }
                cmp = cmp < 0 ? -1 : 1;
                break;
            }
            return cmp;
        }

    }
}
