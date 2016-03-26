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

import java.io.File;
import java.io.FileFilter;
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
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.executer.pagination.lru.LRUCacheKey;
import org.carbondata.query.util.MolapEngineLogEvent;

//import java.lang.reflect.Constructor;

public class DataFileMerger implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DataFileMerger.class.getName());

    /**
     * dataOutFile
     */
    private File[] dataOutFiles;

    /**
     * outLocation
     */
    private String folderLocation;

    /**
     * queryId
     */
    //    private String queryId;

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
    //    private MeasureAggregator[]measureAggregators;

    /**
     * prevMsrs
     */
    private MeasureAggregator[] prevMsrs;

    /**
     * prevKey
     */
    private byte[] prevKey;

    /**
     * processor
     */
    private DataProcessor processor;

    /**
     * ResultSizeHolder
     */
    private LRUCacheKey holder;

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
     */
    public DataFileMerger(PaginationModel model, Comparator<DataFileChunkHolder> heapComparator,
            DataProcessor processor, String folderLocation) {
        this.keySize = model.getKeySize();
        this.folderLocation = folderLocation;
        //        this.queryId = model.getQueryId();
        File path = new File(folderLocation);
        if (path.exists()) {
            this.dataOutFiles = path.listFiles(new FileFilter() {

                @Override public boolean accept(File pathname) {
                    return pathname.getName().endsWith(MolapCommonConstants.QUERY_OUT_FILE_EXT)
                            || pathname.getName()
                            .endsWith(MolapCommonConstants.QUERY_MERGED_FILE_EXT);
                }
            });
        } else {
            this.dataOutFiles = new File[0];
        }
        this.calculatedMeasures = model.getCalculatedMeasures();
        this.fileCounter = this.dataOutFiles.length;
        this.fileBufferSize = model.getFileBufferSize();
        //        this.measureAggregators = model.getMeasureAggregators();
        this.recordHolderHeap =
                new PriorityQueue<DataFileChunkHolder>(this.fileCounter, heapComparator);
        holder = model.getHolder();
        slice = model.getSlices().get(0);
        keyGenerator = model.getKeyGenerator();
        measures = model.getQueryMsrs();
        this.processor = processor;
    }

    /**
     * @see Callable#call()
     */
    @Override public Void call() throws Exception {
        try {
            for (File file : dataOutFiles) {
                // create chunk holder
                DataFileChunkHolder molapSortTempFileChunkHolder =
                        new DataFileChunkHolder(file, this.keySize, this.measures,
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

            processor.processRow(prevKey, prevMsrs);
            processor.finish();
        } catch (MolapPaginationException e) {
            throw e;
        } catch (Throwable e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new MolapPaginationException("Problem while deleting the query part file", e);
        }
        try {
            File file = new File(folderLocation);
            File[] listFiles = file.listFiles();
            if (null != listFiles) {
                for (int i = 0; i < listFiles.length; i++) {
                    holder.setDecrementalSize(listFiles[i].length());
                }
            }
            MolapUtil.deleteFoldersAndFiles(file);
        } catch (MolapUtilException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new MolapPaginationException("Problem while deleting the query part file", e);
        }

        return null;
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
        addRow(poll);
        if (!poll.hasNext()) {
            // if chunk is empty then close the stream
            poll.closeStream();
            // change the file counter
            --this.fileCounter;
            return;
        }
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
                processor.processRow(prevKey, prevMsrs);
            }
        }

        poll.setMeasureAggs(
                AggUtil.getAggregators(this.measures, calculatedMeasures, false, keyGenerator,
                        slice.getCubeUniqueName()));
        key = key.clone();
        prevKey = key;
        prevMsrs = measures;

    }

    private void aggregateData(MeasureAggregator[] src, MeasureAggregator[] dest) {
        for (int i = 0; i < dest.length; i++) {
            dest[i].merge(src[i]);
        }
    }

}
