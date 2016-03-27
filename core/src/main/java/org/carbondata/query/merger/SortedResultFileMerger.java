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

package org.carbondata.query.merger;

import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.util.AggUtil;
import org.carbondata.query.merger.exception.ResultMergerException;
import org.carbondata.query.processor.DataProcessor;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.reader.ResultTempFileReader;
import org.carbondata.query.reader.exception.ResultReaderException;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Project Name  : Carbon
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : SortedResultFileMerger.java
 * Description   : This class is responsible for handling the merge of the result data
 * in a sorted order. This will use the heap to get the data sorted.
 * Class Version  : 1.0
 */
public class SortedResultFileMerger implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(SortedResultFileMerger.class.getName());

    /**
     * dataProcessorInfo holds all the metadata related to query execution.
     */
    private DataProcessorInfo dataProcessorInfo;

    /**
     * recordHolderHeap
     */
    private AbstractQueue<ResultTempFileReader> recordHolderHeap;

    /**
     * fileCounter
     */
    private int fileCounter;

    /**
     * dataProcessor This is the data processor object which will process the data.
     */
    private DataProcessor dataProcessor;

    /**
     * files all the intermediate files.
     */
    private CarbonFile[] files;

    public SortedResultFileMerger(final DataProcessor dataProcessor,
            final DataProcessorInfo dataProcessorInfo, final CarbonFile[] files) {
        this.dataProcessorInfo = dataProcessorInfo;
        this.files = files;
        fileCounter = files.length;
        this.recordHolderHeap = new PriorityQueue<ResultTempFileReader>(this.fileCounter,
                dataProcessorInfo.getHeapComparator());
        this.dataProcessor = dataProcessor;
    }

    /**
     * @see Callable#call()
     */
    @Override
    public Void call() throws Exception {
        try {
            dataProcessor.initialise(dataProcessorInfo);
            // For each intermediate result file.
            for (CarbonFile file : this.files) {
                // reads the temp files and creates ResultTempFileReader object.
                ResultTempFileReader molapSortTempFileChunkHolder =
                        new ResultTempFileReader(file.getAbsolutePath(),
                                dataProcessorInfo.getKeySize(),
                                AggUtil.getAggregators(dataProcessorInfo.getAggType(), false,
                                        dataProcessorInfo.getKeyGenerator(),
                                        dataProcessorInfo.getCubeUniqueName(),
                                        dataProcessorInfo.getMsrMinValue(), null,
                                        dataProcessorInfo.getDataTypes()),
                                dataProcessorInfo.getFileBufferSize());
                // initialize
                molapSortTempFileChunkHolder.initialize();
                molapSortTempFileChunkHolder.readRow();
                // add ResultTempFileReader object to heap
                this.recordHolderHeap.add(molapSortTempFileChunkHolder);
            }
            while (hasNext()) {
                writeSortedRecordToFile();
            }
        } catch (ResultReaderException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw e;
        } finally {
            dataProcessor.finish();
            //delete the temp files.
            CarbonUtil.deleteFoldersAndFiles(this.files);
        }
        return null;
    }

    /**
     * This method will be used to check whether any more files are present or
     * not.
     *
     * @return more element is present
     */
    private boolean hasNext() {
        return this.fileCounter > 0;
    }

    /**
     * This method will be used to write the sorted record from file
     *
     * @return sorted record sorted record
     * @throws MolapSortKeyAndGroupByException
     */
    private void writeSortedRecordToFile() throws ResultMergerException {
        // poll the top object from heap
        // heap maintains binary tree which is based on heap condition that will
        // be based on comparator we are passing the heap
        // when will call poll it will always delete root of the tree and then
        // it does trickle down operation. complexity is log(n)
        ResultTempFileReader dataFile = this.recordHolderHeap.poll();
        // check if there no entry present.
        if (!dataFile.hasNext()) {
            // if chunk is empty then close the stream
            dataFile.closeStream();
            // change the file counter
            --this.fileCounter;
            return;
        }
        try {
            // process the row based on the dataprocessor type.
            dataProcessor.processRow(dataFile.getKey(), dataFile.getMeasures());
        } catch (DataProcessorException e) {
            throw new ResultMergerException(e);
        }

        dataFile.setMeasureAggs(AggUtil.getAggregators(dataProcessorInfo.getAggType(), false,
                dataProcessorInfo.getKeyGenerator(), dataProcessorInfo.getCubeUniqueName(),
                dataProcessorInfo.getMsrMinValue(), null, dataProcessorInfo.getDataTypes()));
        try {
            // read the next row to process and add to the heap.
            dataFile.readRow();
        } catch (ResultReaderException e) {
            throw new ResultMergerException(e);
        }
        // add to heap
        this.recordHolderHeap.add(dataFile);
    }
}
