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

/**
 *
 */
package org.carbondata.query.processor.sort;

import java.util.*;

import org.apache.commons.collections.comparators.ComparatorChain;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.directinterface.impl.MeasureSortModel;
import org.carbondata.query.executer.Tuple;
import org.carbondata.query.executer.impl.comparator.MaksedByteComparatorForDFCH;
import org.carbondata.query.executer.impl.comparator.MaksedByteComparatorForTuple;
import org.carbondata.query.executer.impl.comparator.MeasureComparatorDFCH;
import org.carbondata.query.executer.impl.comparator.MeasureComparatorTuple;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.merger.SortedResultFileMerger;
import org.carbondata.query.processor.DataProcessor;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.util.ScannedResultProcessorUtil;
import org.carbondata.query.wrappers.ByteArrayWrapper;
import org.carbondata.query.writer.HeapBasedDataFileWriterThread;

/**
 * Project Name  : Carbon
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : SortMeasureProcessor.java
 * Description   : This class is responsible for sorting the data based on a measure.
 * Class Version  : 1.0
 */
public class SortMeasureProcessor implements DataProcessor {

    private DataProcessor dataProcessor;

    private DataProcessorInfo dataProcessorInfo;

    private String interFileLocation;

    /**
     * entryCount
     */
    private int entryCount;

    /**
     * comparatorChain
     */
    private Comparator comparatorChain;

    /**
     * dataHeap
     */
    private AbstractQueue<Tuple> dataHeap;

    private String outLocation;

    private boolean isFileBased;

    private MeasureSortModel measureSortModel;

    public SortMeasureProcessor(DataProcessor dataProcessor, String outLocation,
            boolean isFilebased, MeasureSortModel measureSortModel) {
        this.dataProcessor = dataProcessor;
        this.outLocation = outLocation;
        this.isFileBased = isFilebased;
        this.measureSortModel = measureSortModel;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.huawei.unibi.molap.engine.processor.DataProcessor#initialise(com.
     * huawei.unibi.molap.engine.processor.DataProcessorInfo)
     */
    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException {
        dataProcessorInfo = model;
        createComparatorChain();
        createHeap();
        this.interFileLocation = outLocation + '/' + model.getQueryId() + '/'
                + MolapCommonConstants.MEASURE_SORT_FOLDER;

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.huawei.unibi.molap.engine.processor.DataProcessor#processRow(byte[],
     * com.huawei.unibi.molap.engine.aggregator.MeasureAggregator[])
     */
    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException {
        addRow(key, value);
    }

    @Override
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value)
            throws DataProcessorException {
        processRow(key.getMaskedKey(), value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.huawei.unibi.molap.engine.processor.DataProcessor#finish()
     */
    @Override
    public void finish() throws DataProcessorException {

        if (isFileBased) {
            writeData();
            List compratorList = new ArrayList(10);
            MaksedByteComparatorForDFCH keyComparator = null;
            if (measureSortModel.getSortOrder() < 2) {
                for (int i = 0;
                     i < dataProcessorInfo.getMaskedByteRangeForSorting().length - 1; i++) {
                    if (null == dataProcessorInfo.getMaskedByteRangeForSorting()[i]) {
                        continue;
                    }
                    keyComparator = new MaksedByteComparatorForDFCH(
                            dataProcessorInfo.getMaskedByteRangeForSorting()[i],
                            dataProcessorInfo.getDimensionSortOrder()[i],
                            dataProcessorInfo.getDimensionMasks()[i]);
                    compratorList.add(keyComparator);
                }

            }
            MeasureComparatorDFCH measureComparator =
                    new MeasureComparatorDFCH(measureSortModel.getMeasureIndex(),
                            measureSortModel.getSortOrder());
            compratorList.add(measureComparator);
            //ComparatorChain comparatorChain = new ComparatorChain(compratorList);
            // DataFileMerger dataFileMerger = new DataFileMerger(model,
            // comparatorChain,dataProcessor,interFileLocation);
            SortedResultFileMerger sortedFileMergerThread =
                    new SortedResultFileMerger(dataProcessor, dataProcessorInfo,
                            ScannedResultProcessorUtil.getFiles(interFileLocation,
                                    new String[] { MolapCommonConstants.QUERY_OUT_FILE_EXT,
                                            MolapCommonConstants.QUERY_MERGED_FILE_EXT }));
            try {
                sortedFileMergerThread.call();
            } catch (Exception e) {
                throw new DataProcessorException(e);
            }
        } else {
            // CHECKSTYLE:OFF Approval No:Approval-V3R8C00_003
            int size = dataHeap.size();
            if (measureSortModel.isBreakHeir()) {
                List compratorList = new ArrayList();
                if (dataProcessorInfo.getDimensionSortOrder().length < 1) {
                    MaksedByteComparatorForTuple keyComparator = null;

                    for (int i = 0;
                         i < dataProcessorInfo.getMaskedByteRangeForSorting().length - 1; i++) {
                        if (null == dataProcessorInfo.getMaskedByteRangeForSorting()[i]) {
                            continue;
                        }
                        keyComparator = new MaksedByteComparatorForTuple(
                                dataProcessorInfo.getMaskedByteRangeForSorting()[i],
                                dataProcessorInfo.getDimensionSortOrder()[i],
                                dataProcessorInfo.getDimensionMasks()[i]);
                        compratorList.add(keyComparator);
                    }

                }
                MeasureComparatorTuple measureComparator =
                        new MeasureComparatorTuple(measureSortModel.getMeasureIndex(),
                                measureSortModel.getSortOrder());
                compratorList.add(measureComparator);
                ComparatorChain comparatorChain = new ComparatorChain(compratorList);

                List<Tuple> tuples = new ArrayList<Tuple>(dataHeap);

                Collections.sort(tuples, comparatorChain);

                for (Tuple tuple : tuples) {
                    dataProcessor.processRow(tuple.getKey(), tuple.getMeasures());
                }
                // CHECKSTYLE:ON
            } else {
                for (int i = 0; i < size; i++) {
                    Tuple tuple = dataHeap.poll();
                    dataProcessor.processRow(tuple.getKey(), tuple.getMeasures());
                }
            }

            dataProcessor.finish();
        }

    }

    /**
     * Below method will be used to create chain comparator for sorting
     */
    private void createComparatorChain() {
        List compratorList = new ArrayList(10);
        MaksedByteComparatorForTuple keyComparator = null;
        if (measureSortModel.getSortOrder() < 2) {
            for (int i = 0; i < dataProcessorInfo.getMaskedByteRangeForSorting().length - 1; i++) {
                if (null == dataProcessorInfo.getMaskedByteRangeForSorting()[i]) {
                    continue;
                }
                keyComparator = new MaksedByteComparatorForTuple(
                        dataProcessorInfo.getMaskedByteRangeForSorting()[i],
                        dataProcessorInfo.getDimensionSortOrder()[i],
                        dataProcessorInfo.getDimensionMasks()[i]);
                compratorList.add(keyComparator);
            }
        }
        this.comparatorChain = new ComparatorChain(compratorList);

        MeasureComparatorTuple measureComparator =
                new MeasureComparatorTuple(measureSortModel.getMeasureIndex(),
                        measureSortModel.getSortOrder());
        compratorList.add(measureComparator);

    }

    /**
     * Below method will be used to create the heap holder
     */
    private void createHeap() {
        this.dataHeap =
                new PriorityQueue<Tuple>(dataProcessorInfo.getHolderSize(), this.comparatorChain);
    }

    /**
     * below method will be used to add row to heap, heap size reach holder size
     * it will sort the data based dimension and measures(dimension for keep
     * heir) and write data to file
     *
     * @param key
     * @param measures
     * @throws MolapPaginationException
     */
    private void addRow(byte[] key, MeasureAggregator[] measures) throws DataProcessorException {
        if ((this.entryCount == dataProcessorInfo.getHolderSize()) && isFileBased) {
            writeData();
        }
        Tuple tuple = new Tuple();
        tuple.setKey(key);
        tuple.setMeasures(measures);
        this.dataHeap.add(tuple);
        this.entryCount++;
    }

    /**
     * Below method will be used to write sorted data to file
     *
     * @throws MolapPaginationException
     */
    private void writeData() throws DataProcessorException {
        HeapBasedDataFileWriterThread dataWriter =
                new HeapBasedDataFileWriterThread(dataHeap, dataProcessorInfo, interFileLocation);
        try {
            dataWriter.call();
        } catch (Exception e) {
            throw new DataProcessorException(e);
        }
        this.entryCount = 0;
        createHeap();
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator() {
        // TODO Auto-generated method stub
        return null;
    }

}
