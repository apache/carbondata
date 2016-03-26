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

package org.carbondata.query.executer.impl.measure.sort;

import java.io.File;
import java.util.*;

import org.apache.commons.collections.comparators.ComparatorChain;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.directinterface.impl.MeasureSortModel;
import org.carbondata.query.executer.Tuple;
import org.carbondata.query.executer.groupby.GroupByHolder;
import org.carbondata.query.executer.impl.comparator.MaksedByteComparatorForDFCH;
import org.carbondata.query.executer.impl.comparator.MaksedByteComparatorForTuple;
import org.carbondata.query.executer.impl.comparator.MeasureComparatorDFCH;
import org.carbondata.query.executer.impl.comparator.MeasureComparatorTuple;
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.executer.pagination.impl.DataFileMerger;
import org.carbondata.query.executer.pagination.impl.DataFileWriter;

/**
 * Class Description : Processor class to sort the data based on measures
 * Version 1.0
 */
public class MeasureSortProcessor implements DataProcessor {
    /**
     * dataProcessor
     */
    private DataProcessor dataProcessor;

    /**
     * msrSortModel
     */
    private MeasureSortModel msrSortModel;

    /**
     * maskedByteRangeForsorting
     */
    private int[][] maskedByteRangeForSorting;

    /**
     * dataHeap
     */
    private AbstractQueue<Tuple> dataHeap;

    /**
     * holderSize
     */
    private int holderSize;

    /**
     * entryCount
     */
    private int entryCount;

    /**
     * dimensionSortOrder
     */
    private byte[] dimensionSortOrder;

    /**
     * dataFileWriter
     */
    private DataFileWriter dataFileWriter;

    /**
     * queryId
     */
    private String queryId;

    /**
     * outLocation
     */
    private String outLocation;

    /**
     * dimensionMasks
     */
    private byte[][] dimensionMasks;

    /**
     * comparatorChain
     */
    private Comparator comparatorChain;

    /**
     * model
     */
    private PaginationModel model;

    /**
     * interFileLocation
     */
    private String interFileLocation;

    /**
     * paginationEnabled
     */
    private boolean paginationEnabled;

    /**
     * MeasureFilterProcessor Constructor
     *
     * @param dataProcessor
     */
    public MeasureSortProcessor(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    /**
     * Below method will be used to initialize the Processor
     *
     * @throws MolapPaginationException
     */
    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException {
        this.model = model;
        this.msrSortModel = model.getMsrSortModel();
        this.maskedByteRangeForSorting = model.getMaskedByteRangeForSorting();
        this.holderSize = model.getHolderSize();
        this.dimensionSortOrder = model.getDimensionSortOrder();
        this.dimensionMasks = model.getDimensionMasks();
        this.queryId = model.getQueryId();
        this.outLocation = model.getOutLocation();
        createComparatorChain();
        createHeap();
        this.interFileLocation = this.outLocation + File.separator + queryId + File.separator
                + MolapCommonConstants.MEASURE_SORT_FOLDER;
        this.paginationEnabled = model.isPaginationEnabled();
        dataProcessor.initModel(model);

    }

    /**
     * Below method will be used to process the data
     *
     * @param key
     * @param measures
     */
    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures)
            throws MolapPaginationException {
        addRow(key, measures);
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
    private void addRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException {
        if ((this.entryCount == this.holderSize) && paginationEnabled) {
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
    private void writeData() throws MolapPaginationException {
        model.setRecordHolderType(MolapCommonConstants.HEAP);
        dataFileWriter =
                new DataFileWriter(dataHeap, model, MolapCommonConstants.HEAP, interFileLocation);
        try {
            dataFileWriter.call();
        } catch (Exception e) {
            throw new MolapPaginationException(e);
        }
        this.entryCount = 0;
        createHeap();
    }

    /**
     * Below method will be used to create chain comparator for sorting
     */
    private void createComparatorChain() {
        List compratorList = new ArrayList(MolapCommonConstants.CONSTANT_SIZE_TEN);
        MaksedByteComparatorForTuple keyComparator = null;
        if (this.msrSortModel.getSortOrder() < 2) {
            for (int i = 0; i < this.maskedByteRangeForSorting.length - 1; i++) {
                keyComparator = new MaksedByteComparatorForTuple(this.maskedByteRangeForSorting[i],
                        this.dimensionSortOrder[i], this.dimensionMasks[i]);
                compratorList.add(keyComparator);
            }
        }
        MeasureComparatorTuple measureComparator =
                new MeasureComparatorTuple(msrSortModel.getMeasureIndex(),
                        this.msrSortModel.getSortOrder());
        compratorList.add(measureComparator);
        this.comparatorChain = new ComparatorChain(compratorList);
    }

    /**
     * Below method will be used to create the heap holder
     */
    private void createHeap() {
        this.dataHeap = new PriorityQueue<Tuple>(this.holderSize, this.comparatorChain);
    }

    /**
     * Below method will be call to finish the processor
     *
     * @throws MolapPaginationException
     */
    @Override
    public void finish() throws MolapPaginationException {
        if (paginationEnabled) {
            writeData();
            List compratorList = new ArrayList(MolapCommonConstants.CONSTANT_SIZE_TEN);
            MaksedByteComparatorForDFCH keyComparator = null;
            if (this.msrSortModel.getSortOrder() < 2) {
                for (int i = 0; i < this.maskedByteRangeForSorting.length - 1; i++) {
                    keyComparator =
                            new MaksedByteComparatorForDFCH(this.maskedByteRangeForSorting[i],
                                    this.dimensionSortOrder[i], this.dimensionMasks[i]);
                    compratorList.add(keyComparator);
                }
            }
            MeasureComparatorDFCH measureComparator =
                    new MeasureComparatorDFCH(msrSortModel.getMeasureIndex(),
                            this.msrSortModel.getSortOrder());
            compratorList.add(measureComparator);
            ComparatorChain comparatorChain = new ComparatorChain(compratorList);
            DataFileMerger dataFileMerger =
                    new DataFileMerger(model, comparatorChain, dataProcessor, interFileLocation);
            try {
                dataFileMerger.call();
            } catch (Exception e) {
                throw new MolapPaginationException(e);
            }
        } else {
            //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_003
            int size = dataHeap.size();
            if (msrSortModel.isBreakHeir()) {
                List compratorList = new ArrayList();
                MaksedByteComparatorForTuple keyComparator = null;
                for (int i = 0; i < this.maskedByteRangeForSorting.length - 1; i++) {
                    keyComparator =
                            new MaksedByteComparatorForTuple(this.maskedByteRangeForSorting[i],
                                    this.dimensionSortOrder[i], this.dimensionMasks[i]);
                    compratorList.add(keyComparator);
                }
                MeasureComparatorTuple measureComparator =
                        new MeasureComparatorTuple(msrSortModel.getMeasureIndex(),
                                this.msrSortModel.getSortOrder());
                compratorList.add(measureComparator);
                ComparatorChain comparatorChain = new ComparatorChain(compratorList);

                List<Tuple> tupleList = new ArrayList<Tuple>(dataHeap);

                Collections.sort(tupleList, comparatorChain);

                for (Tuple tuple : tupleList) {
                    dataProcessor.processRow(tuple.getKey(), tuple.getMeasures());
                }
                // CHECKSTYLE:ON
            } else {
                for (int j = 0; j < size; j++) {
                    Tuple tuple = dataHeap.poll();
                    dataProcessor.processRow(tuple.getKey(), tuple.getMeasures());
                }
            }

            dataProcessor.finish();
        }
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder) {
        // No need to implement any thing.

    }
}
