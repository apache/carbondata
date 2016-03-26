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

package org.carbondata.query.executer.groupby;

import java.util.Arrays;
import java.util.List;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.MolapMetadata.Measure;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.datastorage.InMemoryCube;
import org.carbondata.query.executer.calcexp.MolapCalcExpressionResolverUtil;
import org.carbondata.query.executer.calcexp.MolapCalcFunction;
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.schema.metadata.MeasureFilterProcessorModel;

/**
 * It will group by and aggregate measures by the dimension
 */
public class GroupByProcessor implements DataProcessor {

    /**
     * topMeasureIndex
     */
    private int topMeasureIndex;
    //    /**
    //     * topNCount
    //     */
    //    private int topNCount;
    //    /**
    //     * topNType
    //     */
    //    private MolapTopNType topNType;
    //
    //
    //    /**
    //     * groupMaskedBytes
    //     */
    //    private byte[] groupMaskedBytes;

    /**
     * maskedBytes
     */
    private byte[] maskedBytes;

    /**
     *
     */
    private GroupByHolder holder;

    /**
     * avgMsrIndex
     */
    private int avgMsrIndex;

    /**
     * aggName
     */
    private String aggName;

    /**
     * countMsrIndex
     */
    private int countMsrIndex;

    /**
     *
     */
    private DataProcessor processor;

    /**
     *
     */
    private boolean isCalculatedMsr;

    /**
     * Query measures
     */
    private Measure[] queryMsrs;

    /**
     * Calc function
     */
    private MolapCalcFunction calcFunction;

    /**
     * maskedBytesPos
     */
    private int[] maskedBytesPos;

    private MeasureFilterProcessorModel msrFilterProcessorModel;

    private KeyGenerator keyGenerator;

    private String cubeUniqueName;

    /**
     * @param dimIndexes
     * @param topMeasureIndex
     * @param topNCount
     * @param topNType
     */
    public GroupByProcessor(DataProcessor processor) {
        this.processor = processor;

    }

    public GroupByProcessor(DataProcessor processor,
            MeasureFilterProcessorModel msrFilterProcessorModel) {
        this.processor = processor;
        this.msrFilterProcessorModel = msrFilterProcessorModel;
    }

    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException {
        //        this.groupMaskedBytes = model.getGroupMaskedBytes();
        if (null != msrFilterProcessorModel) {
            this.maskedBytes = msrFilterProcessorModel.getMaskedBytes();
            this.maskedBytesPos = msrFilterProcessorModel.getMaskedBytesPos();

        } else {
            this.maskedBytes = model.getMaskedBytes();
            this.maskedBytesPos = model.getTopNMaskedBytesPos();
        }
        this.topMeasureIndex = model.getTopMeasureIndex();
        //        this.topNCount = model.getTopNCount();
        //        this.topNType = model.getTopNType();
        this.avgMsrIndex = model.getAvgMsrIndex();
        this.countMsrIndex = model.getCountMsrIndex();
        this.aggName = model.getAggName();
        this.isCalculatedMsr = model.isTopCountOnCalcMeasure();
        this.queryMsrs = model.getQueryMsrs();

        if (isCalculatedMsr) {
            int calcMsrIndex = topMeasureIndex - queryMsrs.length;
            calcFunction = MolapCalcExpressionResolverUtil
                    .createCalcExpressions(model.getCalculatedMeasures()[calcMsrIndex].getExp(),
                            Arrays.asList(queryMsrs));
        }
        if (countMsrIndex < 0) {
            countMsrIndex = 0;
        }
        if (topMeasureIndex < 0) {
            topMeasureIndex = 0;
        }
        keyGenerator = model.getKeyGenerator();
        List<InMemoryCube> slices = model.getSlices();
        if (null != slices && slices.size() > 0 && null != slices.get(0)) {
            cubeUniqueName = slices.get(0).getCubeUniqueName();
        }
        holder =
                new GroupByHolder(maskedBytes, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex,
                        isCalculatedMsr, queryMsrs, calcFunction, maskedBytesPos, keyGenerator,
                        cubeUniqueName);
        if (processor != null) {
            processor.initModel(model);
        }
    }

    /**
     * Add row to processor.
     *
     * @param row
     * @param aggregators
     * @throws MolapPaginationException
     */
    private void addRow(byte[] row, MeasureAggregator[] aggregators)
            throws MolapPaginationException {
        if (!holder.addRow(row, aggregators)) {
            processor.processGroup(holder);
            holder = new GroupByHolder(maskedBytes, topMeasureIndex, aggName, countMsrIndex,
                    avgMsrIndex, isCalculatedMsr, queryMsrs, calcFunction, maskedBytesPos,
                    keyGenerator, cubeUniqueName);
            holder.addRow(row, aggregators);
        }
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures)
            throws MolapPaginationException {
        addRow(key, measures);
    }

    @Override
    public void finish() throws MolapPaginationException {
        if (holder.getRows().size() > 0) {
            processor.processGroup(holder);
        }

        processor.finish();
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder) {
        //No implementation is required.

    }
}
