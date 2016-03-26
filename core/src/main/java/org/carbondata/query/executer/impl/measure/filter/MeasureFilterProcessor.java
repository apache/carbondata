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

package org.carbondata.query.executer.impl.measure.filter;

import java.util.Arrays;
import java.util.List;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.groupby.GroupByHolder;
import org.carbondata.query.executer.impl.topn.TopNProcessorBytes;
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.filters.measurefilter.MeasureFilter;
import org.carbondata.query.filters.measurefilter.util.MeasureFilterFactory;

/**
 * Class Description : Measure Filter Processor to filter tuple based on measure filter condition
 * Version 1.0
 */
public class MeasureFilterProcessor implements DataProcessor {

    /**
     * measureFilters
     */
    private MeasureFilter[] measureFilters;

    /**
     * dataProcessor
     */
    private DataProcessor dataProcessor;

    private boolean passGroupBy;

    private boolean afterTopN;

    /**
     * MeasureFilterProcessor Constructor
     *
     * @param dataProcessor
     */
    public MeasureFilterProcessor(DataProcessor dataProcessor, boolean afterTopN) {
        this.dataProcessor = dataProcessor;
        this.afterTopN = afterTopN;
    }

    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException {

        this.dataProcessor.initModel(model);
        measureFilters = MeasureFilterFactory.getFilterMeasures(
                afterTopN ? model.getMsrConstraintsAfterTopN() : model.getMsrConstraints(),
                Arrays.asList(model.getQueryMsrs()));
        if (dataProcessor instanceof TopNProcessorBytes) {
            passGroupBy = true;
        }
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures)
            throws MolapPaginationException {
        if (filterMeasure(measures)) {
            this.dataProcessor.processRow(key, measures);
        }
    }

    private boolean filterMeasure(MeasureAggregator[] measures) {
        for (int k = 0; k < measureFilters.length; k++) {
            if (!(measureFilters[k].filter(measures))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void finish() throws MolapPaginationException {
        this.dataProcessor.finish();
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder) throws MolapPaginationException {
        if (filterMeasure(groupByHolder.getMeasureAggregators())) {
            if (passGroupBy) {
                dataProcessor.processGroup(groupByHolder);
            } else {
                List<byte[]> rows = groupByHolder.getRows();
                List<MeasureAggregator[]> msrs = groupByHolder.getMsrs();

                for (int i = 0; i < rows.size(); i++) {
                    this.dataProcessor.processRow(rows.get(i), msrs.get(i));
                }
            }
        }

    }

}
