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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.executer.pagination.GlobalPaginatedAggregator;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;

/**
 * Class Description : scan the data from store and aggregate
 * Class Version 1.0
 */
public class LocalDataAggregatorForAutoAggregateImpl extends LocalDataAggregatorImpl {

    /**
     * customMeasureIndex
     */
    private int[] customMeasureIndex;

    public LocalDataAggregatorForAutoAggregateImpl(SliceExecutionInfo info,
            GlobalPaginatedAggregator paginatedAggregator, int rowLimit, String id) {
        super(info, paginatedAggregator, rowLimit, id);
        generator = info.getFactKeyGenerator();
        customMeasureIndex = getCustomMeasureIndex();
        if (aggTable) {
            otherMsrIndexes = getOtherMsrIndexesWithOutCustomMeasureAndAverageMeasure();
        } else {
            otherMsrIndexes = getOtherMsrIndexesWithOutCustomMeasure();
        }
    }

    private int[] getCustomMeasureIndex() {
        List<Integer> list = new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < queryMsrs.length; i++) {
            if (queryMsrs[i].getAggName().equals(MolapCommonConstants.CUSTOM) || queryMsrs[i]
                    .getAggName().equals(MolapCommonConstants.DISTINCT_COUNT)) {
                list.add(i);
            }
        }
        return QueryExecutorUtil.convertIntegerListToIntArray(list);
    }

    /**
     * aggregateMsrs
     *
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrs(KeyValue available, MeasureAggregator[] currentMsrRowData) {
        if (aggTable) {
            aggregateMsrsForAggTable(available, currentMsrRowData);
            return;
        }
        Object msrValue/* = 0.0*/;
        for (int i = 0; i < otherMsrIndexes.length; i++) {
            msrValue = available.getValue(measureOrdinal[otherMsrIndexes[i]],
                    queryMsrs[otherMsrIndexes[i]].getDataType());
            if (!uniqueValues[measureOrdinal[otherMsrIndexes[i]]].equals(msrValue)) {
                currentMsrRowData[otherMsrIndexes[i]].agg(msrValue);
            }
        }
        byte[] byteValue = null;
        for (int i = 0; i < customMeasureIndex.length; i++) {
            byteValue = available.getByteArrayValue(measureOrdinal[customMeasureIndex[i]]);
            currentMsrRowData[customMeasureIndex[i]].agg(byteValue);
        }
    }

    private int[] getOtherMsrIndexesWithOutCustomMeasureAndAverageMeasure() {
        int[] indexes =
                new int[queryMsrs.length - (avgMsrIndexes.length + customMeasureIndex.length)];
        int k = 0;
        for (int i = 0; i < queryMsrs.length; i++) {
            if (Arrays.binarySearch(avgMsrIndexes, i) < 0
                    && Arrays.binarySearch(customMeasureIndex, i) < 0) {
                indexes[k++] = i;
            }
        }
        return indexes;
    }

    private int[] getOtherMsrIndexesWithOutCustomMeasure() {
        int[] indexes = new int[queryMsrs.length - (customMeasureIndex.length)];
        int k = 0;
        for (int i = 0; i < queryMsrs.length; i++) {
            if (Arrays.binarySearch(customMeasureIndex, i) < 0) {
                indexes[k++] = i;
            }
        }
        return indexes;
    }

    /**
     * aggregateMsrs
     *
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrsForAggTable(KeyValue available,
            MeasureAggregator[] currentMsrRowData) {

        Object avgValue/*= 0.0*/;
        for (int i = 0; i < avgMsrIndexes.length; i++) {
            avgValue = available.getValue(measureOrdinal[avgMsrIndexes[i]],
                    queryMsrs[avgMsrIndexes[i]].getDataType());
            if (uniqueValues[measureOrdinal[avgMsrIndexes[i]]] != avgValue) {
                currentMsrRowData[avgMsrIndexes[i]]
                        .agg(available.getMsrData(measureOrdinal[avgMsrIndexes[i]]),
                                available.getRow());
            }
        }
        Object otherValue/*= 0.0*/;
        for (int i = 0; i < otherMsrIndexes.length; i++) {
            otherValue = available.getValue(measureOrdinal[otherMsrIndexes[i]],
                    queryMsrs[otherMsrIndexes[i]].getDataType());
            if (uniqueValues[measureOrdinal[otherMsrIndexes[i]]] != otherValue) {
                currentMsrRowData[otherMsrIndexes[i]].agg(otherValue);
            }
        }
        byte[] byteValue = null;
        for (int i = 0; i < customMeasureIndex.length; i++) {
            byteValue = available.getByteArrayValue(measureOrdinal[customMeasureIndex[i]]);
            currentMsrRowData[customMeasureIndex[i]].agg(byteValue);
        }
    }
}
