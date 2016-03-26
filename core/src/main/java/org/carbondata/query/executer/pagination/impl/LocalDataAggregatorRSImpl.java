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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.executer.pagination.GlobalPaginatedAggregator;
import org.carbondata.query.scanner.Scanner;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.carbondata.query.wrappers.ByteArrayWrapper;

//import org.carbondata.core.engine.aggregator.util.AggUtil;

public class LocalDataAggregatorRSImpl extends LocalDataAggregatorImpl {

    /**
     * LOGGER.
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LocalDataAggregatorRSImpl.class.getName());

    public LocalDataAggregatorRSImpl(SliceExecutionInfo info,
            GlobalPaginatedAggregator paginatedAggregator, int rowLimit, String id) {
        super(info, paginatedAggregator, rowLimit, id);
    }

    /* (non-Javadoc)
     * @see com.huawei.unibi.molap.engine.executer.pagination.DataAggregator#aggregate(byte[], com.huawei.unibi.molap.engine.aggregator.MeasureAggregator[])
     */
    @Override
    public void aggregate(Scanner scanner) {
        long startTime = System.currentTimeMillis();
        try {

            // for aggregating without decomposition using masking

            int count = 0;
            // Search till end of the data
            while (!scanner.isDone() && !interrupted) {
                KeyValue available = scanner.getNext();
                // Set the data into the wrapper
                dimensionsRowWrapper.setData(available.getBackKeyArray(), available.getKeyOffset(),
                        maxKeyBasedOnDim, maskedByteRanges, maskedKeyByteSize);

                // 2) Extract required measures
                MeasureAggregator[] currentMsrRowData = data.get(dimensionsRowWrapper);

                if (currentMsrRowData == null) {
                    //                    currentMsrRowData = AggUtil.getAggregators(queryMsrs, aggTable, generator, slice.getCubeUniqueName());
                    // dimensionsRowWrapper.setActualData(available.getBackKeyArray(),
                    // available.getKeyOffset(), available.getKeyLength());
                    data.put(dimensionsRowWrapper, currentMsrRowData);
                    dimensionsRowWrapper = new ByteArrayWrapper();
                    counter++;
                    if (counter > rowLimit) {
                        aggregateMsrsRS(available, currentMsrRowData);
                        rowLimitExceeded();
                        counter = 0;
                        count++;
                        continue;
                    }
                }
                aggregateMsrsRS(available, currentMsrRowData);
                count++;
            }

            finish();
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Time taken for scan for range " + id + " : " + (System.currentTimeMillis()
                            - startTime) + " && Scanned Count : " + count + "  && Map Size : "
                            + rowCount);
        } catch (Exception e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
        }
    }

    /**
     * @param available
     * @param currentMsrRowData
     */
    private void aggregateMsrsRS(KeyValue available, MeasureAggregator[] currentMsrRowData) {
        if (aggTable) {
            aggregateMsrsForAggTable(available, currentMsrRowData);
            return;
        }
        for (int i = 0; i < queryMsrs.length; i++) {
            Object value = msrExists[i] ?
                    available.getValue(measureOrdinal[i],
                            queryMsrs[measureOrdinal[i]].getDataType()) :
                    msrDft[i];
            if (msrExists[i] && !uniqueValues[measureOrdinal[i]].equals(value)) {
                currentMsrRowData[i].agg(value);
            } else if (!msrExists[i]) {
                currentMsrRowData[i].agg(value);
            }
        }
    }

    /**
     * aggregateMsrs
     *
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrsForAggTable(KeyValue available,
            MeasureAggregator[] currentMsrRowData) {

        for (int i = 0; i < avgMsrIndexes.length; i++) {
            int index = avgMsrIndexes[i];
            Object value = msrExists[i] ?
                    available.getValue(measureOrdinal[index],
                            queryMsrs[measureOrdinal[index]].getDataType()) :
                    msrDft[index];
            if (msrExists[index] && !uniqueValues[measureOrdinal[index]].equals(value)) {
                currentMsrRowData[index]
                        .agg(available.getMsrData(measureOrdinal[index]), available.getRow());
            } else if (!msrExists[index]) {
                currentMsrRowData[index]
                        .agg(available.getMsrData(measureOrdinal[index]), available.getRow());
            }
        }

        for (int i = 0; i < otherMsrIndexes.length; i++) {
            int index = otherMsrIndexes[i];
            Object value = msrExists[i] ?
                    available.getValue(measureOrdinal[index],
                            queryMsrs[measureOrdinal[index]].getDataType()) :
                    msrDft[index];
            if (msrExists[index] && !uniqueValues[measureOrdinal[index]].equals(value)) {
                currentMsrRowData[index].agg(value);
            } else if (!msrExists[index]) {
                currentMsrRowData[index].agg(value);
            }
        }
    }
}
