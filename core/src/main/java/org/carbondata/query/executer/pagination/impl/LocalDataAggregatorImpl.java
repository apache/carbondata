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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.MolapMetadata.Measure;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.query.datastorage.InMemoryCube;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.executer.pagination.DataAggregator;
import org.carbondata.query.executer.pagination.GlobalPaginatedAggregator;
import org.carbondata.query.scanner.Scanner;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.carbondata.query.wrappers.ByteArrayWrapper;

//import mondrian.olap.ResourceLimitExceededException;
//import org.carbondata.core.engine.aggregator.util.AggUtil;

/**
 * It scans the data from store and aggregates the data.
 */
public class LocalDataAggregatorImpl implements DataAggregator {

    /**
     * LOGGER.
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LocalDataAggregatorImpl.class.getName());
    /**
     *
     */
    protected byte[] maxKeyBasedOnDim;
    /**
     *
     */
    protected int[] maskedByteRanges;
    /**
     *
     */
    protected int maskedKeyByteSize;
    /**
     * Measures
     */
    protected Measure[] queryMsrs;
    /**
     * Key generator
     */
    protected KeyGenerator generator;
    /**
     * slice
     */
    protected InMemoryCube slice;
    /**
     * Row counter to interrupt;
     */
    protected int rowLimit;
    /**
     * Internal row counter;
     */
    protected int counter;
    /**
     * Unique values represents null values of measure.
     */
    protected Object[] uniqueValues;
    protected ByteArrayWrapper dimensionsRowWrapper = new ByteArrayWrapper();
    /**
     * Measures ordinal
     */
    protected int[] measureOrdinal;
    /**
     * Data Map
     */
    protected Map<ByteArrayWrapper, MeasureAggregator[]> data;
    /**
     * paginatedAggregator
     */
    protected GlobalPaginatedAggregator paginatedAggregator;
    protected SliceExecutionInfo info;
    /**
     * msrExists
     */
    protected boolean[] msrExists;
    /**
     * msrDft
     */
    protected Object[] msrDft;
    protected boolean interrupted;

    /**
     * ID
     */
    protected String id;

    protected long rowCount;

    protected int[] avgMsrIndexes;

    protected int countMsrIndex;

    protected boolean aggTable;

    protected int[] otherMsrIndexes;

    protected int limit;

    protected boolean detailQuery;

    public LocalDataAggregatorImpl(SliceExecutionInfo info,
            GlobalPaginatedAggregator paginatedAggregator, int rowLimit, String id) {
        //        queryMsrs = info.getQueryMsrs();
        measureOrdinal = info.getMeasureOrdinal();
        //        maxKeyBasedOnDim = info.getMaxKeyBasedOnDimensions();
        //        maskedByteRanges = info.getMaskedByteRanges();
        maskedKeyByteSize = info.getMaskedKeyByteSize();
        this.generator = info.getKeyGenerator();
        this.slice = info.getSlice();
        //        this.msrExists = info.getMsrsExists();
        //        this.msrDft = info.getNewMeasureDftValue();
        this.uniqueValues = info.getUniqueValues();
        this.avgMsrIndexes = QueryExecutorUtil.convertIntegerListToIntArray(info.getAvgIndexes());
        Arrays.sort(avgMsrIndexes);
        this.countMsrIndex = info.getCountMsrsIndex();
        aggTable = countMsrIndex > -1;
        if (aggTable) {
            otherMsrIndexes = getOtherMsrIndexes();
        }
        String mapSize =
                MolapProperties.getInstance().getProperty("molap.intial.agg.mapsize", "5000");
        int size = Integer.parseInt(mapSize);
        data = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(size);
        this.paginatedAggregator = paginatedAggregator;
        this.info = info;
        this.rowLimit = rowLimit;
        this.id = id;
        this.limit = info.getLimit();
        this.detailQuery = info.isDetailQuery();
        if (detailQuery && limit > 0) {
            this.rowLimit = limit;
        }
    }

    /* (non-Javadoc)
     * @see com.huawei.unibi.molap.engine.executer.pagination.DataAggregator#aggregate(byte[], com.huawei.unibi.molap.engine.aggregator.MeasureAggregator[])
     */
    @Override
    public void aggregate(Scanner scanner) {
        long startTime = System.currentTimeMillis();
        try {
            XXHash32 xxHash32 = null;
            boolean useXXHASH = Boolean.valueOf(
                    MolapProperties.getInstance().getProperty("molap.enableXXHash", "false"));
            if (useXXHASH) {
                xxHash32 = XXHashFactory.unsafeInstance().hash32();
            }
            dimensionsRowWrapper = new ByteArrayWrapper(xxHash32);
            //            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "XXXXXXXXXXXXXX  Using XXHash32 ");

            // for aggregating without decomposition using masking

            int count = 0;
            // Search till end of the data
            while (!scanner.isDone() && !interrupted) {
                KeyValue available = scanner.getNext();
                // Set the data into the wrapper
                dimensionsRowWrapper
                        .setData(available.backKeyArray, available.keyOffset, maxKeyBasedOnDim,
                                maskedByteRanges, maskedKeyByteSize);

                // 2) Extract required measures
                MeasureAggregator[] currentMsrRowData = data.get(dimensionsRowWrapper);

                if (currentMsrRowData == null) {
                    //                    currentMsrRowData = AggUtil.getAggregators(queryMsrs, aggTable, generator, slice.getCubeUniqueName());
                    //                    dimensionsRowWrapper.setActualData(available.getBackKeyArray(), available.getKeyOffset(), available.getKeyLength());
                    data.put(dimensionsRowWrapper, currentMsrRowData);
                    dimensionsRowWrapper = new ByteArrayWrapper(xxHash32);
                    counter++;
                    if (counter > rowLimit) {
                        aggregateMsrs(available, currentMsrRowData);
                        rowLimitExceeded();
                        counter = 0;
                        count++;
                        continue;
                    }
                }
                aggregateMsrs(available, currentMsrRowData);
                count++;
            }
            rowCount += data.size();

            //            Entry[] table;
            if (data.size() > 0) {
                Field field = HashMap.class.getDeclaredField("table");
                field.setAccessible(true);
                Object[] table = (Object[]) field.get(data);
                int counter = 0;
                for (Object entry : table) {
                    if (entry != null) {
                        counter++;
                    }
                }
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "YYYYYYYYYYY  Using Non Empty entries:  " + counter);
            }

            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Time taken for scan for range " + id + " : " + (System.currentTimeMillis()
                            - startTime) + " && Scanned Count : " + count + "  && Map Size : "
                            + rowCount);

            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "CUBE: " + info.getCubeName() + ", Time taken for scan : " + (
                            System.currentTimeMillis() - startTime));

            finish();
        }

        //        catch (ResourceLimitExceededException e)
        //        {
        //            throw e;
        //        }
        catch (Exception e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        }

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
        for (int i = 0; i < queryMsrs.length; i++) {
            Object value = available
                    .getValue(measureOrdinal[i], queryMsrs[measureOrdinal[i]].getDataType());
            if (!uniqueValues[measureOrdinal[i]].equals(value)) {
                currentMsrRowData[i].agg(value);
            }
        }
    }

    private int[] getOtherMsrIndexes() {
        int[] indexes = new int[queryMsrs.length - (avgMsrIndexes.length)];
        int k = 0;
        for (int i = 0; i < queryMsrs.length; i++) {
            if (Arrays.binarySearch(avgMsrIndexes, i) < 0) {
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

        for (int i = 0; i < avgMsrIndexes.length; i++) {
            Object value = available.getValue(measureOrdinal[avgMsrIndexes[i]],
                    queryMsrs[measureOrdinal[avgMsrIndexes[i]]].getDataType());
            if (!uniqueValues[measureOrdinal[avgMsrIndexes[i]]].equals(value)) {
                currentMsrRowData[avgMsrIndexes[i]]
                        .agg(available.getMsrData(measureOrdinal[avgMsrIndexes[i]]),
                                available.getRow());
            }
        }

        for (int i = 0; i < otherMsrIndexes.length; i++) {
            Object value = available.getValue(measureOrdinal[otherMsrIndexes[i]],
                    queryMsrs[measureOrdinal[otherMsrIndexes[i]]].getDataType());
            if (!uniqueValues[measureOrdinal[otherMsrIndexes[i]]].equals(value)) {
                currentMsrRowData[otherMsrIndexes[i]].agg(value);
            }
        }
    }

    public void rowLimitExceeded() throws Exception {
        rowCount += data.size();
        paginatedAggregator.writeToDisk(data, info.getRestructureHolder());
        data = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(16);
        if (detailQuery && limit > 0) {
            if (rowCount > limit) {
                interrupt();
            }
        }
    }

    @Override
    public void finish() throws Exception {

        paginatedAggregator.writeToDisk(data, info.getRestructureHolder());

    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

}
