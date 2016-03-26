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

package org.carbondata.query.executer.impl.dimension;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.groupby.GroupByHolder;
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.util.MolapEngineLogEvent;

/**
 * It filters the data as per the post topN filters
 */
public class DimensionFilterProcessor implements DataProcessor {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DimensionFilterProcessor.class.getName());

    @Override public void initModel(PaginationModel model) throws MolapPaginationException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void processRow(byte[] key, MeasureAggregator[] measures)
            throws MolapPaginationException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void processGroup(GroupByHolder groupByHolder)
            throws MolapPaginationException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void finish() throws MolapPaginationException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    //    /**
    //     * dataProcessor
    //     */
    //    private DataProcessor dataProcessor;
    //
    //    private KeyFilterImpl filterImpl;
    //
    //    private KeyGenerator keyGenerator;
    //
    //    private int[] maskedBytePos;
    //
    //    private InMemoryCube[] slices;
    //
    //    private int[] dimensions;
    //
    //    private String[] colNames;
    //
    //
    //    public DimensionFilterProcessor(DataProcessor dataProcessor)
    //    {
    //        this.dataProcessor =dataProcessor;
    //    }
    //
    //    @Override
    //    public void initModel(PaginationModel model) throws MolapPaginationException
    //    {
    //
    //        keyGenerator = model.getKeyGenerator();
    //        Map<Dimension, MolapFilterInfo> constraintsAfterTopN = model.getConstraintsAfterTopN();
    //        filterImpl = new KeyFilterImpl(model.getFilterModelAfterTopN(), keyGenerator,
    //                new long[keyGenerator.getDimCount()]);
    //
    //        if(CacheUtil.checkAnyIncludeOrExists(model.getConstraintsAfterTopN()))
    //        {
    //
    //            filterImpl = new IncludeOrKeyFilterImpl(model.getFilterModelAfterTopN(),keyGenerator, null);
    //        }
    //        else if(CacheUtil.checkAnyExcludeExists(model.getConstraintsAfterTopN()))
    //        {
    //            filterImpl = new IncludeExcludeKeyFilterImpl(model.getFilterModelAfterTopN(), keyGenerator,
    //                    new long[keyGenerator.getDimCount()]);
    //        }
    //        //
    //        else if(CacheUtil.checkAnyIncludeExists(model.getConstraintsAfterTopN()))
    //        {
    //            filterImpl = new KeyFilterImpl(model.getFilterModelAfterTopN(), keyGenerator,
    //                    new long[keyGenerator.getDimCount()]);
    //        }
    //        maskedBytePos = model.getMaskedByteRange();
    //        slices = model.getSlices().toArray(new InMemoryCube[model.getSlices().size()]);
    //        extractDimensionsFromFilters(constraintsAfterTopN);
    //        dataProcessor.initModel(model);
    //    }
    //
    //    /**
    //     * Process each row and filter
    //     */
    //    @Override
    //    public void processRow(final byte[] key, final MeasureAggregator[] measures) throws MolapPaginationException
    //    {
    //
    //        if(filterImpl.filterKey(getByteArray(key)))
    //        {
    //            dataProcessor.processRow(key, measures);
    //        }
    //
    //    }
    //
    //    private byte[] getByteArray(final byte[] maskKey) throws MolapPaginationException
    //    {
    //
    //        long[] keyArray = keyGenerator.getKeyArray(maskKey, maskedBytePos);
    //        for(int i = 0;i < dimensions.length;i++)
    //        {
    //          //CHECKSTYLE:OFF    Approval No:Approval-358
    //            keyArray[dimensions[i]] = getActualSurrogateKeyFromSortedIndex(colNames[i],
    //                    (int)keyArray[dimensions[i]],slices);
    //          //CHECKSTYLE:ON
    //        }
    //        try
    //        {
    //            return keyGenerator.generateKey(keyArray);
    //        }
    //        catch(KeyGenException e)
    //        {
    //            throw new MolapPaginationException(e);
    //        }
    //
    //    }
    //
    //    /**
    //     * Below method will be used to get the sor index
    //     * @param columnName
    //     * @param id
    //     * @return sort index
    //     */
    //    private static int getActualSurrogateKeyFromSortedIndex(String columnName, int id,InMemoryCube[] slices)
    //    {
    //        for(InMemoryCube slice : slices)
    //        {
    //            int index = slice.getMemberCache(columnName).getActualSurrogateKeyFromSortedIndex(id);
    //            if(index != -MolapCommonConstants.DIMENSION_DEFAULT)
    //            {
    //                return index;
    //            }
    //        }
    //        return -MolapCommonConstants.DIMENSION_DEFAULT;
    //    }
    //
    //
    //    private void extractDimensionsFromFilters(Map<Dimension, MolapFilterInfo> constraintsAfterTopN)
    //    {
    //        dimensions = new int[constraintsAfterTopN.size()];
    //        colNames = new String[constraintsAfterTopN.size()];
    //
    //        int i = 0;
    //        for(Dimension dimension : constraintsAfterTopN.keySet())
    //        {
    //            dimensions[i] = dimension.getOrdinal();
    //            colNames[i] = dimension.getTableName()+'_'+dimension.getColName() + '_' + dimension.getDimName() + '_' + dimension.getHierName();
    //            i++;
    //        }
    //    }
    //
    //
    //    @Override
    //    public void processGroup(GroupByHolder groupByHolder) throws MolapPaginationException
    //    {
    //        // TODO Auto-generated method stub
    //
    //    }
    //
    //    @Override
    //    public void finish() throws MolapPaginationException
    //    {
    //        dataProcessor.finish();
    //
    //    }

}
