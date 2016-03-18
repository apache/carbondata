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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7I3iayOmRUeVCsS1yhcczDqCuM61JQReZj2OkIHOBIvDplJTBTelC2/p0QCa9qzV04H9
Pt6geBS02++m9pB7M+0SartgD05XkxBG3vdGj92XEJ02IwvwNjhzNoxJQVAG8w==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */

package com.huawei.unibi.molap.engine.executer.impl.measure.filter;

import java.util.Arrays;
import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.impl.topn.TopNProcessorBytes;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;

import com.huawei.unibi.molap.engine.filters.measurefilter.MeasureFilter;
import com.huawei.unibi.molap.engine.filters.measurefilter.util.MeasureFilterFactory;



/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :MeasureFilterProcessor.java
 * Class Description : Measure Filter Processor to filter tuple based on measure filter condition  
 * Version 1.0
 */
public class MeasureFilterProcessor implements DataProcessor
{

       
    /**
     * measureFilters
     */
    private MeasureFilter [] measureFilters;
    
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
    public MeasureFilterProcessor(DataProcessor dataProcessor,boolean afterTopN)
    {
        this.dataProcessor=dataProcessor;
        this.afterTopN = afterTopN;
    }
    
    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {

        this.dataProcessor.initModel(model);
        measureFilters = MeasureFilterFactory.getFilterMeasures(afterTopN?model.getMsrConstraintsAfterTopN():model.getMsrConstraints(),Arrays.asList(model.getQueryMsrs()));
        if(dataProcessor instanceof TopNProcessorBytes)
        {
            passGroupBy = true;
        }
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
        if(filterMeasure(measures))
        {
            this.dataProcessor.processRow(key, measures);
        }
    }

    private boolean filterMeasure(MeasureAggregator[] measures)
    {
        for(int k = 0;k < measureFilters.length;k++)
        {
            if(!(measureFilters[k].filter(measures)))
            {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public void finish() throws MolapPaginationException
    {
        this.dataProcessor.finish();
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder) throws MolapPaginationException
    {
        if(filterMeasure(groupByHolder.getMeasureAggregators()))
        {
            if(passGroupBy)
            {
                dataProcessor.processGroup(groupByHolder);
            }
            else
            {
                List<byte[]> rows = groupByHolder.getRows();
                List<MeasureAggregator[]> msrs = groupByHolder.getMsrs();

                for(int i = 0;i < rows.size();i++)
                {
                    this.dataProcessor.processRow(rows.get(i), msrs.get(i));
                }
            }
        }
        
    }

}
