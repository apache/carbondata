/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOSFjEpYAA2/czV4L9IGlVg6k439RXUvBxeW2CNTdfzVEkoPcyLFK8GNsTDznCwL6k94j
GxM9V/tch37QKCtPXwoe8lyrAwFgvS9DfOQvau+Q47fiZve5y1/b1O6tCCPxQA==*/
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
package com.huawei.unibi.molap.engine.executer;

import java.util.Map;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP Engine
 * Created Date :16-May-2013 9:47:04 PM
 * FileName : ParallelSliceExecutor.java
 * Class Description : This class executes the query for slice and return the
 * result map.
 *  
 * Version 1.0
 */
public interface ParallelSliceExecutor
{
    /**
     * Execute the slice based on ranges
     * 
     * @param ranges
     * @return the resultMap
     * @throws Exception
     * 
     */
    Map<ByteArrayWrapper, MeasureAggregator[]> executeSliceInParallel() throws Exception;
    
    /**
     * Execute the slice based on ranges
     * 
     * @param ranges
     * @return the resultMap
     * @throws Exception
     * 
     */
    QueryResult executeSlices() throws Exception;
    
    /**
     * It interrupts the executor.  
     */
    void interruptExecutor();
}
