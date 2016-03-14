package com.huawei.unibi.molap.engine.executer;

import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.iterator.MolapIterator;

public interface QueryExecutor
{
    
    /**
     * Below method will be used to execute the query 
     * 
     * @param queryModel
     *          query model, properties which will be required to execute the query 
     *          
     * @throws QueryExecutionException
     *          will throw query execution exception in case of any abnormal scenario 
     */
    MolapIterator<RowResult> execute(MolapQueryExecutorModel queryModel) throws QueryExecutionException;
    
    /**
     * Below method will be used to execute the query of QuickFilter 
     * 
     * @param queryModel
     *          , properties which will be required to execute the query 
     *          
     * @throws QueryExecutionException
     *          will throw query execution exception in case of any abnormal scenario 
     */    
    MolapIterator<RowResult> executeDimension(MolapQueryExecutorModel queryModel) throws QueryExecutionException;

}
