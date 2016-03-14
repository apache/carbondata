package com.huawei.unibi.molap.engine.executer.processor;

import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.iterator.MolapIterator;

public interface ScannedResultProcessor
{
    /**
     * Below method will be used to add the scanned result 
     * @param scannedResult
     *          scanned result
     * @param restructureHolder
     *          restructure holder will have the information about the slice 
     * @throws QueryExecutionException
     */
    void addScannedResult(Result scannedResult) throws QueryExecutionException;
    
    /**
     * Below method will be used to process the scanned result 
     * processing type:
     * 1. TopN
     * 2. Dim Column Sorting
     * 3. Measure column Sorting 
     * @throws QueryExecutionException
     */
    MolapIterator<QueryResult> getQueryResultIterator() throws QueryExecutionException;
}
