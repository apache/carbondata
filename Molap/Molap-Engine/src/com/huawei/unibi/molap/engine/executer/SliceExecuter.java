package com.huawei.unibi.molap.engine.executer;

import java.util.List;

import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.iterator.MolapIterator;

public interface SliceExecuter
{
    /**
     * Below method will be used to execute the slices in parallel and get the
     * processed query result from each slice lastesSlice
     * 
     * @return query result
     */
    MolapIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos, int[] sliceIndex) throws QueryExecutionException;
}
