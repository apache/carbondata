package com.huawei.unibi.molap.engine.executer.pagination.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public class MolapQueryDummyProcessor implements DataProcessor
{
    
    private int limit = -1;
    
    private QueryResult result = new QueryResult();
    
    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {
        limit = model.getLimit();
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
        if(limit == -1 || result.size() < limit)
        {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key);
            result.add(arrayWrapper, measures);
        }
    }

    @Override
    public void finish() throws MolapPaginationException
    {

        
    }
    
    public QueryResult getData()
    {
        return result;
    }

    /**
     * processGroup
     */
    @Override
    public void processGroup(GroupByHolder groupByHolder)
    {
        // No need to implement any thing
        
    }
}