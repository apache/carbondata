package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public class QueryResult
{
    private List<ByteArrayWrapper> keys;
    
    private List<MeasureAggregator[]> values;
    
    private int rsize;
    
    public QueryResult()
    {
        keys = new ArrayList<ByteArrayWrapper>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        values = new ArrayList<MeasureAggregator[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    }
    
    public void add(ByteArrayWrapper key, MeasureAggregator[] value)
    {
        keys.add(key);
        values.add(value);
        rsize++;
    }
    
    public int size()
    {
        return rsize;
    }
    
    public QueryResultIterator iterator()
    {
        return new QueryResultIterator();
    }
    
    public void prepareResult(KeyValueHolder[] data)
    {
        for(int i = 0;i < data.length;i++)
        {
            keys.add(data[i].key);
            values.add(data[i].value);
        }
        rsize=keys.size();
    }
    
    public class QueryResultIterator
    {
        private int counter=-1;
        
        public boolean hasNext()
        {
            ++counter;
            return counter<rsize;
        }
        
        public ByteArrayWrapper getKey()
        {
            return keys.get(counter);
        }
        
        public MeasureAggregator[] getValue()
        {
            return values.get(counter);
        }
    }

    
}
