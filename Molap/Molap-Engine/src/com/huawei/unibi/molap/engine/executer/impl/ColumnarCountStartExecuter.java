package com.huawei.unibi.molap.engine.executer.impl;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CountAggregator;
import com.huawei.unibi.molap.engine.datastorage.CubeDataStore;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.SliceExecuter;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.iterator.MemoryBasedResultIterator;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ColumnarCountStartExecuter implements SliceExecuter
{
    private List<InMemoryCube> slices;
    
    private String tableName;
    
    public ColumnarCountStartExecuter(List<InMemoryCube> slices, String tableName)
    {
        this.slices=slices;
        this.tableName=tableName;
    }
    
    @Override
    public MolapIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos, int[] sliceIndex) throws QueryExecutionException
    {
        long count=0;
        for(InMemoryCube slice:slices)
        {
            CubeDataStore dataCache = slice.getDataCache(this.tableName);
            if(null!=dataCache)
            {
                count+=dataCache.getData().size();
            }
        }
        MeasureAggregator[] countAgg = new MeasureAggregator[1];
        countAgg[0]=new CountAggregator();
        countAgg[0].setNewValue(count);
        QueryResult result = new QueryResult();
        ByteArrayWrapper wrapper = new ByteArrayWrapper();
        wrapper.setMaskedKey(new byte[0]);
        result.add(wrapper, countAgg);
        return new MemoryBasedResultIterator(result);
    }

}
