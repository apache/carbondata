package com.huawei.unibi.molap.engine.processor;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class FileBasedLimitProcessor implements DataProcessorExt
{
    private DataProcessorExt processor;
    
    private int limit;
    
    private int counter;
    
    public FileBasedLimitProcessor(DataProcessorExt processor)
    {
        this.processor=processor;
    }
    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException
    {
        this.processor.initialise(model);
        this.limit=model.getLimit();
    }


    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException
    {
        if(limit==-1 || counter<limit)
        {
            processor.processRow(key, value);
            counter++;
        }
    }
    
    @Override
    public void finish() throws DataProcessorException
    {
        processor.finish();
        
    }
    @Override
    public MolapIterator<QueryResult> getQueryResultIterator()
    {
        // TODO Auto-generated method stub
        return processor.getQueryResultIterator();
    }
    
    @Override
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException
    {
        if(limit==-1 || counter<limit)
        {
            processor.processRow(key, value);
            counter++;
        }
        
    }

}
