package com.huawei.unibi.molap.engine.processor.row;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.processor.DataProcessorExt;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class RowProcessor implements DataProcessorExt
{

    protected DataProcessorExt dataProcessor;
    
    public RowProcessor(DataProcessorExt dataProcessor)
    {
        this.dataProcessor=dataProcessor;
    }
    
    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException
    {
        dataProcessor.initialise(model);
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException
    {
        dataProcessor.processRow(key, value);
    }
    
    @Override
    public void finish() throws DataProcessorException
    {
        dataProcessor.finish();
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator()
    {
        return dataProcessor.getQueryResultIterator();
    }

    @Override
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException
    {
        dataProcessor.processRow(key, value);
        
    }

}
