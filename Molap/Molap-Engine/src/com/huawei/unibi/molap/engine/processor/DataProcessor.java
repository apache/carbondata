package com.huawei.unibi.molap.engine.processor;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;


public interface DataProcessor
{
    void initialise(DataProcessorInfo model) throws DataProcessorException;

    void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException;
    
    //void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException;

    void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException;

    void finish() throws DataProcessorException;

    MolapIterator<QueryResult> getQueryResultIterator();

}
