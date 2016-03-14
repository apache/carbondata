package com.huawei.unibi.molap.engine.processor;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public interface DataProcessorExt extends DataProcessor
{

    /**
     * This interface will help the engine to process the data based on entire byte array object.
     * @param key
     * @param value
     * @throws DataProcessorException
     */
    void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException;
}
