package com.huawei.unibi.molap.engine.processor.row;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.processor.DataProcessorExt;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.util.ByteUtil;

public class AggreagtedRowProcessor extends RowProcessor
{
    public AggreagtedRowProcessor(DataProcessorExt dataProcessor)
    {
        super(dataProcessor);
    }

    /**
     * prevMsrs
     */
    private MeasureAggregator[] prevMsrs;

    /**
     * prevKey
     */
    private byte[] prevKey;
    
    @Override
    public void processRow(final byte[] key, final MeasureAggregator[] value) throws DataProcessorException
    {
        if(prevKey != null)
        {
            if(ByteUtil.compare(key, prevKey) == 0)
            {
                aggregateData(prevMsrs, value);
            }
            else
            {
                dataProcessor.processRow(key, value);
            }
        }
        prevKey = key.clone();
        prevMsrs = value;
    }
    
    private void aggregateData(final MeasureAggregator[] src, final MeasureAggregator[] dest)
    {
        for(int i = 0;i < dest.length;i++)
        {
            dest[i].merge(src[i]);
        }
    }
    
    public void finish() throws DataProcessorException
    {
        dataProcessor.processRow(prevKey, prevMsrs);
        super.finish();
    }
}
