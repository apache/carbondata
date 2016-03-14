package com.huawei.unibi.molap.engine.processor;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.result.iterator.MemoryBasedResultIterator;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class MemoryBasedLimitProcessor implements DataProcessorExt
{
    private int limit;
    
    private QueryResult result;
    
    
    public MemoryBasedLimitProcessor()
    {
        result = new QueryResult();
    }
    
    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException
    {
        limit=model.getLimit();
    }



    @Override
    public void finish() throws DataProcessorException
    {
        
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator()
    {
        return new MemoryBasedResultIterator(result);
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException
    {

        if(limit == -1 || result.size() < limit)
        {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key);
            result.add(arrayWrapper, value);
        }
        
    }
    
    /**
     * While processing the row the direct surrogate key values will be added directly as byte[] to
     * ByteArrayWrapper instance, Internally list will be maintaned in each ByteArrayWrapper instance
     * inorder to hold the direct surrogate key value for different surrogate keys.
     */
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException
    {
        if(limit == -1 || result.size() < limit)
        {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key.getMaskedKey());
            List<byte []> listOfDirectKey=key.getDirectSurrogateKeyList();
            if(null!=listOfDirectKey)
            {
                for(byte[] byteArray:listOfDirectKey)
                {
                    arrayWrapper.addToDirectSurrogateKeyList(byteArray);
                }
            }
            List<byte []> listOfComplexTypes=key.getCompleteComplexTypeData();
            if(null!=listOfComplexTypes)
            {
                for(byte[] byteArray:listOfComplexTypes)
                {
                    arrayWrapper.addComplexTypeData(byteArray);
                }
            }
            result.add(arrayWrapper, value);
        }
        
    }

}
