/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d2niHu8823kF8JhAmZyTMNQqDNIQY0cUNnPQdminbkUec7xNPmwdzdfOcomcZsm0CBSz
vZZGZiHwcSaLrRr6gAyT6AEG4QX9G9gs9g/EsV04tdnVlv9++8r98R0OxbSqNw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * This class is used in case pagination is enabled. It just collects the data and form one map.
 * @author R00900208
 *
 */
public class MolapQueryDummyDataWriterProcessor implements DataProcessor
{

    private Map<ByteArrayWrapper, MeasureAggregator[]> map = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>();
    
    private int limit = -1;
    
    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {
        limit = model.getLimit();
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
        if(limit == -1 || map.size() < limit)
        {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key);
            map.put(arrayWrapper, measures);
        }
    }

    @Override
    public void finish() throws MolapPaginationException
    {

        
    }
    
    public  Map<ByteArrayWrapper, MeasureAggregator[]> getData()
    {
        return map;
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
