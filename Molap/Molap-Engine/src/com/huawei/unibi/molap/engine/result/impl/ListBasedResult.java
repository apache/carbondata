package com.huawei.unibi.molap.engine.result.impl;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public class ListBasedResult implements Result<List<ByteArrayWrapper>,List<MeasureAggregator[]>>
{
    private List<ByteArrayWrapper> keys;
    
    private List<List<ByteArrayWrapper>> allKeys;
    
    private List<List<MeasureAggregator[]>> allValues;
    
    private List<MeasureAggregator[]> values;
    
    private int counter=-1;
    
    private int listSize;
    
    private int currentCounter=-1;
    
    private int currentListCounter;
    
    public ListBasedResult()
    {
        keys = new ArrayList<ByteArrayWrapper>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        values = new ArrayList<MeasureAggregator[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        allKeys = new ArrayList<List<ByteArrayWrapper>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        allValues = new ArrayList<List<MeasureAggregator[]>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    @Override
    public void addScannedResult(List<ByteArrayWrapper> keys, List<MeasureAggregator[]> values)
    {
        this.keys=keys;
        this.values=values;
        listSize=keys.size();
    }

    @Override
    public boolean hasNext()
    {
        if(allKeys.size()==0)
        {
            return false;
        }
        counter++;
        currentCounter++;
        if(currentCounter==0 || (currentCounter>=keys.size() && currentListCounter<allKeys.size()))
        {
            currentCounter=0;
            keys=allKeys.get(currentListCounter);
            values=allValues.get(currentListCounter);
            currentListCounter++;
        }
        return counter<listSize;
    }

    @Override
    public ByteArrayWrapper getKey()
    {
        try
        {
            return keys.get(currentCounter);
        }
        catch(IndexOutOfBoundsException  e)
        {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public MeasureAggregator[] getValue()
    {
        return values.get(currentCounter);
    }

    @Override
    public void merge(Result<List<ByteArrayWrapper>, List<MeasureAggregator[]>> otherResult)
    {
        if(otherResult.getKeys().size()>0 && otherResult.getKeys().size()>0)
        {
            listSize+=otherResult.getKeys().size();
            this.allKeys.add(otherResult.getKeys());
            this.allValues.add(otherResult.getValues());
        }
    }

    @Override
    public int size()
    {
        return listSize;
    }

    @Override
    public List<ByteArrayWrapper> getKeys()
    {
        return keys;
    }

    @Override
    public List<MeasureAggregator[]> getValues()
    {
        return values;
    }
}
