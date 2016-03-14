package com.huawei.unibi.molap.engine.columnar.filter.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.util.MolapUtil;

public class ColumnarUniqueBlockIncludeExcludeFilterInfo extends ColumnarUniqueBlockIncludeFilterProcessor
{

    public ColumnarUniqueBlockIncludeExcludeFilterInfo(InMemFilterModel filterModel)
    {
        super(filterModel);
    }
    
    @Override
    public BitSet getFilteredIndexes(int keyBlockIndex, int numerOfRows,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        BitSet bitset = new BitSet(numerOfRows);
        boolean isExcludeFilterPresent=null!=filterModel.getExcludeFilter()[keyBlockIndex];
        if(null != filterModel.getFilter()[keyBlockIndex])
        {
            bitset = super.getFilteredIndexes(keyBlockIndex, numerOfRows,keyBlockArray[0]);
            if(isExcludeFilterPresent)
            {
                getFilteredIndexesAfterExclude(keyBlockArray[0], keyBlockIndex, numerOfRows, bitset);
            }
        }
        else
        {
            if(isExcludeFilterPresent)
            {
                setFilterdIndexToBitSet(keyBlockArray[0], numerOfRows, filterModel.getExcludeFilter()[keyBlockIndex], bitset);
            }
        }
        return bitset;
    }
    
    //TODO SIMIAN
    @Override 
    public void getFilteredIndexes(int keyBlockIdx, int numerOfRows, BitSet bitset,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        boolean isExcludeFilterPresent=null!=filterModel.getExcludeFilter()[keyBlockIdx];
        if(null != filterModel.getFilter()[keyBlockIdx])
        {
            super.getFilteredIndexes(keyBlockIdx, numerOfRows, bitset,keyBlockArray[0]);
            if(isExcludeFilterPresent)
            {
                getFilteredIndexesAfterExclude(keyBlockArray[0], keyBlockIdx, numerOfRows, bitset);
            }
        }
        else
        {
            if(isExcludeFilterPresent)
            {
                getFilteredIndexesAfterExclude(keyBlockArray[0], keyBlockIdx, numerOfRows, bitset);
            }
        }
    }
    
    private void getFilteredIndexesAfterExclude(ColumnarKeyStoreDataHolder keyBlockArray, int keyBlockIndex, int numerOfRows, BitSet set)
    {
        byte[][] filter = filterModel.getExcludeFilter()[keyBlockIndex];
        boolean isExcludeFilterPresent = null != filterModel.getExcludeFilter()[keyBlockIndex];
        if(!isExcludeFilterPresent)
        {
            return;
        }
        BitSet localBitSet = new BitSet(set.cardinality());
        setFilterdIndexToBitSet(keyBlockArray, numerOfRows, filter, localBitSet);
        set.and(localBitSet);
    }
    
    private void setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filter,
            BitSet bitSet)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] dataIndex = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        BitSet localBitset = new BitSet(numerOfRows);
        localBitset.set(0, numerOfRows, true);
        for(int i = 0;i < filter.length;i++)
        {
            int index = MolapUtil.getIndexUsingBinarySearch(keyBlockArray, 0, dataIndex.length/2-1, filter[i]);
            if(index == -1)
            {
                continue;
            }
//            int last=dataIndex[index*2]+dataIndex[index*2+1];
            int last = dataIndex.length > 0 ? dataIndex[index * 2] + dataIndex[index * 2 + 1] : 1;
            if(null!=columnIndex)
            {
                for(int start = dataIndex.length > 0 ? dataIndex[index * 2] : index;start < last;start++)
                {
                    localBitset.clear(columnIndex[start]);
                }
            }
            else
            {
                for(int start = dataIndex.length > 0 ? dataIndex[index * 2] : index;start < last;start++)
                {
                    localBitset.clear(start);
                }
            }
        }
        bitSet.or(localBitset);
    }
}
