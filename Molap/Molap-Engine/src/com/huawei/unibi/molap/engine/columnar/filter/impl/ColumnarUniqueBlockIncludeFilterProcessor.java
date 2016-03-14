package com.huawei.unibi.molap.engine.columnar.filter.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.columnar.filter.AbstractColumnarFilterProcessor;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.util.MolapUtil;

public class ColumnarUniqueBlockIncludeFilterProcessor extends AbstractColumnarFilterProcessor
{
    public ColumnarUniqueBlockIncludeFilterProcessor(InMemFilterModel filterModel)
    {
        super(filterModel);
    }
    
    @Override
    public void getFilteredIndexes(int keyBlockIndex, int numerOfRows,
            BitSet set,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        byte[][] filter = filterModel.getFilter()[keyBlockIndex];
        BitSet localBitSet = new BitSet(set.cardinality());
        setFilterdIndexToBitSet(keyBlockArray[0], numerOfRows,filter, localBitSet);
        set.and(localBitSet);

    }

    @Override
    public BitSet getFilteredIndexes(int keyBlockIndex, int numerOfRows,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        byte[][] filter = filterModel.getFilter()[keyBlockIndex];
        BitSet bitset = new BitSet(numerOfRows);
        setFilterdIndexToBitSet(keyBlockArray[0],numerOfRows,filter, bitset);
        return bitset;
    }
    
    private void setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,byte[][] filter,
            BitSet bitSet)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] dataIndex = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        int startIndex = 0;
        int lastIndex = dataIndex.length == 0 ? numerOfRows - 1 : dataIndex.length / 2 - 1;
        for(int i = 0;i < filter.length;i++)
        {
            int index = MolapUtil.getIndexUsingBinarySearch(keyBlockArray, startIndex, lastIndex, filter[i]);
            if(index == -1)
            {
                continue;
            }
            startIndex = index + 1;
            int last = dataIndex.length > 0 ? dataIndex[index * 2] + dataIndex[index * 2 + 1] : 1;
            if(null != columnIndex)
            {
                for(int start = dataIndex.length > 0 ? dataIndex[index * 2] : index;start < last;start++)
                {
                    bitSet.set(columnIndex[start]);
                }
            }
            else
            {
                for(int start = dataIndex.length > 0 ? dataIndex[index * 2] : index;start < last;start++)
                {
                    bitSet.set(start);
                }
            }
        }
    }
}
