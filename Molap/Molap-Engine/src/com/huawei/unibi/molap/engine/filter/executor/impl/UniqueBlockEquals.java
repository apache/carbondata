package com.huawei.unibi.molap.engine.filter.executor.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.util.MolapUtil;

public class UniqueBlockEquals extends NonUniqueBlockEquals
{
    @Override
    public BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filterValues)
    {
        if(keyBlockArray.getColumnarKeyStoreMetadata().isUnCompressed())
        {
            return super.getFilteredIndexes(keyBlockArray, numerOfRows, filterValues);
        }
        return setFilterdIndexToBitSet(keyBlockArray, numerOfRows, filterValues);
    }

    private BitSet setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[][] filterValues)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] dataIndex = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        int startIndex = 0;
        int lastIndex = dataIndex.length == 0 ? numerOfRows - 1 : dataIndex.length / 2 - 1;
        BitSet bitSet = new BitSet(numerOfRows);
        for(int i = 0;i < filterValues.length;i++)
        {
            int index = MolapUtil.getIndexUsingBinarySearch(keyBlockArray, startIndex, lastIndex, filterValues[i]);
            if(index == -1)
            {
                continue;
            }
            if(dataIndex.length == 0)
            {
                if(null != columnIndex)
                {
                    bitSet.set(columnIndex[index]);
                }
                else
                {
                    bitSet.set(index);
                }
                continue;
            }

            startIndex = index + 1;
            int last = dataIndex[index * 2] + dataIndex[index * 2 + 1];
            if(null != columnIndex)
            {
                for(int start = dataIndex[index * 2];start < last;start++)
                {
                    bitSet.set(columnIndex[start]);
                }
            }
            else
            {
                for(int start = dataIndex[index * 2];start < last;start++)
                {
                    bitSet.set(start);
                }
            }
        }
        return bitSet;
    }
}
