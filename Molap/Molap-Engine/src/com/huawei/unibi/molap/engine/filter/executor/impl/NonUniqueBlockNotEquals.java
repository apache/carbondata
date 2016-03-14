package com.huawei.unibi.molap.engine.filter.executor.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.filter.executor.FilterExecutor;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapUtil;

public class NonUniqueBlockNotEquals implements FilterExecutor
{
    @Override
    public BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filterValues)
    {
        return setFilterdIndexToBitSet(keyBlockArray, numerOfRows, filterValues);
    }

    private BitSet setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[][] filterValues)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int start;
        int last;
        int startIndex = 0;
        BitSet bitSet = new BitSet(numerOfRows);
        bitSet.flip(0,numerOfRows);
        for(int i = 0;i < filterValues.length;i++)
        {
            start = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
                    filterValues[i]);
            if(start == -1)
            {
                continue;
            }
            if(null!=columnIndex)
            {
                bitSet.flip(columnIndex[start]);
            }
            else
            {
                bitSet.flip(start);
            }
            last = start;
            for(int j = start + 1;j < numerOfRows;j++)
            {
                if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j
                        * filterValues[i].length, filterValues[i].length, filterValues[i], 0, filterValues[i].length) == 0)
                {
                    if(null != columnIndex)
                    {
                        bitSet.flip(columnIndex[j]);
                    }
                    else
                    {
                        bitSet.flip(j);
                    }
                    last++;
                }
                else
                {
                    break;
                }
            }
            startIndex = last;
            if(startIndex >= numerOfRows)
            {
                break;
            }
        }
        return bitSet;
    }

}
