package com.huawei.unibi.molap.engine.filter.executor.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.filter.executor.FilterExecutor;
import com.huawei.unibi.molap.util.ByteUtil;

public class MergeSortBasedNonUniqueBlockEquals implements FilterExecutor
{

    @Override
    public BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filterValues)
    {
        return setFilterdIndexToBitSetSortedBased(keyBlockArray, numerOfRows, filterValues);
    }

    private BitSet setFilterdIndexToBitSetSortedBased(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[][] filterValues)
    {
        BitSet bitSet = new BitSet(numerOfRows);
        int filterCounter = 0;
        int rowCounter = 0;
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        while(filterCounter < filterValues.length && rowCounter < numerOfRows)
        {
            if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), rowCounter
                    * filterValues[filterCounter].length, filterValues[filterCounter].length,
                    filterValues[filterCounter], 0, filterValues[filterCounter].length) == 0)
            {
                if(columnIndex != null)
                {
                    bitSet.set(columnIndex[rowCounter]);
                }
                else
                {
                    bitSet.set(rowCounter);
                }
                rowCounter++;
            }
            else
            {
                filterCounter++;
            }
        }
        return bitSet;
    }

}
