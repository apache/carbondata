package com.huawei.unibi.molap.engine.columnar.filter.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.columnar.filter.AbstractColumnarFilterProcessor;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapUtil;

public class ColumnarIncludeFilterProcessor extends AbstractColumnarFilterProcessor
{
    
    public ColumnarIncludeFilterProcessor(InMemFilterModel filterModel)
    {
        super(filterModel);
    }
    
    @Override
    public BitSet getFilteredIndexes(int keyBlockIndex, int numerOfRows,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        byte[][] filter = filterModel.getFilter()[keyBlockIndex];
        BitSet bitset = new BitSet(numerOfRows);
        if(null==keyBlockArray[0].getColumnarKeyStoreMetadata().getColumnIndex())
        {
            setFilterdIndexToBitSet(keyBlockArray[0], numerOfRows, filter, bitset);
        }
        else
        {
            setFilterdIndexToBitSetWithColumnIndex(keyBlockArray[0], numerOfRows, filter, bitset);
        }
        return bitset;
    }
    
    @Override
    public void getFilteredIndexes(int keyBlockIndex, int numerOfRows,BitSet set,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        byte[][] filter = filterModel.getFilter()[keyBlockIndex];
        BitSet localBitSet = new BitSet(set.cardinality());
       /* if(useBitSet(set, filter, numerOfRows) && false)
        {
            int[] columnReverseIndex = keyBlockArray[0].getColumnarKeyStoreMetadata().getColumnReverseIndex();
            int serachIndex=0;
            for(int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i+1)) 
            {
                serachIndex=MolapUtil.byteArrayBinarySearch(filter,keyBlockArray[0],columnReverseIndex[i]);
                if(serachIndex!=-1)
                {
                    localBitSet.set(i);
                }
            }
            set.and(localBitSet);
        }
        else
        {
            if(null==keyBlockArray[0].getColumnarKeyStoreMetadata().getColumnIndex())
            {
                setFilterdIndexToBitSet(keyBlockArray[0], numerOfRows, filter, localBitSet);
            }
            else
            {
                setFilterdIndexToBitSetWithColumnIndex(keyBlockArray[0], numerOfRows, filter, localBitSet);
            }
             set.and(localBitSet);
        }*/
        
        if(null==keyBlockArray[0].getColumnarKeyStoreMetadata().getColumnIndex())
        {
            setFilterdIndexToBitSet(keyBlockArray[0], numerOfRows, filter, localBitSet);
        }
        else
        {
            setFilterdIndexToBitSetWithColumnIndex(keyBlockArray[0], numerOfRows, filter, localBitSet);
        }
         set.and(localBitSet);
    }

    private void setFilterdIndexToBitSetWithColumnIndex(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filter,
            BitSet bitSet)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int start=0;
        int last/*=numerOfRows-1*/;
        int startIndex=0;
        for(int i = 0;i < filter.length;i++)
        {
            start = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows-1, filter[i]);
            if(start == -1)
            {
                continue;
            }
            bitSet.set(columnIndex[start]);
            last=start;
            for(int j = start + 1;j < numerOfRows;j++)
            {
                if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j*filter[i].length, filter[i].length,
                        filter[i], 0, filter[i].length)==0)
                {
                    bitSet.set(columnIndex[j]);
                    last++;
                }
                else
                {
                    break;
                }
            }
            startIndex=last;
            if(startIndex>=numerOfRows)
            {
                break;
            }
            
        }
    }
    private void setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filter,
            BitSet bitSet)
    {
        int start=0;
        int last/*=numerOfRows-1*/;
        int startIndex=0;
        for(int i = 0;i < filter.length;i++)
        {
            start = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows-1, filter[i]);
            if(start == -1)
            {
                continue;
            }
            bitSet.set(start);
            last=start;
            for(int j = start + 1;j < numerOfRows;j++)
            {
                if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j*filter[i].length, filter[i].length,
                        filter[i], 0, filter[i].length)==0)
                {
                    bitSet.set(j);
                    last++;
                }
                else
                {
                    break;
                }
            }
            startIndex=last;
            if(startIndex>=numerOfRows)
            {
                break;
            }
            
        }
    }
}
