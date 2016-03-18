/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.engine.columnar.filter.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.util.MolapUtil;

public class ColumnarIncludeExculeFilterProcessor extends ColumnarIncludeFilterProcessor
{
    public ColumnarIncludeExculeFilterProcessor(InMemFilterModel filterModel)
    {
        super(filterModel);
    }

    //  TODO SIMIAN
    @Override
    public BitSet getFilteredIndexes(int keyBlockIndx, int numerOfRows,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        BitSet bitset = new BitSet(numerOfRows);
        boolean isExcludeFilterPresent=null!=filterModel.getExcludeFilter()[keyBlockIndx];
        if(null != filterModel.getFilter()[keyBlockIndx])
        {
            bitset = super.getFilteredIndexes(keyBlockIndx, numerOfRows,keyBlockArray[0]);
            if(isExcludeFilterPresent)
            {
                getFilteredIndexesAfterExclude(keyBlockArray[0], keyBlockIndx, numerOfRows, bitset);
            }
        }
        else 
        {
            if(isExcludeFilterPresent)
            {
                setFilterdIndexToBitSet(keyBlockArray[0], numerOfRows, filterModel.getExcludeFilter()[keyBlockIndx], bitset);
            }
        }
        return bitset;
    }

    private void getFilteredIndexesAfterExclude(ColumnarKeyStoreDataHolder keyBlockArray, int keyBlockIndex, int numerOfRows, BitSet set)
    {
        byte[][] filter = filterModel.getExcludeFilter()[keyBlockIndex];
        boolean isExcludeFilterPresent=null!=filterModel.getExcludeFilter()[keyBlockIndex];
        if(!isExcludeFilterPresent)
        {
            return;
        }
        BitSet localBitSet = new BitSet(set.cardinality());
       /* if(useBitSet(set, filter, numerOfRows) && false)
        {
            int[] columnReverseIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnReverseIndex();
            int serachIndex=0;
            for(int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i+1)) 
            {
                serachIndex=MolapUtil.byteArrayBinarySearch(filter,keyBlockArray,columnReverseIndex[i]);
                if(serachIndex==-1)
                {
                    localBitSet.set(i);
                }
            }
            set.and(localBitSet);
        }
        else
        {
            setFilterdIndexToBitSet(keyBlockArray, numerOfRows, filter, localBitSet);
            set.and(localBitSet);
        }*/
        setFilterdIndexToBitSet(keyBlockArray, numerOfRows, filter, localBitSet);
        set.and(localBitSet);
    }

    @Override
    public void getFilteredIndexes(int keyBlockIndex, int numerOfRows, BitSet bitset,ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        boolean isExcludeFilterPresent=null!=filterModel.getExcludeFilter()[keyBlockIndex];
        if(null != filterModel.getFilter()[keyBlockIndex])
        {
            super.getFilteredIndexes(keyBlockIndex, numerOfRows, bitset,keyBlockArray[0]);
            if(isExcludeFilterPresent)
            {
                getFilteredIndexesAfterExclude(keyBlockArray[0], keyBlockIndex, numerOfRows, bitset);
            }
        }
        else
        {
            if(isExcludeFilterPresent)
            {
                getFilteredIndexesAfterExclude(keyBlockArray[0], keyBlockIndex, numerOfRows, bitset);
            }
        }
    }

    private void setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filter, BitSet bitSet)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        BitSet localBitSet = new BitSet(numerOfRows);
        localBitSet.set(0, numerOfRows, true);
        for(int i = 0;i < filter.length;i++)
        {
            int start = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, 0, numerOfRows - 1, filter[i]);
            int last = MolapUtil.getLastIndexUsingBinarySearch(keyBlockArray, 0, numerOfRows - 1, filter[i],
                    numerOfRows);
            if(start == -1 || last == -1)
            {
                continue;
            }
            if(null!=columnIndex)
            {
                for(int j = start;j <= last;j++)
                {
                    localBitSet.clear(columnIndex[j]);
                }
            }
            else
            {
                for(int j = start;j <= last;j++)
                {
                    localBitSet.clear(j);
                }
            }
        }
        bitSet.or(localBitSet);
    }
    
    
}
