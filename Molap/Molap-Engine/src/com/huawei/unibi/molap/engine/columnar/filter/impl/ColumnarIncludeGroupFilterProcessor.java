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
import com.huawei.unibi.molap.engine.columnar.filter.AbstractColumnarFilterProcessor;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapUtil;

public class ColumnarIncludeGroupFilterProcessor extends AbstractColumnarFilterProcessor
{
    // short 
    private int[][] groupFilterIndexes;

    public ColumnarIncludeGroupFilterProcessor(InMemFilterModel filterModel, int[][] groupFilterIndexes)
    {
        super(filterModel);
        this.groupFilterIndexes = groupFilterIndexes;
    }

    @Override
    public void getFilteredIndexes(int keyBlockIndex, int numerOfRows, BitSet set,
            ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        byte[][] filter = filterModel.getFilter()[keyBlockIndex];
        BitSet localBitSet = new BitSet(set.cardinality());
        setFilterdIndexToBitSet(numerOfRows, filter, set, groupFilterIndexes[keyBlockIndex], keyBlockArray);
        set.and(localBitSet);
    }

    @Override
    public BitSet getFilteredIndexes(int keyBlockIndex, int numerOfRows, ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        byte[][] filter = filterModel.getFilter()[keyBlockIndex];
        BitSet bitset = new BitSet(numerOfRows);
        setFilterdIndexToBitSet(numerOfRows, filter, bitset, groupFilterIndexes[keyBlockIndex], keyBlockArray);
        return bitset;
    }

    private void setFilterdIndexToBitSet(int numerOfRows, byte[][] filter, BitSet bitSet, int[] groupFilterIndexLocal,
            ColumnarKeyStoreDataHolder... keyBlockArray)
    {
        BitSet[] localBitSet = new BitSet[2];
        int offset = 0;
        for(int i = 0;i < filter.length;i++)
        {
            localBitSet[0] = new BitSet(numerOfRows);
            setFilterdIndexToBitSet(numerOfRows, filter[i], localBitSet[0], keyBlockArray[0], 0, offset,
                    groupFilterIndexLocal[0]);
            offset += keyBlockArray[0].getColumnarKeyStoreMetadata().getEachRowSize();
            
            if(localBitSet[0].isEmpty())
            {
                continue;
            }
            for(int j = 1;j < keyBlockArray.length;j++)
            {
                setFilterdIndexToBitSet(numerOfRows, filter[i], localBitSet[1], keyBlockArray[j], j, offset,
                        groupFilterIndexLocal[j]);
                offset += keyBlockArray[0].getColumnarKeyStoreMetadata().getEachRowSize();
                localBitSet[0].and(localBitSet[j]);
                localBitSet[1].clear();
            }
            bitSet.or(localBitSet[0]);
            localBitSet[0].clear();
        }
    }

    private void setFilterdIndexToBitSet(int numerOfRows, byte[] filter, BitSet bitSet,
            ColumnarKeyStoreDataHolder keyBlockArray, int index, int offset, int filterTypeIndexLocal)
    {
        if(1 == filterTypeIndexLocal)
        {
            setFilterIndexToBitSetForUniqueValues(keyBlockArray, numerOfRows, filter, bitSet, offset);
        }
        else if(null == keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex())
        {
            setFilterIndexToBitSetForDuplicateValues(keyBlockArray, numerOfRows, filter, bitSet, offset);
        }
        else
        {
            setFilterIndexToBitSetForDuplicateValuesWithColumnIndex(keyBlockArray, numerOfRows, filter, bitSet, offset);
        }

    }

    private void setFilterIndexToBitSetForUniqueValues(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[] filter, BitSet bitSet, int offset)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] dataIndex = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        int startIndex = 0;
        int index = MolapUtil.getIndexUsingBinarySearch(keyBlockArray, startIndex, dataIndex.length / 2 - 1, filter,
                keyBlockArray.getColumnarKeyStoreMetadata().getEachRowSize(), offset);
        if(index == -1)
        {
            return;
        }
//        startIndex = index + 1;
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

    private void setFilterIndexToBitSetForDuplicateValues(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[] filter, BitSet bitSet, int offset)
    {
        int start = 0;
        start = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, 0, numerOfRows - 1, filter, numerOfRows,
                keyBlockArray.getColumnarKeyStoreMetadata().getEachRowSize(), offset);
        if(start == -1)
        {
            return;
        }
        bitSet.set(start);
        for(int j = start;j < numerOfRows;j++)
        {
            if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j
                    * keyBlockArray.getColumnarKeyStoreMetadata().getEachRowSize(), keyBlockArray
                    .getColumnarKeyStoreMetadata().getEachRowSize(), filter, offset, keyBlockArray
                    .getColumnarKeyStoreMetadata().getEachRowSize()) == 0)
            {
                bitSet.set(j);
            }
            else
            {
                break;
            }
        }
    }

    private void setFilterIndexToBitSetForDuplicateValuesWithColumnIndex(ColumnarKeyStoreDataHolder keyBlockArray,
            int numerOfRows, byte[] filter, BitSet bitSet, int offset)
    {

        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int start = 0;
        start = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, 0, numerOfRows - 1, filter, numerOfRows,
                keyBlockArray.getColumnarKeyStoreMetadata().getEachRowSize(), offset);
        bitSet.set(columnIndex[start]);
        for(int j = start;j < numerOfRows;j++)
        {
            if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j
                    * keyBlockArray.getColumnarKeyStoreMetadata().getEachRowSize(), keyBlockArray
                    .getColumnarKeyStoreMetadata().getEachRowSize(), filter, offset, keyBlockArray
                    .getColumnarKeyStoreMetadata().getEachRowSize()) == 0)
            {
                bitSet.set(columnIndex[j]);
            }
            else
            {
                break;
            }
        }
    }

}
