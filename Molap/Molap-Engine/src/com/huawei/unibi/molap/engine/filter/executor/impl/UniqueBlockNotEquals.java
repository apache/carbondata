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

package com.huawei.unibi.molap.engine.filter.executor.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.util.MolapUtil;

public class UniqueBlockNotEquals extends NonUniqueBlockNotEquals
{  
    
    private BitSet setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[][] filterValues)
    {
        BitSet bitSet = new BitSet(numerOfRows);
        bitSet.flip(0, numerOfRows);
        int[] dataIndexTemp = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();        
        int startIndex = 0;
        int lastIndex = dataIndexTemp.length == 0 ? numerOfRows - 1 : dataIndexTemp.length / 2 - 1;
       
        for(int i = 0;i < filterValues.length;i++)
        {
            int index = MolapUtil.getIndexUsingBinarySearch(keyBlockArray, startIndex, lastIndex, filterValues[i]);
            if(index == -1)
            {
                continue;
            }
            if(dataIndexTemp.length == 0)
            {
                if(null != columnIndex)
                {
                    bitSet.flip(columnIndex[index]);
                }
                else
                {
                    bitSet.flip(index);
                }
                continue;
            }

            startIndex = index + 1;
            int last = dataIndexTemp[index * 2] + dataIndexTemp[index * 2 + 1];
            if(null != columnIndex)
            {
                for(int start = dataIndexTemp[index * 2];start < last;start++)
                {
                    bitSet.flip(columnIndex[start]);
                }
            }
            else
            {
                for(int start = dataIndexTemp[index * 2];start < last;start++)
                {
                    bitSet.flip(start);
                }
            }
        }
        return bitSet;
    }
    
    @Override
    public BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filterValues)
    {
        if(keyBlockArray.getColumnarKeyStoreMetadata().isUnCompressed())
        {
            return super.getFilteredIndexes(keyBlockArray, numerOfRows, filterValues);
        }
        return setFilterdIndexToBitSet(keyBlockArray, numerOfRows, filterValues);
    }
}

