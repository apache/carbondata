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

package com.huawei.unibi.molap.engine.evaluators.conditional.dimcolumns;

import java.util.BitSet;
import java.util.Map;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.evaluators.AbstractConditionalEvalutor;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterProcessorPlaceHolder;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapUtil;

public class NonUniqueBlockNotEqualsEvaluator extends AbstractConditionalEvalutor
{
    public NonUniqueBlockNotEqualsEvaluator(Expression exp, boolean isExpressionResolve, boolean isIncludeFilter)
    {
        super(exp,isExpressionResolve,isIncludeFilter);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder blockDataHolderArray, FilterProcessorPlaceHolder placeHolder)
    {
        if(null==blockDataHolderArray.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()])
        {
            blockDataHolderArray.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()] = blockDataHolderArray
                    .getLeafDataBlock().getColumnarKeyStore(blockDataHolderArray.getFileHolder(),
                            dimColEvaluatorInfoList.get(0).getColumnIndex(),
                            dimColEvaluatorInfoList.get(0).isNeedCompressedData());
        }
        return getFilteredIndexes(
                blockDataHolderArray.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()],
                blockDataHolderArray.getLeafDataBlock().getnKeys());
    }

    private BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyStoreArray, int numerOfRows)
    {
        //For high cardinality dimensions.
        if(keyStoreArray.getColumnarKeyStoreMetadata().isDirectSurrogateColumn())
        {
            return setDirectKeyFilterIndexToBitSet(keyStoreArray,numerOfRows);
        }
        if(null!=keyStoreArray.getColumnarKeyStoreMetadata().getColumnIndex())
        {
            return setFilterdIndexToBitSetWithColumnIndex(keyStoreArray,numerOfRows);
        }
        return setFilterdIndexToBitSet(keyStoreArray,numerOfRows);
    }
    
    private BitSet setDirectKeyFilterIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows)
    {
        BitSet bitSet = new BitSet(numerOfRows);
        bitSet.flip(0,numerOfRows);
        Map<Integer, byte[]> mapOfColumnarKeyBlockDataForDirectSurroagtes = keyBlockArray
                .getColumnarKeyStoreMetadata().getMapOfColumnarKeyBlockDataForDirectSurroagtes();
        byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
        int[] columnIndexArray = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] columnReverseIndexArray = keyBlockArray.getColumnarKeyStoreMetadata()
                .getColumnReverseIndex();
        for(int i = 0;i < filterValues.length;i++)
        {
            byte[] filterVal = filterValues[i];
            if(null != mapOfColumnarKeyBlockDataForDirectSurroagtes)
            {

                if(null != columnReverseIndexArray)
                {
                    for(int index : columnIndexArray)
                    {
                        byte[] directSurrogate = mapOfColumnarKeyBlockDataForDirectSurroagtes.get(columnReverseIndexArray[index]);
                        if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, directSurrogate) == 0)
                        {
                            bitSet.flip(index);
                        }
                    }
                }
                else if(null != columnIndexArray)
                {

                    for(int index : columnIndexArray)
                    {
                        byte[] directSurrogate = mapOfColumnarKeyBlockDataForDirectSurroagtes.get(columnIndexArray[index]);
                        if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, directSurrogate) == 0)
                        {
                            bitSet.flip(index);
                        }
                    }
                }

            }
        }
        return bitSet;
        
    }

    
    private BitSet setFilterdIndexToBitSetWithColumnIndex(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows)
    {
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int startKey = 0;
        int last = 0;
        int startIndex = 0;
        BitSet bitSet = new BitSet(numerOfRows);
        bitSet.flip(0,numerOfRows);
        byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
        for(int i = 0;i < filterValues.length;i++)
        {
            startKey = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
                    filterValues[i]);
            if(startKey == -1)
            {
                continue;
            }
            bitSet.flip(columnIndex[startKey]);
            last = startKey;
            for(int j = startKey + 1;j < numerOfRows;j++)
            {
                if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j
                        * filterValues[i].length,
                        filterValues[i].length,
                        filterValues[i], 0,
                        filterValues[i].length) == 0)
                {
                    bitSet.flip(columnIndex[j]);
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
    private BitSet setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows)
    {
        int startKey = 0;
        int last = 0;
        BitSet bitSet = new BitSet(numerOfRows);
        bitSet.flip(0,numerOfRows);
        int startIndex = 0;
        byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
        for(int k = 0;k < filterValues.length;k++)
        {
            startKey = MolapUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
                    filterValues[k]);
            if(startKey == -1)
            {
                continue;
            }
            bitSet.flip(startKey);
            last = startKey;
            for(int j = startKey + 1;j < numerOfRows;j++)
            {
                if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), j
                        *filterValues[k].length,
                        filterValues[k].length,
                        filterValues[k], 0,
                        filterValues[k].length) == 0)
                {
                    bitSet.flip(j);
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
    
/* For Not equals, range check is not valid.
 *    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue)
    {
        BitSet bitSet = new BitSet(1);
     
        byte[][] fltrValues = dimColEvaluatorInfoList.get(0).getFilterValues(); 
        int columnIndex = dimColEvaluatorInfoList.get(0).getColumnIndex();
        boolean isScanRequired=false;
        for(int k = 0;k < fltrValues.length;k++)
        {
            //filter value should not be in range of max and min value i.e max<filtervalue>min
           
            // filter-max should be positive
            int maxCompare=ByteUtil.UnsafeComparer.INSTANCE.compareTo(fltrValues[k],blockMaxValue[columnIndex]);
            //filter-min should be positive
            int minCompare=ByteUtil.UnsafeComparer.INSTANCE.compareTo(fltrValues[k],blockMinValue[columnIndex]);
            //if any filter meets this criteria
            if(maxCompare!=0 || minCompare!=0)
            {
                isScanRequired=true;
            }
        }
        if(isScanRequired)
        {
            bitSet.flip(0); 
        }
        return bitSet;
    }*/
    
    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue)
    {
        BitSet bitSet = new BitSet(1);
        bitSet.flip(0,1);
        return bitSet;
    }
}
