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

package org.carbondata.query.evaluators.conditional.dimcolumns;

import java.util.BitSet;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.FilterProcessorPlaceHolder;
import org.carbondata.query.expression.Expression;

public class UniqueBlockEqualsEvalutor extends NonUniqueBlockEqualsEvalutor {
    public UniqueBlockEqualsEvalutor(Expression exp, boolean isExpressionResolve,
            boolean isIncludeFilter) {
        super(exp, isExpressionResolve, isIncludeFilter);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder dataBlockHolder,
            FilterProcessorPlaceHolder placeHolder,int[] noDictionaryColIndexes) {
        if (null == dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0)
                .getColumnIndex()]) {
            dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()] =
                    dataBlockHolder.getLeafDataBlock()
                            .getColumnarKeyStore(dataBlockHolder.getFileHolder(),
                                    dimColEvaluatorInfoList.get(0).getColumnIndex(),
                                    dimColEvaluatorInfoList.get(0).isNeedCompressedData(),noDictionaryColIndexes);
        }

        if (dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()]
                .getColumnarKeyStoreMetadata().isUnCompressed()) {
            return super.applyFilter(dataBlockHolder, placeHolder,noDictionaryColIndexes);
        }
        return getFilteredIndexes(
                dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0)
                        .getColumnIndex()], dataBlockHolder.getLeafDataBlock().getnKeys());
    }

    private BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows) {
        int[] colIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] dataIndex = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        int startIndex = 0;
        int lastIndex = dataIndex.length == 0 ? numerOfRows - 1 : dataIndex.length / 2 - 1;
        BitSet bitSet = new BitSet(numerOfRows);
        for (int i = 0; i < dimColEvaluatorInfoList.get(0).getFilterValues().length; i++) {
            int index = CarbonUtil.getIndexUsingBinarySearch(keyBlockArray, startIndex, lastIndex,
                    dimColEvaluatorInfoList.get(0).getFilterValues()[i]);
            if (index == -1) {
                continue;
            }
            if (dataIndex.length == 0) {
                if (null != colIndex) {
                    bitSet.set(colIndex[index]);
                } else {
                    bitSet.set(index);
                }
                continue;
            }

            startIndex = index + 1;
            int last = dataIndex[index * 2] + dataIndex[index * 2 + 1];
            if (null != colIndex) {
                for (int start = dataIndex[index * 2]; start < last; start++) {
                    bitSet.set(colIndex[start]);
                }
            } else {
                for (int start = dataIndex[index * 2]; start < last; start++) {
                    bitSet.set(start);
                }
            }
        }
        return bitSet;
    }

}
