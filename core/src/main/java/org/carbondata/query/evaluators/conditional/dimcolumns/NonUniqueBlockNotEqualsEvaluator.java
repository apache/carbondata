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
import java.util.Iterator;
import java.util.List;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.evaluators.AbstractConditionalEvalutor;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.FilterProcessorPlaceHolder;
import org.carbondata.query.expression.Expression;
@Deprecated
public class NonUniqueBlockNotEqualsEvaluator extends AbstractConditionalEvalutor {
  public NonUniqueBlockNotEqualsEvaluator(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter) {
    super(exp, isExpressionResolve, isIncludeFilter);
  }

  @Override public BitSet applyFilter(BlockDataHolder blockDataHolderArray,
      FilterProcessorPlaceHolder placeHolder, int[] noDictionaryColIndexes) {
    if (null == blockDataHolderArray.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0)
        .getColumnIndex()]) {
      blockDataHolderArray.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()] =
          blockDataHolderArray.getLeafDataBlock()
              .getColumnarKeyStore(blockDataHolderArray.getFileHolder(),
                  dimColEvaluatorInfoList.get(0).getColumnIndex(),
                  dimColEvaluatorInfoList.get(0).isNeedCompressedData(), noDictionaryColIndexes);
    }
    return getFilteredIndexes(
        blockDataHolderArray.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()],
        blockDataHolderArray.getLeafDataBlock().getnKeys());
  }

  private BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyStoreArray, int numerOfRows) {
    //For high cardinality dimensions.
    if (keyStoreArray.getColumnarKeyStoreMetadata().isNoDictionaryValColumn()) {
      return setDirectKeyFilterIndexToBitSet(keyStoreArray, numerOfRows);
    }
    if (null != keyStoreArray.getColumnarKeyStoreMetadata().getColumnIndex()) {
      return setFilterdIndexToBitSetWithColumnIndex(keyStoreArray, numerOfRows);
    }
    return setFilterdIndexToBitSet(keyStoreArray, numerOfRows);
  }

  private BitSet setDirectKeyFilterIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    List<byte[]> listOfColumnarKeyBlockDataForNoDictionaryVals =
        keyBlockArray.getNoDictionaryValBasedKeyBlockData();
    byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    int[] columnIndexArray = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
    int[] columnReverseIndexArray =
        keyBlockArray.getColumnarKeyStoreMetadata().getColumnReverseIndex();
    for (int i = 0; i < filterValues.length; i++) {
      byte[] filterVal = filterValues[i];
      if (null != listOfColumnarKeyBlockDataForNoDictionaryVals) {

        if (null != columnReverseIndexArray) {
          for (int index : columnIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVals.get(columnReverseIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.flip(index);
            }
          }
        } else if (null != columnIndexArray) {

          for (int index : columnIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVals.get(columnIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.flip(index);
            }
          }

        } else {
          Iterator<byte[]> itr = listOfColumnarKeyBlockDataForNoDictionaryVals.iterator();
          int index = 0;
          while (itr.hasNext()) {
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, itr.next()) == 0) {
              bitSet.flip(index++);
            }

          }

        }

      }
    }
    return bitSet;

  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(ColumnarKeyStoreDataHolder keyBlockArray,
      int numerOfRows) {
    int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
    int startKey = 0;
    int last = 0;
    int startIndex = 0;
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
   /* byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    for (int i = 0; i < filterValues.length; i++) {
      startKey = CarbonUtil
          .getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
              filterValues[i]);
      if (startKey == -1) {
        continue;
      }
      bitSet.flip(columnIndex[startKey]);
      last = startKey;
      for (int j = startKey + 1; j < numerOfRows; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(keyBlockArray.getKeyBlockData(), j * filterValues[i].length,
                filterValues[i].length, filterValues[i], 0, filterValues[i].length) == 0) {
          bitSet.flip(columnIndex[j]);
          last++;
        } else {
          break;
        }
      }
      startIndex = last;
      if (startIndex >= numerOfRows) {
        break;
      }
    }*/
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray,
      int numerOfRows) {
    int startKey = 0;
    int last = 0;
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    int startIndex = 0;
   /* byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    for (int k = 0; k < filterValues.length; k++) {
      startKey = CarbonUtil
          .getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
              filterValues[k]);
      if (startKey == -1) {
        continue;
      }
      bitSet.flip(startKey);
      last = startKey;
      for (int j = startKey + 1; j < numerOfRows; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(keyBlockArray.getKeyBlockData(), j * filterValues[k].length,
                filterValues[k].length, filterValues[k], 0, filterValues[k].length) == 0) {
          bitSet.flip(j);
          last++;
        } else {
          break;
        }
      }
      startIndex = last;
      if (startIndex >= numerOfRows) {
        break;
      }
    }*/
    return bitSet;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.flip(0, 1);
    return bitSet;
  }
}
