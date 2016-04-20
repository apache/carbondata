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
public class NonUniqueBlockEqualsEvalutor extends AbstractConditionalEvalutor {

  public NonUniqueBlockEqualsEvalutor(Expression exp, boolean isExpressionResolve,
      boolean isExcludeFilter) {
    super(exp, isExpressionResolve, isExcludeFilter);
  }

  @Override
  public BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder,
      int[] noDictionaryColIndexes) {
    if (null == blockDataHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0)
        .getColumnIndex()]) {
      blockDataHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()] =
          blockDataHolder.getLeafDataBlock().getColumnarKeyStore(blockDataHolder.getFileHolder(),
              dimColEvaluatorInfoList.get(0).getColumnIndex(),
              dimColEvaluatorInfoList.get(0).isNeedCompressedData(), noDictionaryColIndexes);
    }
    return getFilteredIndexes(
        blockDataHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()],
        blockDataHolder.getLeafDataBlock().getnKeys());
  }

  private BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows) {
    if (keyBlockArray.getColumnarKeyStoreMetadata().isNoDictionaryValColumn()) {
      return setDirectKeyFilterIndexToBitSet(keyBlockArray, numerOfRows);
    } else if (null != keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex()) {
      return setFilterdIndexToBitSetWithColumnIndex(keyBlockArray, numerOfRows);
    }

    return setFilterdIndexToBitSet(keyBlockArray, numerOfRows);
  }

  private BitSet setDirectKeyFilterIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    List<byte[]> listOfColumnarKeyBlockDataForNoDictionaryVals =
        keyBlockArray.getNoDictionaryValBasedKeyBlockData();
    byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    int[] columnIndexArray = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
    int[] columnReverseIndexArray =
        keyBlockArray.getColumnarKeyStoreMetadata().getColumnReverseIndex();
    for (int i = 0; i < filterValues.length; i++) {
      byte[] filterVal = filterValues[i];
      if (null != listOfColumnarKeyBlockDataForNoDictionaryVals) {

        if (null != columnIndexArray) {
          for (int index : columnIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVals.get(columnReverseIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.set(index);
            }
          }
        } else if (null != columnReverseIndexArray) {

          for (int index : columnReverseIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVals.get(columnReverseIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.set(index);
            }
          }
        } else {
          Iterator<byte[]> itr = listOfColumnarKeyBlockDataForNoDictionaryVals.iterator();
          int index = 0;
          while (itr.hasNext()) {
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, itr.next()) == 0) {
              bitSet.set(index++);
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
    int start = 0;
    int last = 0;
    int startIndex = 0;
    BitSet bitSet = new BitSet(numerOfRows);
  /*  byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    for (int i = 0; i < filterValues.length; i++) {
      start = CarbonUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
          filterValues[i]);
      if (start == -1) {
        continue;
      }
      bitSet.set(columnIndex[start]);
      last = start;
      for (int j = start + 1; j < numerOfRows; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(keyBlockArray.getKeyBlockData(), j * filterValues[i].length,
                filterValues[i].length, filterValues[i], 0, filterValues[i].length) == 0) {
          bitSet.set(columnIndex[j]);
          last++;
        } else {
          break;
        }
      }
      startIndex = last;
      if (startIndex >= numerOfRows) {
        break;
      }*/
    
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(ColumnarKeyStoreDataHolder keyBlockArray,
      int numerOfRows) {
    int start = 0;
    int last = 0;
    BitSet bitSet = new BitSet(numerOfRows);
    int startIndex = 0;
   /* byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    for (int k = 0; k < filterValues.length; k++) {
      start = CarbonUtil.getFirstIndexUsingBinarySearch(keyBlockArray, startIndex, numerOfRows - 1,
          filterValues[k]);
      if (start == -1) {
        continue;
      }
      bitSet.set(start);
      last = start;
      for (int j = start + 1; j < numerOfRows; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(keyBlockArray.getKeyBlockData(), j * filterValues[k].length,
                filterValues[k].length, filterValues[k], 0, filterValues[k].length) == 0) {
          bitSet.set(j);
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

  @Override public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
    int columnIndex = dimColEvaluatorInfoList.get(0).getColumnIndex();
    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      //filter value should be in range of max and min value i.e max>filtervalue>min
      //so filter-max should be negative
      int maxCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blkMaxVal[columnIndex]);
      //and filter-min should be positive
      int minCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blkMinVal[columnIndex]);

      //if any filter value is in range than this block needs to be scanned
      if (maxCompare <= 0 && minCompare >= 0) {
        isScanRequired = true;
        break;
      }
    }
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

}