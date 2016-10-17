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
package org.apache.carbondata.processing.newflow.sort.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * This iterator transform the row to how carbon sorter interface expects it.
 * TODO : It supposed to return a comparable ROW which can sort the row.
 */
public class SortPreparatorIterator extends CarbonIterator<CarbonRowBatch> {

  private Iterator<CarbonRowBatch> iterator;

  private int[] dictionaryFieldIndexes;

  private int[] nonDictionaryFieldIndexes;

  private int[] measueFieldIndexes;

  private int dimIndexInRow = IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex();

  private int byteArrayIndexInRow = IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex();

  private int measureIndexInRow = IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex();

  public SortPreparatorIterator(Iterator<CarbonRowBatch> iterator, DataField[] dataFields) {
    this.iterator = iterator;
    List<Integer> dictIndexes = new ArrayList<>();
    List<Integer> nonDictIndexes = new ArrayList<>();
    List<Integer> msrIndexes = new ArrayList<>();
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().isDimesion()) {
        if (dataFields[i].hasDictionaryEncoding()) {
          dictIndexes.add(i);
        } else {
          nonDictIndexes.add(i);
        }
      } else {
        msrIndexes.add(i);
      }
    }
    dictionaryFieldIndexes =
        ArrayUtils.toPrimitive(dictIndexes.toArray(new Integer[dictIndexes.size()]));
    nonDictionaryFieldIndexes =
        ArrayUtils.toPrimitive(nonDictIndexes.toArray(new Integer[nonDictIndexes.size()]));
    measueFieldIndexes = ArrayUtils.toPrimitive(msrIndexes.toArray(new Integer[msrIndexes.size()]));
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public CarbonRowBatch next() {
    CarbonRowBatch batch = iterator.next();
    Iterator<CarbonRow> batchIterator = batch.getBatchIterator();
    while (batchIterator.hasNext()) {
      Object[] outputArray = new Object[3];
      CarbonRow row = batchIterator.next();
      fillDictionaryArrayFromRow(row, outputArray);
      fillNonDictionaryArrayFromRow(row, outputArray);
      fillMeasureArrayFromRow(row, outputArray);
    }
    return batch;
  }

  /**
   * Collect all dictionary values to single integer array and store it in 0 index of out put array.
   *
   * @param row
   * @param outputArray
   */
  private void fillDictionaryArrayFromRow(CarbonRow row, Object[] outputArray) {
    if (dictionaryFieldIndexes.length > 0) {
      int[] dictArray = new int[dictionaryFieldIndexes.length];
      for (int i = 0; i < dictionaryFieldIndexes.length; i++) {
        dictArray[i] = row.getInt(dictionaryFieldIndexes[i]);
      }
      outputArray[dimIndexInRow] = dictArray;
    }
  }

  /**
   * collect all non dictionary columns and compose it to single byte array and store it in 1 index
   * of out put array
   *
   * @param row
   * @param outputArray
   */
  private void fillNonDictionaryArrayFromRow(CarbonRow row, Object[] outputArray) {
    if (nonDictionaryFieldIndexes.length > 0) {
      byte[][] nonDictByteArray = new byte[nonDictionaryFieldIndexes.length][];
      for (int i = 0; i < nonDictByteArray.length; i++) {
        nonDictByteArray[i] = row.getBinary(nonDictionaryFieldIndexes[i]);
      }

      byte[] nonDictionaryCols =
          RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(nonDictByteArray);
      outputArray[byteArrayIndexInRow] = nonDictionaryCols;
    }
  }

  /**
   * Collect all measure values as array and store it in 2 index of out put array.
   *
   * @param row
   * @param outputArray
   */
  private void fillMeasureArrayFromRow(CarbonRow row, Object[] outputArray) {
    if (measueFieldIndexes.length > 0) {
      Object[] measureArray = new Object[measueFieldIndexes.length];
      for (int i = 0; i < measueFieldIndexes.length; i++) {
        measureArray[i] = row.getObject(measueFieldIndexes[i]);
      }
      outputArray[measureIndexInRow] = measureArray;
    }
  }
}
