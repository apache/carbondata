/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.loading.sort;

import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;

public class SortStepRowUtil {
  private int measureCount;
  private int dimensionCount;
  private int complexDimensionCount;
  private int noDictionaryCount;
  private int[] dictDimIdx;
  private int[] nonDictIdx;
  private int[] measureIdx;

  public SortStepRowUtil(SortParameters parameters) {
    this.measureCount = parameters.getMeasureColCount();
    this.dimensionCount = parameters.getDimColCount();
    this.complexDimensionCount = parameters.getComplexDimColCount();
    this.noDictionaryCount = parameters.getNoDictionaryCount();
    boolean[] isNoDictionaryDimensionColumn = parameters.getNoDictionaryDimnesionColumn();

    int index = 0;
    int nonDicIndex = 0;
    int allCount = 0;

    // be careful that the default value is 0
    this.dictDimIdx = new int[dimensionCount - noDictionaryCount];
    this.nonDictIdx = new int[noDictionaryCount + complexDimensionCount];
    this.measureIdx = new int[measureCount];

    // indices for dict dim columns
    for (int i = 0; i < isNoDictionaryDimensionColumn.length; i++) {
      if (isNoDictionaryDimensionColumn[i]) {
        nonDictIdx[nonDicIndex++] = i;
      } else {
        dictDimIdx[index++] = allCount;
      }
      allCount++;
    }

    // indices for non dict dim/complex columns
    for (int i = 0; i < complexDimensionCount; i++) {
      nonDictIdx[nonDicIndex++] = allCount;
      allCount++;
    }

    // indices for measure columns
    for (int i = 0; i < measureCount; i++) {
      measureIdx[i] = allCount;
      allCount++;
    }
  }

  public Object[] convertRow(Object[] data) {
    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)
    Object[] holder = new Object[3];
    try {

      int[] dictDims = new int[dimensionCount - noDictionaryCount];
      byte[][] nonDictArray = new byte[noDictionaryCount + complexDimensionCount][];
      Object[] measures = new Object[measureCount];

      // write dict dim data
      for (int idx = 0; idx < dictDimIdx.length; idx++) {
        dictDims[idx] = (int) data[dictDimIdx[idx]];
      }

      // write non dict dim data
      for (int idx = 0; idx < nonDictIdx.length; idx++) {
        nonDictArray[idx] = (byte[]) data[nonDictIdx[idx]];
      }

      // write measure data
      for (int idx = 0; idx < measureIdx.length; idx++) {
        measures[idx] = data[measureIdx[idx]];
      }
      NonDictionaryUtil.prepareOutObj(holder, dictDims, nonDictArray, measures);

      // increment number if record read
    } catch (Exception e) {
      throw new RuntimeException("Problem while converting row ", e);
    }
    //return out row
    return holder;
  }
}
