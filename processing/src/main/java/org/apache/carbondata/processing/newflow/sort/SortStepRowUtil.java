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

package org.apache.carbondata.processing.newflow.sort;

import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

public class SortStepRowUtil {
  public static Object[] convertRow(Object[] data, SortParameters parameters,
      boolean needConvertDecimalToByte) {
    int measureCount = parameters.getMeasureColCount();
    int dimensionCount = parameters.getDimColCount();
    int complexDimensionCount = parameters.getComplexDimColCount();
    int noDictionaryCount = parameters.getNoDictionaryCount();
    boolean[] isNoDictionaryDimensionColumn = parameters.getNoDictionaryDimnesionColumn();

    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)

    Object[] holder = new Object[3];
    int index = 0;
    int nonDicIndex = 0;
    int allCount = 0;
    int[] dim = new int[dimensionCount];
    byte[][] nonDicArray = new byte[noDictionaryCount + complexDimensionCount][];
    Object[] measures = new Object[measureCount];
    try {
      // read dimension values
      for (int i = 0; i < isNoDictionaryDimensionColumn.length; i++) {
        if (isNoDictionaryDimensionColumn[i]) {
          nonDicArray[nonDicIndex++] = (byte[]) data[i];
        } else {
          dim[index++] = (int) data[allCount];
        }
        allCount++;
      }

      for (int i = 0; i < complexDimensionCount; i++) {
        nonDicArray[nonDicIndex++] = (byte[]) data[allCount];
        allCount++;
      }

      index = 0;

      // read measure values
      for (int i = 0; i < measureCount; i++) {
        if (needConvertDecimalToByte) {
          measures[index++] = data[allCount];
        } else {
          measures[index++] = data[allCount];
        }

        allCount++;
      }

      NonDictionaryUtil.prepareOutObj(holder, dim, nonDicArray, measures);

      // increment number if record read
    } catch (Exception e) {
      throw new RuntimeException("Problem while converting row ", e);
    }

    //return out row
    return holder;
  }
}
