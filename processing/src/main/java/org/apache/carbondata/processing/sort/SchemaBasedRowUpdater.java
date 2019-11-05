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

package org.apache.carbondata.processing.sort;

import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;

/**
 * Below class will be used to update the sort output row based on schema order during filnal merge
 * this is required because in case of older version(eg:1.1) alter add column was supported
 * only with sort columns and sort step will return the data based on
 * sort column order(sort columns first) so as writer step understand format based on schema order
 * so we need to arrange based on schema order
 */
public class SchemaBasedRowUpdater implements SortTempRowUpdater {

  private static final long serialVersionUID = -8864989617597611912L;

  private boolean isUpdateDictDims;

  private boolean isUpdateNonDictDims;

  private int[] dictDimActualPosition;

  private int[] noDictActualPosition;

  public SchemaBasedRowUpdater(int[] dictDimActualPosition, int[] noDictActualPosition,
      boolean isUpdateDictDims, boolean isUpdateNonDictDims) {
    this.dictDimActualPosition = dictDimActualPosition;
    this.noDictActualPosition = noDictActualPosition;
    this.isUpdateDictDims = isUpdateDictDims;
    this.isUpdateNonDictDims = isUpdateNonDictDims;
  }

  @Override
  public void updateSortTempRow(IntermediateSortTempRow intermediateSortTempRow) {
    int[] dictSortDims = intermediateSortTempRow.getDictSortDims();
    if (isUpdateDictDims) {
      int[] dimArrayNew = new int[intermediateSortTempRow.getDictSortDims().length];
      for (int i = 0; i < dictSortDims.length; i++) {
        dimArrayNew[dictDimActualPosition[i]] = dictSortDims[i];
      }
      dictSortDims = dimArrayNew;
    }
    Object[] noDictSortDims = intermediateSortTempRow.getNoDictSortDims();
    if (isUpdateNonDictDims) {
      Object[] noDictArrayNew = new Object[noDictSortDims.length];
      for (int i = 0; i < noDictArrayNew.length; i++) {
        noDictArrayNew[noDictActualPosition[i]] = noDictSortDims[i];
      }
      noDictSortDims = noDictArrayNew;
    }
    intermediateSortTempRow.setDictData(dictSortDims);
    intermediateSortTempRow.setNoDictData(noDictSortDims);
  }

  @Override
  public void updateOutputRow(Object[] out, int[] dimArray, Object[] noDictArray,
      Object[] measureArray) {
    if (isUpdateDictDims) {
      int[] dimArrayNew = new int[dimArray.length];
      for (int i = 0; i < dimArray.length; i++) {
        dimArrayNew[dictDimActualPosition[i]] = dimArray[i];
      }
      dimArray = dimArrayNew;
    }
    if (isUpdateNonDictDims) {
      Object[] noDictArrayNew = new Object[noDictArray.length];
      for (int i = 0; i < noDictArrayNew.length; i++) {
        noDictArrayNew[noDictActualPosition[i]] = noDictArray[i];
      }
      noDictArray = noDictArrayNew;
    }
    out[WriteStepRowUtil.DICTIONARY_DIMENSION] = dimArray;
    out[WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX] = noDictArray;
    out[WriteStepRowUtil.MEASURE] = measureArray;
  }
}
