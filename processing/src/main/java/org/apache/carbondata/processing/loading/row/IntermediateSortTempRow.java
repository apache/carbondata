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
package org.apache.carbondata.processing.loading.row;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * During sort procedure, each row will be written to sort temp file in this logic format.
 * an intermediate sort temp row consists 3 parts:
 * dictSort, noDictSort, noSortDimsAndMeasures(dictNoSort, noDictNoSort, measure)
 */
public class IntermediateSortTempRow {
  private int[] dictSortDims;
  private byte[][] noDictSortDims;
  private byte[] noSortDimsAndMeasures;

  public IntermediateSortTempRow(int[] dictSortDims, byte[][] noDictSortDims,
      byte[] noSortDimsAndMeasures) {
    this.dictSortDims = dictSortDims;
    this.noDictSortDims = noDictSortDims;
    this.noSortDimsAndMeasures = noSortDimsAndMeasures;
  }

  public int[] getDictSortDims() {
    return dictSortDims;
  }

  public byte[][] getNoDictSortDims() {
    return noDictSortDims;
  }

  public byte[] getNoSortDimsAndMeasures() {
    return noSortDimsAndMeasures;
  }

  /**
   * deserialize from bytes array to get the no sort fields
   * @param outDictNoSort stores the dict & no-sort fields
   * @param outNoDictNoSort stores the no-dict & no-sort fields, including complex
   * @param outMeasures stores the measure fields
   * @param dataTypes data type for the measure
   */
  public void unpackNoSortFromBytes(int[] outDictNoSort, byte[][] outNoDictNoSort,
      Object[] outMeasures, DataType[] dataTypes) {
    ByteBuffer rowBuffer = ByteBuffer.wrap(noSortDimsAndMeasures);
    // read dict_no_sort
    int dictNoSortCnt = outDictNoSort.length;
    for (int i = 0; i < dictNoSortCnt; i++) {
      outDictNoSort[i] = rowBuffer.getInt();
    }

    // read no_dict_no_sort (including complex)
    int noDictNoSortCnt = outNoDictNoSort.length;
    for (int i = 0; i < noDictNoSortCnt; i++) {
      short len = rowBuffer.getShort();
      byte[] bytes = new byte[len];
      rowBuffer.get(bytes);
      outNoDictNoSort[i] = bytes;
    }

    // read measure
    int measureCnt = outMeasures.length;
    DataType tmpDataType;
    Object tmpContent;
    for (short idx = 0 ; idx < measureCnt; idx++) {
      if ((byte) 0 == rowBuffer.get()) {
        outMeasures[idx] = null;
        continue;
      }

      tmpDataType = dataTypes[idx];
      if (DataTypes.BOOLEAN == tmpDataType) {
        if ((byte) 1 == rowBuffer.get()) {
          tmpContent = true;
        } else {
          tmpContent = false;
        }
      } else if (DataTypes.SHORT == tmpDataType) {
        tmpContent = rowBuffer.getShort();
      } else if (DataTypes.INT == tmpDataType) {
        tmpContent = rowBuffer.getInt();
      } else if (DataTypes.LONG == tmpDataType) {
        tmpContent = rowBuffer.getLong();
      } else if (DataTypes.DOUBLE == tmpDataType) {
        tmpContent = rowBuffer.getDouble();
      } else if (DataTypes.isDecimal(tmpDataType)) {
        short len = rowBuffer.getShort();
        byte[] decimalBytes = new byte[len];
        rowBuffer.get(decimalBytes);
        tmpContent = DataTypeUtil.byteToBigDecimal(decimalBytes);
      } else {
        throw new IllegalArgumentException("Unsupported data type: " + tmpDataType);
      }
      outMeasures[idx] = tmpContent;
    }
  }


}
