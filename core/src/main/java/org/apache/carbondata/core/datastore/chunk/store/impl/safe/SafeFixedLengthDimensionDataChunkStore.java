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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used to store fixed length dimension data
 */
public class SafeFixedLengthDimensionDataChunkStore extends SafeAbsractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;

  private int numOfRows;

  public SafeFixedLengthDimensionDataChunkStore(boolean isInvertedIndex, int columnValueSize,
      int numOfRows) {
    super(isInvertedIndex);
    this.columnValueSize = columnValueSize;
    this.numOfRows = numOfRows;
  }

  @Override
  public void fillVector(int[] invertedIndex, int[] invertedIndexReverse, byte[] data,
      ColumnVectorInfo vectorInfo) {
    CarbonColumnVector vector = vectorInfo.vector;
    fillVector(data, vectorInfo, vector);
  }

  private void fillVector(byte[] data, ColumnVectorInfo vectorInfo, CarbonColumnVector vector) {
    DataType dataType = vectorInfo.vector.getBlockDataType();
    if (dataType == DataTypes.DATE) {
      for (int i = 0; i < numOfRows; i++) {
        int surrogateInternal =
            CarbonUtil.getSurrogateInternal(data, i * columnValueSize, columnValueSize);
        if (surrogateInternal == 1) {
          vector.putNull(i);
        } else {
          vector.putInt(i, surrogateInternal - DateDirectDictionaryGenerator.cutOffDate);
        }
      }
    } else if (dataType == DataTypes.TIMESTAMP) {
      for (int i = 0; i < numOfRows; i++) {
        int surrogateInternal =
            CarbonUtil.getSurrogateInternal(data, i * columnValueSize, columnValueSize);
        if (surrogateInternal == 1) {
          vector.putNull(i);
        } else {
          Object valueFromSurrogate =
              vectorInfo.directDictionaryGenerator.getValueFromSurrogate(surrogateInternal);
          vector.putLong(i, (long)valueFromSurrogate);
        }
      }
    } else {
      for (int i = 0; i < numOfRows; i++) {
        vector.putInt(i,
            CarbonUtil.getSurrogateInternal(data, i * columnValueSize, columnValueSize));
      }
    }
  }

  /**
   * Below method will be used to get the row based inverted index
   *
   * @param rowId Inverted index
   */
  @Override public byte[] getRow(int rowId) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplictSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    // creating a row
    byte[] row = new byte[columnValueSize];
    //copy the row from data chunk based on offset
    // offset position will be index * each column value length
    System.arraycopy(this.data, rowId * columnValueSize, row, 0, columnValueSize);
    return row;
  }

  /**
   * Below method will be used to get the surrogate key of the based on the row
   * id passed
   *
   * @param index row id
   * @return surrogate key
   */
  @Override public int getSurrogate(int index) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplictSorted) {
      index = invertedIndexReverse[index];
    }
    // below part is to convert the byte array to surrogate value
    int startOffsetOfData = index * columnValueSize;
    return CarbonUtil.getSurrogateInternal(data, startOffsetOfData, columnValueSize);
  }

  /**
   * Below method will be used to fill the row values to buffer array
   *
   * @param rowId  row id of the data to be filled
   * @param buffer   buffer in which data will be filled
   * @param offset off the of the buffer
   */
  @Override public void fillRow(int rowId, byte[] buffer, int offset) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplictSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    System.arraycopy(data, rowId * columnValueSize, buffer, offset, columnValueSize);
  }

  /**
   * @return size of each column value
   */
  @Override public int getColumnValueSize() {
    return columnValueSize;
  }

  /**
   * to compare the two byte array
   *
   * @param rowId        index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  @Override public int compareTo(int rowId, byte[] compareValue) {
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(data, rowId * columnValueSize, columnValueSize, compareValue, 0,
            columnValueSize);
  }

}
