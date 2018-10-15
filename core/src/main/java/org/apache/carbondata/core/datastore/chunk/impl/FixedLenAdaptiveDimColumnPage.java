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
package org.apache.carbondata.core.datastore.chunk.impl;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.blocklet.PresenceMeta;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * Class responsible for processing global dictionary data type dimension column data
 */
public class FixedLenAdaptiveDimColumnPage implements DimensionColumnPage {

  protected PresenceMeta presenceMeta;
  /**
   * data page
   */
  protected ColumnPage columnPage;
  /**
   * inverted index
   */
  private int[] invertedIndex;
  /**
   * inverted reverse index
   */
  private int[] invertedIndexReverse;

  /**
   * size of each row
   */
  private int eachRowSize;

  /**
   * whether data was already sorted
   */
  protected boolean isExplictSorted;

  /**
   * data comparator
   */
  private SerializableComparator serializableComparator;

  public FixedLenAdaptiveDimColumnPage(ColumnPage columnPage, int[] invertedIndex,
      int[] invertedIndexReverse, int eachRowSize) {
    this.isExplictSorted = null != invertedIndex && invertedIndex.length > 0;
    this.presenceMeta = columnPage.getPresenceMeta();
    this.eachRowSize = eachRowSize;
    this.columnPage = columnPage;
    this.invertedIndex = invertedIndex;
    this.invertedIndexReverse = invertedIndexReverse;
    this.serializableComparator = Comparator.getComparator(DataTypes.INT);
  }

  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    if ((presenceMeta.isNullBitset() && presenceMeta.getBitSet().get(rowId))
        || (!presenceMeta.isNullBitset() && !presenceMeta.getBitSet().get(rowId))) {
      fillDataInternal(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY, data, offset);
    } else {
      fillDataInternal(getDataBasedOnActualRowId(rowId), data, offset);
    }
    return eachRowSize;
  }

  /**
   * Below method will be used to fill data for dictionary and direct dictionary column
   *
   * @param surrogate dictionary key
   * @param data      output array
   * @param offset    start offset
   */
  private void fillDataInternal(int surrogate, byte[] data, int offset) {
    switch (eachRowSize) {
      case 1:
        data[offset] = (byte) surrogate;
        break;
      case 2:
        data[offset + 1] = (byte) surrogate;
        surrogate >>>= 8;
        data[offset] = (byte) surrogate;
        break;
      case 3:
        data[offset + 2] = (byte) surrogate;
        surrogate >>>= 8;
        data[offset + 1] = (byte) surrogate;
        surrogate >>>= 8;
        data[offset] = (byte) surrogate;
        break;
      default:
        data[offset + 3] = (byte) surrogate;
        surrogate >>>= 8;
        data[offset + 2] = (byte) surrogate;
        surrogate >>>= 8;
        data[offset + 1] = (byte) surrogate;
        surrogate >>>= 8;
        data[offset] = (byte) surrogate;
    }
  }

  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    if (isExplictSorted) {
      rowId = getInvertedReverseIndex(rowId);
    }
    outputSurrogateKey[chunkIndex] = getDataBasedOnActualRowId(rowId);
    return chunkIndex + 1;
  }

  private int getDataBasedOnActualRowId(int rowId) {
    if (isExplictSorted) {
      return (int) columnPage.getLong(getInvertedReverseIndex(rowId));
    }
    return (int) columnPage.getLong(rowId);
  }

  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putInt(vectorOffset++, getDataBasedOnActualRowId(j));
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(j));
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(j));
        } else {
          vector.putNull(vectorOffset++);
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[j]));
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        int filteredIndex = filteredRowId[j];
        if (presenceMeta.getBitSet().get(filteredIndex)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredIndex));
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int j = offset; j < len; j++) {
        int filteredIndex = filteredRowId[j];
        if (presenceMeta.getBitSet().get(filteredIndex)) {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredIndex));
        } else {
          vector.putNull(vectorOffset++);
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override public byte[] getChunkData(int rowId) {
    byte[] data = new byte[eachRowSize];
    if ((presenceMeta.isNullBitset() && presenceMeta.getBitSet().get(rowId))
        || (!presenceMeta.isNullBitset() && !presenceMeta.getBitSet().get(rowId))) {
      fillDataInternal(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY, data, 0);
    } else {
      fillDataInternal(getDataBasedOnActualRowId(rowId), data, 0);
    }
    return data;
  }

  @Override public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return invertedIndexReverse[rowId];
  }

  @Override public boolean isNoDicitionaryColumn() {
    return false;
  }

  @Override public boolean isExplicitSorted() {
    return isExplictSorted;
  }

  @Override public int compareTo(int rowId, Object compareValue) {
    return this.serializableComparator.compare((int) columnPage.getLong(rowId), compareValue);
  }

  @Override public void freeMemory() {
    this.columnPage.freeMemory();
    this.invertedIndexReverse = null;
    this.invertedIndex = null;
  }

  @Override public boolean isAdaptiveEncoded() {
    return true;
  }

  @Override public PresenceMeta getPresentMeta() {
    return presenceMeta;
  }
}
