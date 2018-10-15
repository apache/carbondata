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

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.blocklet.PresenceMeta;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * Class responsible for processing local dictionary data type dimension column data
 */

public class LocalDictAdaptiveDimColumnPage implements DimensionColumnPage {

  /**
   * data page
   */
  private ColumnPage columnPage;

  /**
   * inverted index
   */
  private int[] invertedIndex;

  /**
   * inverted reverse index
   */
  private int[] invertedIndexReverse;

  /**
   * is data is explictly sorted
   */
  private boolean isExplictSorted;

  /**
   * local dictionary
   */
  private CarbonDictionary dictionary;

  private SerializableComparator serializableComparator;

  private PresenceMeta presenceMeta;

  public LocalDictAdaptiveDimColumnPage(ColumnPage columnPage, int[] invertedIndex,
      int[] invertedIndexReverse, CarbonDictionary dictionary) {
    this.columnPage = columnPage;
    this.invertedIndex = invertedIndex;
    this.invertedIndexReverse = invertedIndexReverse;
    this.isExplictSorted = null != invertedIndex && invertedIndex.length > 0;
    this.dictionary = dictionary;
    this.serializableComparator = Comparator.getComparator(DataTypes.INT);
    this.presenceMeta = columnPage.getPresenceMeta();
  }

  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    return 0;
  }

  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
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
    CarbonColumnVector vector = columnVectorInfo.vector.getDictionaryVector();
    if (!dictionary.isDictionaryUsed()) {
      vector.setDictionary(dictionary);
      dictionary.setDictionaryUsed();
    }
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int i = offset; i < len; i++) {
        vector.putInt(vectorOffset++, getDataBasedOnActualRowId(i));
      }
    } else if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int i = offset; i < len; i++) {
        if (presenceMeta.getBitSet().get(i)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(i));
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int i = offset; i < len; i++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int i = offset; i < len; i++) {
        if (presenceMeta.getBitSet().get(i)) {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(i));
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
    CarbonColumnVector vector = columnVectorInfo.vector.getDictionaryVector();
    if (!dictionary.isDictionaryUsed()) {
      vector.setDictionary(dictionary);
      dictionary.setDictionaryUsed();
    }
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int i = offset; i < len; i++) {
        vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[i]));
      }
    } else if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int i = offset; i < len; i++) {
        int filteredRow = filteredRowId[i];
        if (presenceMeta.getBitSet().get(filteredRow)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putInt(vectorOffset, getDataBasedOnActualRowId(filteredRowId[i]));
        }
        vectorOffset++;
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int i = offset; i < len; i++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int i = offset; i < len; i++) {
        int filteredRow = filteredRowId[i];
        if (presenceMeta.getBitSet().get(filteredRow)) {
          vector.putInt(vectorOffset, getDataBasedOnActualRowId(filteredRowId[i]));
        } else {
          vector.putNull(vectorOffset);
        }
        vectorOffset++;
      }
    }
    return chunkIndex + 1;
  }

  @Override public byte[] getChunkData(int rowId) {
    return dictionary.getDictionaryValue((int) columnPage.getLong(rowId));
  }

  @Override public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return invertedIndexReverse[rowId];
  }

  @Override public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override public boolean isExplicitSorted() {
    return isExplictSorted;
  }

  @Override public int compareTo(int rowId, Object compareValue) {
    return this.serializableComparator.compare((int) columnPage.getLong(rowId), compareValue);
  }

  @Override public void freeMemory() {
    if (null != columnPage) {
      columnPage.freeMemory();
      this.invertedIndexReverse = null;
      this.invertedIndex = null;
      columnPage = null;
    }
  }

  @Override public boolean isAdaptiveEncoded() {
    return true;
  }

  @Override public PresenceMeta getPresentMeta() {
    return presenceMeta;
  }

}
