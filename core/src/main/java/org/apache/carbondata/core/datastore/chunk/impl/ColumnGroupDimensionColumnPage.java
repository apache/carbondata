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

import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory.DimensionStoreType;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * This class is gives access to column group dimension data chunk store
 */
public class ColumnGroupDimensionColumnPage extends AbstractDimensionColumnPage {

  /**
   * Constructor for this class
   *
   * @param dataChunk       data chunk
   * @param columnValueSize chunk attributes
   * @param numberOfRows
   */
  public ColumnGroupDimensionColumnPage(byte[] dataChunk, int columnValueSize, int numberOfRows) {
    this.dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(columnValueSize, false, numberOfRows, dataChunk.length,
        DimensionStoreType.FIXEDLENGTH);
    this.dataChunkStore.putArray(null, null, dataChunk);
  }

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param rowId             row id of the chunk
   * @param offset            offset from which data need to be filed
   * @param data              data to filed
   * @param restructuringInfo define the structure of the key
   * @return how many bytes was copied
   */
  @Override public int fillRawData(int rowId, int offset, byte[] data,
      KeyStructureInfo restructuringInfo) {
    byte[] row = dataChunkStore.getRow(rowId);
    byte[] maskedKey = getMaskedKey(row, restructuringInfo);
    System.arraycopy(maskedKey, 0, data, offset, maskedKey.length);
    return maskedKey.length;
  }

  /**
   * Converts to column dictionary integer value
   *
   * @param rowId
   * @param chunkIndex
   * @param outputSurrogateKey
   * @param info          KeyStructureInfo
   * @return
   */
  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey,
      KeyStructureInfo info) {
    byte[] data = dataChunkStore.getRow(rowId);
    long[] keyArray = info.getKeyGenerator().getKeyArray(data);
    int[] ordinal = info.getMdkeyQueryDimensionOrdinal();
    for (int i = 0; i < ordinal.length; i++) {
      outputSurrogateKey[chunkIndex++] = (int) keyArray[ordinal[i]];
    }
    return chunkIndex;
  }

  /**
   * Below method will be used to get the masked key
   *
   * @param data   data
   * @param info
   * @return
   */
  private byte[] getMaskedKey(byte[] data, KeyStructureInfo info) {
    byte[] maskedKey = new byte[info.getMaskByteRanges().length];
    int counter = 0;
    int byteRange = 0;
    for (int i = 0; i < info.getMaskByteRanges().length; i++) {
      byteRange = info.getMaskByteRanges()[i];
      maskedKey[counter++] = (byte) (data[byteRange] & info.getMaxKey()[byteRange]);
    }
    return maskedKey;
  }

  /**
   * @return inverted index
   */
  @Override public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("Operation not supported in case of cloumn group");
  }

  /**
   * @param rowId
   * @return inverted index reverse
   */
  @Override public int getInvertedReverseIndex(int rowId) {
    throw new UnsupportedOperationException("Operation not supported in case of cloumn group");
  }

  /**
   * @return whether columns where explictly sorted or not
   */
  @Override public boolean isExplicitSorted() {
    return false;
  }

  /**
   * to compare the data
   *
   * @param rowId        row index to be compared
   * @param compareValue value to compare
   * @return compare result
   */
  @Override public int compareTo(int rowId, byte[] compareValue) {
    throw new UnsupportedOperationException("Operation not supported in case of cloumn group");
  }

  /**
   * Fill the data to vector
   *
   * @param vectorInfo
   * @param chunkIndex
   * @param restructuringInfo
   * @return next column index
   */
  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    int[] ordinal = restructuringInfo.getMdkeyQueryDimensionOrdinal();
    for (int k = offset; k < len; k++) {
      long[] keyArray = restructuringInfo.getKeyGenerator().getKeyArray(dataChunkStore.getRow(k));
      int index = 0;
      for (int i = chunkIndex; i < chunkIndex + ordinal.length; i++) {
        if (vectorInfo[i].directDictionaryGenerator == null) {
          vectorInfo[i].vector.putInt(vectorOffset, (int) keyArray[ordinal[index++]]);
        } else {
          vectorInfo[i].vector.putLong(vectorOffset, (long) vectorInfo[i].directDictionaryGenerator
              .getValueFromSurrogate((int) keyArray[ordinal[index++]]));
        }
      }
      vectorOffset++;
    }
    return chunkIndex + ordinal.length;
  }

  /**
   * Fill the data to vector
   *
   * @param filteredRowId
   * @param vectorInfo
   * @param chunkIndex
   * @param restructuringInfo
   * @return next column index
   */
  @Override public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo,
      int chunkIndex, KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    int[] ordinal = restructuringInfo.getMdkeyQueryDimensionOrdinal();
    for (int k = offset; k < len; k++) {
      long[] keyArray =
          restructuringInfo.getKeyGenerator().getKeyArray(dataChunkStore.getRow(filteredRowId[k]));
      int index = 0;
      for (int i = chunkIndex; i < chunkIndex + ordinal.length; i++) {
        if (vectorInfo[i].directDictionaryGenerator == null) {
          vectorInfo[i].vector.putInt(vectorOffset, (int) keyArray[ordinal[index++]]);
        } else {
          vectorInfo[i].vector.putLong(vectorOffset, (long) vectorInfo[i].directDictionaryGenerator
              .getValueFromSurrogate((int) keyArray[ordinal[index++]]));
        }
      }
      vectorOffset++;
    }
    return chunkIndex + ordinal.length;
  }
}
