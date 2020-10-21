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
import org.apache.carbondata.core.datastore.blocklet.PresenceMeta;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Class responsible for processing String/Binary/Varchar data type dimension column data
 */
public class BinaryTypeDimColumnPage extends AbstractDimensionColumnPage {

  private final boolean isExplicitSorted;

  private int[] invertedIndex;

  private int[] invertedIndexReverse;

  private byte[] data;

  private final DataType lengthStoredType;

  private int[] dataOffsets;

  private final int numberOfRows;

  private final PresenceMeta presenceMeta;

  private int runningOffset;

  private final int dataTypeSize;

  public BinaryTypeDimColumnPage(byte[] data, int[] invertedIndex, int[] invertedIndexReverse,
      DataType lengthStoredType, int numberOfRows, PresenceMeta presenceMeta,
      ColumnVectorInfo vectorInfo, int dataLength,
      DimensionChunkStoreFactory.DimensionStoreType dimStoreType, CarbonDictionary dictionary,
      boolean isEncodedComplexChild) {
    this.lengthStoredType = lengthStoredType;
    this.numberOfRows = numberOfRows;
    this.data = data;
    this.invertedIndex = invertedIndex;
    this.invertedIndexReverse = invertedIndexReverse;
    this.isExplicitSorted = null != invertedIndex && invertedIndex.length > 0;
    this.presenceMeta = presenceMeta;
    dataTypeSize = lengthStoredType.getSizeInBytes();
    long totalSize;
    switch (dimStoreType) {
      case LOCAL_DICT:
        totalSize = null != invertedIndex ?
            (dataLength + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE)) :
            dataLength;
        break;
      case VARIABLE_INT_LENGTH:
      case VARIABLE_SHORT_LENGTH:
        totalSize = null != invertedIndex ?
            (dataLength + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
                numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE)) :
            (dataLength + (numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE));
        break;
      default:
        throw new UnsupportedOperationException("Invalidate dimension store type");
    }
    dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(0, isExplicitSorted, numberOfRows, totalSize, dimStoreType,
            dictionary, vectorInfo != null, dataLength, lengthStoredType);
    if (vectorInfo != null) {
      dataChunkStore.fillVector(invertedIndex, invertedIndexReverse, data, vectorInfo);
    }
  }

  @Override
  public int fillRawData(int rowId, int offset, byte[] data) {
    return 0;
  }

  @Override
  public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    return chunkIndex + 1;
  }

  @Override
  public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    // for full scan query without inverted index
    if (!isExplicitSorted) {
      int localOffset = runningOffset;
      if (lengthStoredType == DataTypes.BYTE) {
        if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
          for (int i = offset; i < len; i++) {
            byte length = data[localOffset];
            localOffset += 1;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else {
          for (int i = offset; i < len; i++) {
            if (presenceMeta.getBitSet().get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 1;
            } else {
              byte length = data[localOffset];
              localOffset += 1;
              vector.putBytes(vectorOffset, localOffset, data, length);
              localOffset += length;
            }
            vectorOffset++;
          }
        }
      } else if (lengthStoredType == DataTypes.SHORT) {
        if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
          for (int i = offset; i < len; i++) {
            int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
            localOffset += 2;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else {
          for (int i = offset; i < len; i++) {
            if (presenceMeta.getBitSet().get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 2;
            } else {
              int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
              localOffset += 2;
              vector.putBytes(vectorOffset, localOffset, data, length);
              localOffset += length;
            }
            vectorOffset++;
          }
        }
      } else if (lengthStoredType == DataTypes.SHORT_INT) {
        if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
          for (int i = offset; i < len; i++) {
            int length =
                (((data[localOffset] & 0xFF) << 16) | ((data[localOffset + 1] & 0xFF) << 8) | (
                    data[localOffset + 2] & 0xFF));
            localOffset += 3;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else {
          for (int i = offset; i < len; i++) {
            int length =
                (((data[localOffset] & 0xFF) << 16) | ((data[localOffset + 1] & 0xFF) << 8) | (
                    data[localOffset + 2] & 0xFF));
            localOffset += 3;
            if (presenceMeta.getBitSet().get(i)) {
              vector.putNull(vectorOffset);
            } else {
              vector.putByteArray(vectorOffset, localOffset, length, data);
            }
            localOffset += length;
            vectorOffset++;
          }
        }
      } else {
        if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
          for (int i = offset; i < len; i++) {
            int length =
                (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                    (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
            localOffset += 4;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else {
          for (int i = offset; i < len; i++) {
            if (presenceMeta.getBitSet().get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 4;
            } else {
              int length =
                  (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                      (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
              localOffset += 4;
              vector.putBytes(vectorOffset, localOffset, data, length);
              localOffset += length;
            }
            vectorOffset++;
          }
        }
      }
      runningOffset = localOffset;
    } else {
      // for inverted index generated data first calculate the offsets and then fill the vector
      if (null == dataOffsets) {
        dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
      }
      if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
        for (int i = offset; i < len; i++) {
          int actualIndex = getInvertedReverseIndex(i);
          int currentDataOffset = dataOffsets[actualIndex];
          int nextDataOffset = dataOffsets[actualIndex + 1];
          vector.putByteArray(vectorOffset++, currentDataOffset,
              nextDataOffset - currentDataOffset - dataTypeSize, data);
        }
      } else {
        for (int i = offset; i < len; i++) {
          if (presenceMeta.getBitSet().get(i)) {
            vector.putNull(vectorOffset);
          } else {
            int actualIndex = getInvertedReverseIndex(i);
            int currentDataOffset = dataOffsets[actualIndex];
            int nextDataOffset = dataOffsets[actualIndex + 1];
            vector.putBytes(vectorOffset, currentDataOffset, data,
                nextDataOffset - currentDataOffset - dataTypeSize);
          }
          vectorOffset++;
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    if (filteredRowId.length == numberOfRows) {
      fillVector(vectorInfo, chunkIndex);
    } else {
      ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
      CarbonColumnVector vector = columnVectorInfo.vector;
      int offset = columnVectorInfo.offset;
      int vectorOffset = columnVectorInfo.vectorOffset;
      int len = offset + columnVectorInfo.size;
      if (null == dataOffsets) {
        dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
      }
      if (!isExplicitSorted) {
        if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
          for (int i = offset; i < len; i++) {
            int filteredIndex = filteredRowId[i];
            int currentDataOffset = dataOffsets[filteredIndex];
            int nextDataOffset = dataOffsets[filteredIndex + 1];
            vector.putByteArray(vectorOffset++, currentDataOffset,
                nextDataOffset - currentDataOffset - dataTypeSize, data);
          }
        } else {
          for (int i = offset; i < len; i++) {
            int filteredIndex = filteredRowId[i];
            if (presenceMeta.getBitSet().get(filteredIndex)) {
              vector.putNull(vectorOffset);
            } else {
              int currentDataOffset = dataOffsets[filteredIndex];
              int nextDataOffset = dataOffsets[filteredIndex + 1];
              vector.putBytes(vectorOffset, currentDataOffset, data,
                  nextDataOffset - currentDataOffset - dataTypeSize);
            }
            vectorOffset++;
          }
        }
      } else {
        if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
          for (int i = offset; i < len; i++) {
            int filteredIndex = getInvertedReverseIndex(filteredRowId[i]);
            int currentDataOffset = dataOffsets[filteredIndex];
            int nextDataOffset = dataOffsets[filteredIndex + 1];
            vector.putBytes(vectorOffset++, currentDataOffset, data,
                nextDataOffset - currentDataOffset - dataTypeSize);
          }
        } else {
          for (int i = offset; i < len; i++) {
            int filteredIndex = filteredRowId[i];
            if (presenceMeta.getBitSet().get(filteredIndex)) {
              vector.putNull(vectorOffset);
            } else {
              int actualIndex = getInvertedReverseIndex(filteredRowId[i]);
              int currentDataOffset = dataOffsets[actualIndex];
              int nextDataOffset = dataOffsets[actualIndex + 1];
              vector.putBytes(vectorOffset, currentDataOffset, data,
                  nextDataOffset - currentDataOffset - dataTypeSize);
            }
            vectorOffset++;
          }
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override
  public byte[] getChunkData(int rowId) {
    if ((presenceMeta.isNullBitset() && presenceMeta.getBitSet().get(rowId)) || (
        !presenceMeta.isNullBitset() && !presenceMeta.getBitSet().get(rowId))) {
      return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    if (null == dataOffsets) {
      dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
    }
    if (isExplicitSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    int currentOffset = dataOffsets[rowId];
    int offsetOfNext = dataOffsets[rowId + 1];
    byte[] currentData = new byte[offsetOfNext - currentOffset - dataTypeSize];
    System.arraycopy(data, currentOffset, currentData, 0, currentData.length);
    return currentData;
  }

  @Override
  public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override
  public int getInvertedReverseIndex(int rowId) {
    return invertedIndexReverse[rowId];
  }

  @Override
  public boolean isNoDictionaryColumn() {
    return true;
  }

  @Override
  public boolean isExplicitSorted() {
    return isExplicitSorted;
  }

  @Override
  public int compareTo(int rowId, Object compareValue) {
    if (null == dataOffsets) {
      this.dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
    }
    int currentOffset = dataOffsets[rowId];
    int nextOffset = dataOffsets[rowId + 1];
    byte[] filterValue = (byte[]) compareValue;
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(data, currentOffset, nextOffset - currentOffset - dataTypeSize, filterValue, 0,
            filterValue.length);
  }

  @Override
  public void freeMemory() {
    if (null != data) {
      data = null;
      invertedIndex = null;
      invertedIndexReverse = null;
      dataOffsets = null;
    }
  }

  @Override
  public boolean isAdaptiveEncoded() {
    return false;
  }

  @Override
  public PresenceMeta getPresentMeta() {
    return presenceMeta;
  }
}