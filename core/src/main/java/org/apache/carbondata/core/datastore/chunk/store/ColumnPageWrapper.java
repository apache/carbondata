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

package org.apache.carbondata.core.datastore.chunk.store;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.blocklet.PresenceMeta;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

public class ColumnPageWrapper implements DimensionColumnPage {

  private ColumnPage columnPage;

  private CarbonDictionary localDictionary;

  private boolean isAdaptivePrimitivePage;

  private int[] invertedIndex;

  private int[] invertedReverseIndex;

  private boolean isExplicitSorted;

  private PresenceMeta presenceMeta;

  public ColumnPageWrapper(ColumnPage columnPage, CarbonDictionary localDictionary,
      int[] invertedIndex, int[] invertedReverseIndex, boolean isAdaptivePrimitivePage,
      boolean isExplicitSorted) {
    this.columnPage = columnPage;
    this.localDictionary = localDictionary;
    this.invertedIndex = invertedIndex;
    this.invertedReverseIndex = invertedReverseIndex;
    this.isAdaptivePrimitivePage = isAdaptivePrimitivePage;
    if (null != columnPage) {
      this.presenceMeta = columnPage.getPresenceMeta();
    }
    this.isExplicitSorted = isExplicitSorted;
  }

  @Override
  public int fillRawData(int rowId, int offset, byte[] data) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    for (int i = offset; i < len; i++) {
      fillRow(i, vector, vectorOffset++);
    }
    return chunkIndex + 1;
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    for (int i = offset; i < len; i++) {
      fillRow(filteredRowId[i], vector, vectorOffset++);
    }
    return chunkIndex + 1;
  }

  @Override
  public byte[] getChunkData(int rowId) {
    byte[] nullBitSet = getNullBitSet(rowId, columnPage.getColumnSpec().getColumnType());
    if (nullBitSet != null) {
      // if this row is null, return default null represent in byte array
      return nullBitSet;
    } else {
      if (isExplicitSorted()) {
        rowId = getInvertedReverseIndex(rowId);
      }
      return getChunkDataInBytes(rowId);
    }
  }

  private byte[] getChunkDataInBytes(int rowId) {
    ColumnType columnType = columnPage.getColumnSpec().getColumnType();
    DataType srcDataType = columnPage.getColumnSpec().getSchemaDataType();
    DataType targetDataType = columnPage.getDataType();
    if (null != localDictionary) {
      return localDictionary
          .getDictionaryValue(CarbonUtil.getSurrogateInternal(columnPage.getBytes(rowId), 0, 3));
    } else if (columnType == ColumnType.COMPLEX_PRIMITIVE && isAdaptivePrimitivePage) {
      if (srcDataType == DataTypes.FLOAT) {
        float floatData = columnPage.getFloat(rowId);
        return ByteUtil.toXorBytes(floatData);
      } else if (srcDataType == DataTypes.DOUBLE) {
        double doubleData = columnPage.getDouble(rowId);
        return ByteUtil.toXorBytes(doubleData);
      } else if (DataTypes.isDecimal(srcDataType)) {
        throw new RuntimeException("unsupported type: " + srcDataType);
      } else if ((srcDataType == DataTypes.BYTE) || (srcDataType == DataTypes.BOOLEAN) || (
          srcDataType == DataTypes.SHORT) || (srcDataType == DataTypes.SHORT_INT) || (srcDataType
          == DataTypes.INT) || (srcDataType == DataTypes.LONG) || (srcDataType
          == DataTypes.TIMESTAMP)) {
        long longData = columnPage.getLong(rowId);
        if ((srcDataType == DataTypes.BYTE)) {
          byte out = (byte) longData;
          return new byte[] { out };
        } else if (srcDataType == DataTypes.BOOLEAN) {
          byte out = (byte) longData;
          return ByteUtil.toBytes(ByteUtil.toBoolean(out));
        } else if (srcDataType == DataTypes.SHORT) {
          short out = (short) longData;
          return ByteUtil.toXorBytes(out);
        } else if (srcDataType == DataTypes.SHORT_INT) {
          int out = (int) longData;
          return ByteUtil.toXorBytes(out);
        } else if (srcDataType == DataTypes.INT) {
          int out = (int) longData;
          return ByteUtil.toXorBytes(out);
        } else {
          // timestamp and long
          return ByteUtil.toXorBytes(longData);
        }
      } else if ((targetDataType == DataTypes.STRING) || (targetDataType == DataTypes.VARCHAR) || (
          targetDataType == DataTypes.BYTE_ARRAY)) {
        return columnPage.getBytes(rowId);
      } else {
        throw new RuntimeException("unsupported type: " + targetDataType);
      }
    } else if (columnType == ColumnType.COMPLEX_PRIMITIVE) {
      if ((srcDataType == DataTypes.BYTE) || (srcDataType == DataTypes.BOOLEAN)) {
        byte[] out = new byte[1];
        out[0] = (columnPage.getByte(rowId));
        return ByteUtil.toBytes(ByteUtil.toBoolean(out));
      } else if (srcDataType == DataTypes.BYTE_ARRAY) {
        return columnPage.getBytes(rowId);
      } else if (srcDataType == DataTypes.DOUBLE) {
        return ByteUtil.toXorBytes(columnPage.getDouble(rowId));
      } else if (srcDataType == DataTypes.FLOAT) {
        return ByteUtil.toXorBytes(columnPage.getFloat(rowId));
      } else if (srcDataType == targetDataType) {
        return columnPage.getBytes(rowId);
      } else {
        throw new RuntimeException("unsupported type: " + targetDataType);
      }
    } else {
      if ((srcDataType == DataTypes.BYTE)) {
        long longData = columnPage.getLong(rowId);
        byte out = (byte) longData;
        return new byte[] { out };
      } else if (srcDataType == DataTypes.BOOLEAN) {
        long longData = columnPage.getLong(rowId);
        byte out = (byte) longData;
        return ByteUtil.toBytes(ByteUtil.toBoolean(out));
      } else if (srcDataType == DataTypes.SHORT) {
        long longData = columnPage.getLong(rowId);
        short out = (short) longData;
        return ByteUtil.toXorBytes(out);
      } else if (srcDataType == DataTypes.SHORT_INT) {
        long longData = columnPage.getLong(rowId);
        int out = (int) longData;
        return ByteUtil.toXorBytes(out);
      } else if (srcDataType == DataTypes.INT) {
        long longData = columnPage.getLong(rowId);
        int out = (int) longData;
        return ByteUtil.toXorBytes(out);
      } else if (srcDataType == DataTypes.TIMESTAMP || srcDataType == DataTypes.LONG) {
        long longData = columnPage.getLong(rowId);
        return ByteUtil.toXorBytes(longData);
      } else {
        return columnPage.getBytes(rowId);
      }
    }
  }

  private byte[] getNullBitSet(int rowId, ColumnType columnType) {
    if (presenceMeta.getBitSet().get(rowId) && columnType == ColumnType.COMPLEX_PRIMITIVE) {
      // if this row is null, return default null represent in byte array
      return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    if (presenceMeta.getBitSet().get(rowId)) {
      // if this row is null, return default null represent in byte array
      return CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    }
    return null;
  }

  /**
   * Fill the data to the vector
   *
   * @param rowId
   * @param vector
   * @param vectorRow
   */
  private void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    if (columnPage.getNullBits().get(rowId)) {
      vector.putNull(vectorRow);
    } else {
      if (isExplicitSorted) {
        rowId = invertedReverseIndex[rowId];
      }
      DataType dt = vector.getType();
      long longData = columnPage.getLong(rowId);
      if (dt == DataTypes.BOOLEAN) {
        vector.putBoolean(vectorRow, ByteUtil.toBoolean((byte) longData));
      } else if (dt == DataTypes.BYTE) {
        vector.putByte(vectorRow, (byte) longData);
      } else if (dt == DataTypes.SHORT) {
        vector.putShort(vectorRow, (short) longData);
      } else if (dt == DataTypes.INT) {
        vector.putInt(vectorRow, (int) longData);
      } else if (dt == DataTypes.LONG) {
        // retrieving the data after change in data type restructure operation
        if (vector.getBlockDataType() == DataTypes.INT) {
          vector.putLong(vectorRow, (int) longData);
        } else if (vector.getBlockDataType() == DataTypes.LONG) {
          vector.putLong(vectorRow, longData);
        }
      } else if (dt == DataTypes.TIMESTAMP) {
        vector.putLong(vectorRow, longData * 1000L);
      } else {
        throw new RuntimeException("unsupported type: " + dt);
      }
    }
  }

  @Override
  public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override
  public int getInvertedReverseIndex(int rowId) {
    return invertedReverseIndex[rowId];
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
    // rowId is the inverted index, but the null bitset is based on actual data
    int nullBitSetRowId = rowId;
    if (isExplicitSorted()) {
      nullBitSetRowId = getInvertedReverseIndex(rowId);
    }
    byte[] nullBitSet = getNullBitSet(nullBitSetRowId, columnPage.getColumnSpec().getColumnType());
    if (nullBitSet != null
        && ByteUtil.UnsafeComparer.INSTANCE.compareTo(nullBitSet, (byte[]) compareValue) == 0) {
      // check if the compare value is a null value
      // if the compare value is null and the data is also null we can directly return 0
      return 0;
    } else {
      byte[] chunkData;
      if (nullBitSet != null && nullBitSet.length == 0) {
        chunkData = nullBitSet;
      } else {
        chunkData = this.getChunkDataInBytes(rowId);
      }
      return ByteUtil.UnsafeComparer.INSTANCE.compareTo(chunkData,  (byte[]) compareValue);
    }
  }

  @Override
  public void freeMemory() {
    if (null != columnPage) {
      columnPage.freeMemory();
      columnPage = null;
    }
  }

  @Override
  public boolean isAdaptiveEncoded() {
    return isAdaptivePrimitivePage;
  }

  @Override
  public PresenceMeta getPresentMeta() {
    return presenceMeta;
  }

}
