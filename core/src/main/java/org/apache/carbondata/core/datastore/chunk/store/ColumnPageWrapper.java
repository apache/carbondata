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

import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;


public class ColumnPageWrapper implements DimensionColumnPage {

  private ColumnPage columnPage;

  private CarbonDictionary localDictionary;

  private boolean isAdaptivePrimitivePage;

  private BitSet nullBitset;

  public ColumnPageWrapper(ColumnPage columnPage, CarbonDictionary localDictionary,
      boolean isAdaptivePrimitivePage) {
    this.columnPage = columnPage;
    this.localDictionary = localDictionary;
    this.isAdaptivePrimitivePage = isAdaptivePrimitivePage;
    this.nullBitset = columnPage.getNullBits();
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
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  /**
   * Fill the data to the vector
   *
   * @param rowId
   * @param vector
   * @param vectorRow
   */
  private void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  @Override public byte[] getChunkData(int rowId) {
    if (nullBitset.get(rowId)
        && columnPage.getColumnSpec().getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      // if this row is null, return default null represent in byte array
      return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    return getChunkDataInBytes(rowId);
  }

  private byte[] getChunkDataInBytes(int rowId) {
    ColumnType columnType = columnPage.getColumnSpec().getColumnType();
    DataType srcDataType = columnPage.getColumnSpec().getSchemaDataType();
    DataType targetDataType = columnPage.getDataType();
    if (null != localDictionary) {
      return localDictionary
          .getDictionaryValue(CarbonUtil.getSurrogateInternal(columnPage.getBytes(rowId), 0, 3));
    } else if ((columnType == ColumnType.COMPLEX_PRIMITIVE && isAdaptivePrimitivePage)) {
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
    } else if ((columnType == ColumnType.COMPLEX_PRIMITIVE)) {
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
      return columnPage.getBytes(rowId);
    }
  }

  private byte[] getNullBitSet(int rowId, ColumnType columnType) {
    if (columnPage.getNullBits().get(rowId) && columnType == ColumnType.COMPLEX_PRIMITIVE) {
      // if this row is null, return default null represent in byte array
      return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    if (columnPage.getNullBits().get(rowId)) {
      // if this row is null, return default null represent in byte array
      return CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    }
    return null;
  }

  private Object getActualData(int rowId, boolean isRowIdChanged) {
    ColumnType columnType = columnPage.getColumnSpec().getColumnType();
    DataType srcDataType = columnPage.getColumnSpec().getSchemaDataType();
    DataType targetDataType = columnPage.getDataType();
    if (null != localDictionary) {
      return localDictionary
          .getDictionaryValue(CarbonUtil.getSurrogateInternal(columnPage.getBytes(rowId), 0, 3));
    } else if ((columnType == ColumnType.COMPLEX_PRIMITIVE && this.isAdaptiveEncoded()) || (
        columnType == ColumnType.PLAIN_VALUE && DataTypeUtil.isPrimitiveColumn(srcDataType))) {
      if (!isRowIdChanged && columnPage.getNullBits().get(rowId)
          && columnType == ColumnType.COMPLEX_PRIMITIVE) {
        // if this row is null, return default null represent in byte array
        return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      }
      if (!isRowIdChanged && columnPage.getNullBits().get(rowId)) {
        // if this row is null, return default null represent in byte array
        return CarbonCommonConstants.EMPTY_BYTE_ARRAY;
      }
      if (srcDataType == DataTypes.DOUBLE || srcDataType == DataTypes.FLOAT) {
        double doubleData = columnPage.getDouble(rowId);
        if (srcDataType == DataTypes.FLOAT) {
          return (float) doubleData;
        } else {
          return doubleData;
        }
      } else if (DataTypes.isDecimal(srcDataType)) {
        throw new RuntimeException("unsupported type: " + srcDataType);
      } else if ((srcDataType == DataTypes.BYTE) || (srcDataType == DataTypes.BOOLEAN) || (
          srcDataType == DataTypes.SHORT) || (srcDataType == DataTypes.SHORT_INT) || (srcDataType
          == DataTypes.INT) || (srcDataType == DataTypes.LONG) || (srcDataType
          == DataTypes.TIMESTAMP)) {
        long longData = columnPage.getLong(rowId);
        if ((srcDataType == DataTypes.BYTE)) {
          return (byte) longData;
        } else if (srcDataType == DataTypes.BOOLEAN) {
          byte out = (byte) longData;
          return ByteUtil.toBoolean(out);
        } else if (srcDataType == DataTypes.SHORT) {
          return (short) longData;
        } else if (srcDataType == DataTypes.SHORT_INT) {
          return (int) longData;
        } else if (srcDataType == DataTypes.INT) {
          return (int) longData;
        } else {
          // timestamp and long
          return longData;
        }
      } else if ((targetDataType == DataTypes.STRING) || (targetDataType == DataTypes.VARCHAR) || (
          targetDataType == DataTypes.BYTE_ARRAY)) {
        return columnPage.getBytes(rowId);
      } else {
        throw new RuntimeException("unsupported type: " + targetDataType);
      }
    } else if ((columnType == ColumnType.COMPLEX_PRIMITIVE && !this.isAdaptiveEncoded())) {
      if (!isRowIdChanged && columnPage.getNullBits().get(rowId)) {
        return CarbonCommonConstants.EMPTY_BYTE_ARRAY;
      }
      if ((srcDataType == DataTypes.BYTE) || (srcDataType == DataTypes.BOOLEAN)) {
        byte[] out = new byte[1];
        out[0] = (columnPage.getByte(rowId));
        return ByteUtil.toBoolean(out);
      } else if (srcDataType == DataTypes.BYTE_ARRAY) {
        return columnPage.getBytes(rowId);
      } else if (srcDataType == DataTypes.DOUBLE) {
        return columnPage.getDouble(rowId);
      } else if (srcDataType == targetDataType) {
        return columnPage.getBytes(rowId);
      } else {
        throw new RuntimeException("unsupported type: " + targetDataType);
      }
    } else {
      return columnPage.getBytes(rowId);
    }
  }

  @Override
  public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  @Override
  public int getInvertedReverseIndex(int rowId) {
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  @Override
  public boolean isNoDicitionaryColumn() {
    return false;
  }

  @Override
  public boolean isExplicitSorted() {
    return false;
  }

  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  @Override
  public void freeMemory() {
    if (null != columnPage) {
      columnPage.freeMemory();
      columnPage = null;
    }
  }

  @Override public boolean isAdaptiveEncoded() {
    throw new UnsupportedOperationException("Operation not supported for complex type");
  }

  @Override public BitSet getNullBits() {
    return columnPage.getNullBits();
  }
}