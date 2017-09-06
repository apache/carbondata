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

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * ColumnPage wrapper for dimension column reader
 */
public class ColumnPageWrapper implements DimensionColumnDataChunk {

  private TableSpec.ColumnSpec columnSpec;
  private ColumnPage columnPage;
  private int columnValueSize;

  public ColumnPageWrapper(ColumnPage columnPage, int columnValueSize) {
    this.columnPage = columnPage;
    this.columnValueSize = columnValueSize;
    this.columnSpec = columnPage.getColumnSpec();
  }

  @Override
  public int fillChunkData(byte[] data, int offset, int columnIndex,
      KeyStructureInfo restructuringInfo) {
    int surrogate = columnPage.getInt(columnIndex);
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.putInt(offset, surrogate);
    return columnValueSize;
  }

  @Override
  public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    row[columnIndex] = columnPage.getInt(rowId);
    return columnIndex + 1;
  }

  @Override
  public int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    // fill the vector with data in column page
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    CarbonColumnVector vector = columnVectorInfo.vector;
    fillData(null, columnVectorInfo, vector);
    return column + 1;
  }

  @Override
  public int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    CarbonColumnVector vector = columnVectorInfo.vector;
    fillData(rowMapping, columnVectorInfo, vector);
    return column + 1;
  }

  /**
   * Copy columnar data from internal column page to `vector`, row information described by
   * `columnVectorInfo`
   */
  private void fillData(int[] rowMapping, ColumnVectorInfo columnVectorInfo,
      CarbonColumnVector vector) {
    int offsetRowId = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int maxRowId = offsetRowId + columnVectorInfo.size;
    BitSet nullBitset = columnPage.getNullBits();
    switch (columnSpec.getColumnType()) {
      case DIRECT_DICTIONARY:
        DirectDictionaryGenerator generator = columnVectorInfo.directDictionaryGenerator;
        assert (generator != null);
        DataType dataType = generator.getReturnType();
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          int currentRowId = (rowMapping == null) ? rowId : rowMapping[rowId];
          if (nullBitset.get(currentRowId)) {
            vector.putNull(vectorOffset++);
          } else {
            int surrogate = columnPage.getInt(currentRowId);
            Object valueFromSurrogate = generator.getValueFromSurrogate(surrogate);
            if (valueFromSurrogate == null) {
              vector.putNull(vectorOffset++);
            } else {
              if (dataType == DataType.INT) {
                vector.putInt(vectorOffset++, (int) valueFromSurrogate);
              } else {
                vector.putLong(vectorOffset++, (long) valueFromSurrogate);
              }
            }
          }
        }
        break;
      case GLOBAL_DICTIONARY:
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          int currentRowId = (rowMapping == null) ? rowId : rowMapping[rowId];
          if (nullBitset.get(currentRowId)) {
            vector.putNull(vectorOffset++);
          } else {
            int data = columnPage.getInt(currentRowId);
            vector.putInt(vectorOffset++, data);
          }
        }
        break;
      case PLAIN_VALUE:
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          int currentRowId = (rowMapping == null) ? rowId : rowMapping[rowId];
          if (nullBitset.get(currentRowId)) {
            vector.putNull(vectorOffset++);
          } else {
            if (columnSpec.getSchemaDataType() == DataType.STRING) {
              byte[] data = columnPage.getBytes(currentRowId);
              if (isNullPlainValue(data)) {
                vector.putNull(vectorOffset++);
              } else {
                vector.putBytes(vectorOffset++, 0, data.length, data);
              }
            } else if (columnSpec.getSchemaDataType() == DataType.INT) {
              int data = columnPage.getInt(currentRowId);
              vector.putInt(vectorOffset++, data);
            }
          }
        }
        break;
      default:
        throw new RuntimeException("unsupported data type: " + columnPage.getDataType());
    }
  }

  private boolean isNullPlainValue(byte[] data) {
    return ByteUtil.UnsafeComparer.INSTANCE
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, data);
  }

  /**
   * Return dimension data as byte[] at specified rowId
   */
  @Override
  public byte[] getChunkData(int rowId) {
    ColumnType columnType = columnPage.getColumnSpec().getColumnType();
    if (columnType == ColumnType.DIRECT_DICTIONARY) {
      int data = columnPage.getInt(rowId);
      return ByteUtil.toBytes(data);
    } else if (columnType == ColumnType.PLAIN_VALUE) {
      if (columnPage.getNullBits().get(rowId)) {
        // if this row is null, return default null represent in byte array
        return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      }
      switch (columnPage.getDataType()) {
        case BYTE:
          byte byteData = columnPage.getByte(rowId);
          return ByteUtil.toBytesForPlainValue(byteData);
        case SHORT:
          short shortData = columnPage.getShort(rowId);
          return ByteUtil.toBytesForPlainValue(shortData);
        case INT:
          int intData = columnPage.getInt(rowId);
          return ByteUtil.toBytesForPlainValue(intData);
        case LONG:
          long longData = columnPage.getLong(rowId);
          return ByteUtil.toBytesForPlainValue(longData);
        case STRING:
          return columnPage.getBytes(rowId);
        default:
          throw new RuntimeException("unsupported type: " + columnPage.getDataType());
      }
    } else if (columnType == ColumnType.GLOBAL_DICTIONARY) {
      return columnPage.getBytes(rowId);
    } else {
      throw new RuntimeException("internal error");
    }
  }

  @Override
  public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override
  public int getColumnValueSize() {
    return columnValueSize;
  }

  @Override
  public boolean isExplicitSorted() {
    return false;
  }

  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    if (columnPage.getColumnSpec().getColumnType() == ColumnType.DIRECT_DICTIONARY) {
      int surrogate = columnPage.getInt(rowId);
      int input = ByteBuffer.wrap(compareValue).getInt();
      return surrogate - input;
    } else {
      byte[] data;
      if (columnPage.getDataType() == DataType.INT) {
        data = ByteUtil.toBytesForPlainValue(columnPage.getInt(rowId));
      } else if (columnPage.getDataType() == DataType.STRING) {
        data = columnPage.getBytes(rowId);
      } else {
        throw new RuntimeException("invalid data type for dimension: " + columnPage.getDataType());
      }
      return ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(data, 0, data.length, compareValue, 0, compareValue.length);
    }
  }

  /**
   * return true is data is not present at rowId
   */
  @Override
  public boolean isNull(int rowId) {
    return columnPage.getNullBits().get(rowId);
  }

  @Override
  public void freeMemory() {
    columnPage.freeMemory();
  }

}
