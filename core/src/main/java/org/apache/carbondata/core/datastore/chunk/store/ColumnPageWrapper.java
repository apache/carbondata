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
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;


public class ColumnPageWrapper implements DimensionColumnPage {

  private ColumnPage columnPage;

  private CarbonDictionary localDictionary;

  private boolean isAdaptiveComplexPrimitivePage;

  public ColumnPageWrapper(ColumnPage columnPage, CarbonDictionary localDictionary,
      boolean isAdaptiveComplexPrimitivePage) {
    this.columnPage = columnPage;
    this.localDictionary = localDictionary;
    this.isAdaptiveComplexPrimitivePage = isAdaptiveComplexPrimitivePage;

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
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override public byte[] getChunkData(int rowId) {
    ColumnType columnType = columnPage.getColumnSpec().getColumnType();
    DataType srcDataType = columnPage.getColumnSpec().getSchemaDataType();
    DataType targetDataType = columnPage.getDataType();
    if (null != localDictionary) {
      return localDictionary
          .getDictionaryValue(CarbonUtil.getSurrogateInternal(columnPage.getBytes(rowId), 0, 3));
    } else if (columnType == ColumnType.COMPLEX_PRIMITIVE && this.isAdaptiveComplexPrimitive()) {
      if (columnPage.getNullBits().get(rowId)) {
        // if this row is null, return default null represent in byte array
        return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      }
      if (srcDataType == DataTypes.DOUBLE || srcDataType == DataTypes.FLOAT) {
        double doubleData = columnPage.getDouble(rowId);
        if (srcDataType == DataTypes.FLOAT) {
          float out = (float) doubleData;
          return ByteUtil.toBytes(out);
        } else {
          return ByteUtil.toBytes(doubleData);
        }
      } else if (DataTypes.isDecimal(srcDataType)) {
        throw new RuntimeException("unsupported type: " + srcDataType);
      } else if ((srcDataType == DataTypes.BYTE) || (srcDataType == DataTypes.BOOLEAN) || (
          srcDataType == DataTypes.SHORT) || (srcDataType == DataTypes.SHORT_INT) || (srcDataType
          == DataTypes.INT) || (srcDataType == DataTypes.LONG) || (srcDataType
          == DataTypes.TIMESTAMP)) {
        long longData = columnPage.getLong(rowId);
        if ((srcDataType == DataTypes.BYTE)) {
          byte out = (byte) longData;
          return ByteUtil.toBytes(out);
        } else if (srcDataType == DataTypes.BOOLEAN) {
          byte out = (byte) longData;
          return ByteUtil.toBytes(ByteUtil.toBoolean(out));
        } else if (srcDataType == DataTypes.SHORT) {
          short out = (short) longData;
          return ByteUtil.toBytes(out);
        } else if (srcDataType == DataTypes.SHORT_INT) {
          int out = (int) longData;
          return ByteUtil.toBytes(out);
        } else if (srcDataType == DataTypes.INT) {
          int out = (int) longData;
          return ByteUtil.toBytes(out);
        } else {
          // timestamp and long
          return ByteUtil.toBytes(longData);
        }
      } else if ((targetDataType == DataTypes.STRING) || (targetDataType == DataTypes.VARCHAR) || (
          targetDataType == DataTypes.BYTE_ARRAY)) {
        return columnPage.getBytes(rowId);
      } else {
        throw new RuntimeException("unsupported type: " + targetDataType);
      }
    } else if ((columnType == ColumnType.COMPLEX_PRIMITIVE) && !this.isAdaptiveComplexPrimitive()) {
      if ((srcDataType == DataTypes.BYTE) || (srcDataType == DataTypes.BOOLEAN)) {
        byte[] out = new byte[1];
        out[0] = (columnPage.getByte(rowId));
        return out;
      } else if (srcDataType == DataTypes.BYTE_ARRAY) {
        return columnPage.getBytes(rowId);
      }  else if (srcDataType == DataTypes.DOUBLE) {
        return ByteUtil.toBytes(columnPage.getDouble(rowId));
      } else {
        throw new RuntimeException("unsupported type: " + targetDataType);
      }
    } else {
      return columnPage.getBytes(rowId);
    }
  }


  @Override
  public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int getInvertedReverseIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override
  public boolean isExplicitSorted() {
    return false;
  }

  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void freeMemory() {
    if (null != columnPage) {
      columnPage.freeMemory();
      columnPage = null;
    }
  }

  public boolean isAdaptiveComplexPrimitive() {
    return isAdaptiveComplexPrimitivePage;
  }

}
