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

import java.util.Arrays;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory.DimensionStoreType;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;

/**
 * This class is gives access to variable length dimension data chunk store
 */
public class VariableLengthDimensionDataChunk extends AbstractDimensionDataChunk {

  /**
   * Constructor for this class
   * @param dataChunks
   * @param invertedIndex
   * @param invertedIndexReverse
   * @param numberOfRows
   */
  public VariableLengthDimensionDataChunk(byte[] dataChunks, int[] invertedIndex,
      int[] invertedIndexReverse, int numberOfRows) {
    long totalSize = null != invertedIndex ?
        (dataChunks.length + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
            numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE)) :
        (dataChunks.length + (numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(0, null != invertedIndex, numberOfRows, totalSize,
            DimensionStoreType.VARIABLELENGTH);
    dataChunkStore.putArray(invertedIndex, invertedIndexReverse, dataChunks);
  }

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param data              data to filed
   * @param offset            offset from which data need to be filed
   * @param index             row id of the chunk
   * @param restructuringInfo define the structure of the key
   * @return how many bytes was copied
   */
  @Override public int fillChunkData(byte[] data, int offset, int index,
      KeyStructureInfo restructuringInfo) {
    // no required in this case because this column chunk is not the part if
    // mdkey
    return 0;
  }

  /**
   * Converts to column dictionary integer value
   *
   * @param rowId
   * @param columnIndex
   * @param row
   * @param restructuringInfo
   * @return
   */
  @Override public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    return columnIndex + 1;
  }

  /**
   * @return whether column is dictionary column or not
   */
  @Override public boolean isNoDicitionaryColumn() {
    return true;
  }

  /**
   * @return length of each column
   */
  @Override public int getColumnValueSize() {
    return -1;
  }

  /**
   * Fill the data to vector
   *
   * @param vectorInfo
   * @param column
   * @param restructuringInfo
   * @return next column index
   */
  @Override public int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    for (int i = offset; i < len; i++) {
      byte[] value = dataChunkStore.getRow(i);
      // Considering only String case now as we support only
      // string in no dictionary case at present.
      if (value == null || Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, value)) {
        vector.putNull(vectorOffset++);
      } else {
        DataType dt = vector.getType();
        if (dt instanceof StringType) {
          vector.putBytes(vectorOffset++, value);
        } else if (dt instanceof BooleanType) {
          vector.putBoolean(vectorOffset++, ByteUtil.toBoolean(value));
        } else if (dt instanceof ShortType) {
          vector.putShort(vectorOffset++, ByteUtil.toShort(value, 0, value.length));
        } else if (dt instanceof IntegerType) {
          vector.putInt(vectorOffset++, ByteUtil.toInt(value, 0, value.length));
        } else if (dt instanceof FloatType) {
          vector.putFloat(vectorOffset++, ByteUtil.toFloat(value, 0));
        } else if (dt instanceof DoubleType) {
          vector.putDouble(vectorOffset++, ByteUtil.toDouble(value, 0));
        } else if (dt instanceof LongType) {
          vector.putLong(vectorOffset++, ByteUtil.toLong(value, 0, value.length));
        } else if (dt instanceof DecimalType) {
          vector.putDecimal(vectorOffset++,
              Decimal.apply(ByteUtil.toBigDecimal(value, 0, value.length)),
              DecimalType.MAX_PRECISION());
        }
      }
    }
    return column + 1;
  }

  /**
   * Fill the data to vector
   *
   * @param rowMapping
   * @param vectorInfo
   * @param column
   * @param restructuringInfo
   * @return next column index
   */
  @Override public int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo,
      int column, KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    for (int i = offset; i < len; i++) {
      byte[] value = dataChunkStore.getRow(rowMapping[i]);
      // Considering only String case now as we support only
      // string in no dictionary case at present.
      if (value == null || Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, value)) {
        vector.putNull(vectorOffset++);
      } else {
        DataType dt = vector.getType();
        if (dt instanceof StringType) {
          vector.putBytes(vectorOffset++, value);
        } else if (dt instanceof BooleanType) {
          vector.putBoolean(vectorOffset++, ByteUtil.toBoolean(value));
        } else if (dt instanceof ShortType) {
          vector.putShort(vectorOffset++, ByteUtil.toShort(value, 0, value.length));
        } else if (dt instanceof IntegerType) {
          vector.putInt(vectorOffset++, ByteUtil.toInt(value, 0, value.length));
        } else if (dt instanceof FloatType) {
          vector.putFloat(vectorOffset++, ByteUtil.toFloat(value, 0));
        } else if (dt instanceof DoubleType) {
          vector.putDouble(vectorOffset++, ByteUtil.toDouble(value, 0));
        } else if (dt instanceof LongType) {
          vector.putLong(vectorOffset++, ByteUtil.toLong(value, 0, value.length));
        } else if (dt instanceof DecimalType) {
          vector.putDecimal(vectorOffset++,
              Decimal.apply(ByteUtil.toBigDecimal(value, 0, value.length)),
              DecimalType.MAX_PRECISION());
        }
      }
    }
    return column + 1;
  }
}
