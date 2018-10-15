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
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Class responsible for processing short data type dimension column data
 */

public class ShortTypeDimColumnPage extends AbstractPrimitiveDimColumnPage {
  public ShortTypeDimColumnPage(ColumnPage columnPage, int[] actualRowId,
      int[] invertedIndexReverse) {
    super(columnPage, actualRowId, invertedIndexReverse);
  }

  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putShort(vectorOffset++, getDataBasedOnActualRowId(j));
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putShort(vectorOffset++, getDataBasedOnActualRowId(j));
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putShort(vectorOffset++, getDataBasedOnActualRowId(j));
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
        vector.putShort(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[j]));
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        int filteredIndex = filteredRowId[j];
        if (presenceMeta.getBitSet().get(filteredIndex)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putShort(vectorOffset++, getDataBasedOnActualRowId(filteredIndex));
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
          vector.putShort(vectorOffset++, getDataBasedOnActualRowId(filteredIndex));
        } else {
          vector.putNull(vectorOffset++);
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override public byte[] getChunkData(int rowId) {
    if ((presenceMeta.isNullBitset() && presenceMeta.getBitSet().get(rowId)) || (
        !presenceMeta.isNullBitset() && !presenceMeta.getBitSet().get(rowId))) {
      return CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    }
    if (isExplictSorted) {
      rowId = currentRowId[rowId];
    }
    return ByteUtil.toXorBytes((short) columnPage.getLong(rowId));
  }

  @Override public int compareTo(int rowId, Object compareValue) {
    byte[] data;
    int nullBitSetRowId = isExplictSorted ? actualRowId[rowId] : rowId;
    if ((presenceMeta.isNullBitset() && presenceMeta.getBitSet().get(nullBitSetRowId)) || (
        !presenceMeta.isNullBitset() && !presenceMeta.getBitSet().get(nullBitSetRowId))) {
      data = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    } else {
      data = ByteUtil.toXorBytes((short) columnPage.getLong(rowId));
    }
    return ByteUtil.UnsafeComparer.INSTANCE.compareTo(data, (byte[]) compareValue);
  }

  private short getDataBasedOnActualRowId(int rowId) {
    if (isExplictSorted) {
      return (short) columnPage.getLong(currentRowId[rowId]);
    }
    return (short) columnPage.getLong(rowId);
  }

}
