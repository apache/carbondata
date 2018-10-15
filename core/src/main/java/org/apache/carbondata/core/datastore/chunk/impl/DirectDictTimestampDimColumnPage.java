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

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Class responsible for processing direct dictionary timestamp type dimension column data
 */
public class DirectDictTimestampDimColumnPage extends FixedLenAdaptiveDimColumnPage {

  public DirectDictTimestampDimColumnPage(ColumnPage columnPage, int[] invertedIndex,
      int[] invertedIndexReverse, int eachRowSize) {
    super(columnPage, invertedIndex, invertedIndexReverse, eachRowSize);
  }


  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int cutoffdate = Integer.MAX_VALUE >> 1;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putLong(vectorOffset++, getDataBasedOnActualRowId(j) - cutoffdate);
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putLong(vectorOffset++, getDataBasedOnActualRowId(j) - cutoffdate);
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putLong(vectorOffset++, getDataBasedOnActualRowId(j) - cutoffdate);
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
    int cutoffdate = Integer.MAX_VALUE >> 1;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putLong(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[j]) - cutoffdate);
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        int filteredIndex = filteredRowId[j];
        if (presenceMeta.getBitSet().get(filteredIndex)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putLong(vectorOffset++, getDataBasedOnActualRowId(filteredIndex) - cutoffdate);
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
          vector.putLong(vectorOffset++, getDataBasedOnActualRowId(filteredIndex) - cutoffdate);
        } else {
          vector.putNull(vectorOffset++);
        }
      }
    }
    return chunkIndex + 1;
  }

  private long getDataBasedOnActualRowId(int rowId) {
    if (isExplictSorted) {
      return columnPage.getLong(getInvertedReverseIndex(rowId));
    }
    return columnPage.getLong(rowId);
  }
}
