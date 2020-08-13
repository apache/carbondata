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

package org.apache.carbondata.core.scan.result.vector.impl.directread;

import java.util.BitSet;

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Factory to create ColumnarVectors for inverted index and delete delta queries.
 */
public final class ColumnarVectorWrapperDirectFactory {

  /**
   * Gets carbon vector wrapper to fill the underlying vector based on inverted index and delete
   * delta.
   *
   * @param vectorInfo       vectorInfo used to get the complex child vector
   * @param columnVector     Actual vector to be filled.
   * @param invertedIndex    Inverted index of column page
   * @param nullBitset       row locations of nulls in bitset
   * @param deletedRows      deleted rows locations in bitset.
   * @param isnullBitsExists whether nullBitset present on this page, usually for dimension columns
   *                         there is no null bitset.
   * @return wrapped CarbonColumnVector
   */
  public static CarbonColumnVector getDirectVectorWrapperFactory(ColumnVectorInfo vectorInfo,
      CarbonColumnVector columnVector, int[] invertedIndex, BitSet nullBitset, BitSet deletedRows,
      boolean isnullBitsExists, boolean isDictVector) {
    // If it is sequential data filler then add the null bitset.
    if (columnVector instanceof SequentialFill) {
      if (columnVector.getType().isComplexType() && !vectorInfo.vectorStack.isEmpty()) {
        // update to child vector, as it is child vector filling flow
        columnVector = vectorInfo.vectorStack.peek();
      }
      // If it has inverted index then create a dummy delete rows bitset so that it goes to
      // ColumnarVectorWrapperDirectWithDeleteDeltaAndInvertedIndex, here it does the sequential
      // filling using another vector.
      if ((invertedIndex != null && invertedIndex.length > 0)) {
        if (deletedRows == null) {
          deletedRows = new BitSet();
        }
      } else if (deletedRows == null) {
        ((SequentialFill) columnVector).setNullBits(nullBitset);
      }
    }
    if ((invertedIndex != null && invertedIndex.length > 0) && (deletedRows == null)) {
      return new ColumnarVectorWrapperDirectWithInvertedIndex(columnVector, invertedIndex,
          isnullBitsExists);
    } else if ((invertedIndex == null || invertedIndex.length == 0) && deletedRows != null) {
      return new ColumnarVectorWrapperDirectWithDeleteDelta(columnVector, deletedRows, nullBitset);
    } else if ((invertedIndex != null && invertedIndex.length > 0) && deletedRows != null) {
      return new ColumnarVectorWrapperDirectWithDeleteDeltaAndInvertedIndex(columnVector,
          deletedRows, invertedIndex, nullBitset, isnullBitsExists, isDictVector);
    } else {
      return columnVector;
    }
  }

}
