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

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Interface responsibility is to store dimension data in memory.
 * storage can be on heap or offheap.
 */
public interface DimensionDataChunkStore {

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  void putArray(int[] invertedIndex, int[] invertedIndexReverse, byte[] data);

  void putArray(int[] invertedIndex, int[] invertedIndexReverse, byte[] data, ColumnVectorInfo vectorInfo);

  /**
   * Below method will be used to get the row
   * based on row id passed
   *
   * @param rowId
   * @return row
   */
  byte[] getRow(int rowId);

  /**
   * Below method will be used to fill the row to vector
   * based on row id passed
   *
   */
  void fillRow(int rowId, CarbonColumnVector vector, int vectorRow);

  /**
   * Below method will be used to fill the row values to buffer array
   *
   * @param rowId  row id of the data to be filled
   * @param buffer   buffer in which data will be filled
   * @param offset off the of the buffer
   */
  void fillRow(int rowId, byte[] buffer, int offset);

  /**
   * Below method will be used to get the inverted index
   *
   * @param rowId row id
   * @return inverted index based on row id passed
   */
  int getInvertedIndex(int rowId);

  /**
   * Below method will be used to get the reverse Inverted Index
   * @param rowId
   * @return reverse Inverted Index
   */
  int getInvertedReverseIndex(int rowId);

  /**
   * Below method will be used to get the surrogate key of the
   * based on the row id passed
   *
   * @param rowId row id
   * @return surrogate key
   */
  int getSurrogate(int rowId);

  /**
   * @return size of each column value
   */
  int getColumnValueSize();

  /**
   * @return whether column was explicitly sorted or not
   */
  boolean isExplicitSorted();

  /**
   * Below method will be used to free the memory occupied by
   * the column chunk
   */
  void freeMemory();

  /**
   * to compare the two byte array
   *
   * @param rowId        index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  int compareTo(int rowId, byte[] compareValue);
}
