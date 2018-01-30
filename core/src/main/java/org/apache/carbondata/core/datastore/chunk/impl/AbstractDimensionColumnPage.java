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

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;

/**
 * Class responsibility is to give access to dimension column data chunk store
 */
public abstract class AbstractDimensionColumnPage implements DimensionColumnPage {

  /**
   * data chunks
   */
  DimensionDataChunkStore dataChunkStore;

  /**
   * @return whether columns where explicitly sorted or not
   */
  @Override public boolean isExplicitSorted() {
    return dataChunkStore.isExplicitSorted();
  }

  /**
   * Below method to get the data based in row id
   *
   * @param rowId row id of the data
   * @return chunk
   */
  @Override public byte[] getChunkData(int rowId) {
    return dataChunkStore.getRow(rowId);
  }

  /**
   * @return inverted index
   */
  @Override public int getInvertedIndex(int rowId) {
    return dataChunkStore.getInvertedIndex(rowId);
  }

  /**
   * @param rowId
   * @return inverted index reverse
   */
  @Override public int getInvertedReverseIndex(int rowId) {
    return dataChunkStore.getInvertedReverseIndex(rowId);
  }

  /**
   * To compare the data
   *
   * @param rowId        row index to be compared
   * @param compareValue value to compare
   * @return compare result
   */
  @Override public int compareTo(int rowId, byte[] compareValue) {
    // TODO Auto-generated method stub
    return dataChunkStore.compareTo(rowId, compareValue);
  }

  /**
   * below method will be used to free the allocated memory
   */
  @Override public void freeMemory() {
    dataChunkStore.freeMemory();
  }

  /**
   * @return column is dictionary column or not
   */
  @Override public boolean isNoDicitionaryColumn() {
    return false;
  }
}
