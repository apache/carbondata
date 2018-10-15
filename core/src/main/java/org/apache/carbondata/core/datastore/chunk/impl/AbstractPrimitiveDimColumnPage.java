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

import java.util.BitSet;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;

/**
 * Abstract Class for handling Primitive data type Dimension column
 */
public abstract class AbstractPrimitiveDimColumnPage implements DimensionColumnPage {

  /**
   * column page which holds the actual data
   */
  protected ColumnPage columnPage;

  /**
   * inverted index store the actual position of the data
   */
  protected int[] invertedIndex;

  /**
   * reverse index store the position after sorting the data
   */
  protected int[] invertedIndexReverse;

  /**
   * boolean to check if data was exolictly sorted or not
   */
  protected boolean isExplictSorted;

  /**
   * null bitset represents cell with null value
   */
  protected BitSet nullBitset;

  protected boolean isAllNullValues;

  protected AbstractPrimitiveDimColumnPage(ColumnPage columnPage, int[] invertedIndex,
      int[] invertedIndexReverse, int numberOfRows) {
    this.columnPage = columnPage;
    this.isExplictSorted = null != invertedIndex && invertedIndex.length > 0;
    this.invertedIndex = invertedIndex;
    this.invertedIndexReverse = invertedIndexReverse;
    this.nullBitset = columnPage.getNullBits();
    isAllNullValues = nullBitset.cardinality() == numberOfRows;
  }

  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    return 0;
  }

  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    return 0;
  }

  @Override public boolean isAdaptiveEncoded() {
    return true;
  }

  @Override public void freeMemory() {
    if (null != columnPage) {
      columnPage.freeMemory();
      this.invertedIndexReverse = null;
      this.invertedIndex = null;
      columnPage = null;
    }
  }

  @Override public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override public boolean isExplicitSorted() {
    return isExplictSorted;
  }

  @Override public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return invertedIndexReverse[rowId];
  }

  /**
   * to get the null bit sets in case of adaptive encoded page
   */
  @Override public BitSet getNullBits() {
    return nullBitset;
  }
}