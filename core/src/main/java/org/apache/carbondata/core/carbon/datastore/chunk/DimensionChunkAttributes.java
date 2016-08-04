/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.datastore.chunk;

/**
 * Dimension chunk attributes which holds all the
 * property about the dimension chunk data
 */
public class DimensionChunkAttributes {

  /**
   * inverted index of the data
   */
  private int[] invertedIndexes;

  /**
   * reverse index of the data
   */
  private int[] invertedIndexesReverse;

  /**
   * each row size
   */
  private int columnValueSize;

  /**
   * is no dictionary
   */
  private boolean isNoDictionary;

  /**
   * @return the invertedIndexes
   */
  public int[] getInvertedIndexes() {
    return invertedIndexes;
  }

  /**
   * @param invertedIndexes the invertedIndexes to set
   */
  public void setInvertedIndexes(int[] invertedIndexes) {
    this.invertedIndexes = invertedIndexes;
  }

  /**
   * @return the invertedIndexesReverse
   */
  public int[] getInvertedIndexesReverse() {
    return invertedIndexesReverse;
  }

  /**
   * @param invertedIndexesReverse the invertedIndexesReverse to set
   */
  public void setInvertedIndexesReverse(int[] invertedIndexesReverse) {
    this.invertedIndexesReverse = invertedIndexesReverse;
  }

  /**
   * @return the eachRowSize
   */
  public int getColumnValueSize() {
    return columnValueSize;
  }

  /**
   * @param eachRowSize the eachRowSize to set
   */
  public void setEachRowSize(int eachRowSize) {
    this.columnValueSize = eachRowSize;
  }

  /**
   * @return the isNoDictionary
   */
  public boolean isNoDictionary() {
    return isNoDictionary;
  }

  /**
   * @param isNoDictionary the isNoDictionary to set
   */
  public void setNoDictionary(boolean isNoDictionary) {
    this.isNoDictionary = isNoDictionary;
  }
}
