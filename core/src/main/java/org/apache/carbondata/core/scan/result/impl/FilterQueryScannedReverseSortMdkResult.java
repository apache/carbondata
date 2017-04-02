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
package org.apache.carbondata.core.scan.result.impl;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;

/**
 * Result provider class in case of filter query In case of filter query data
 * will be send based on mdk reverse order
 */
public class FilterQueryScannedReverseSortMdkResult extends FilterQueryScannedResult {

  public FilterQueryScannedReverseSortMdkResult(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key array for all the dictionary dimension selected in
   *         query
   */
  @Override
  public byte[] getDictionaryKeyArray() {
    --currentRow;
    return getDictionaryKeyArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   *         selected in query
   */
  @Override
  public int[] getDictionaryKeyIntegerArray() {
    --currentRow;
    return getDictionaryKeyIntegerArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * to check whether any more row is present in the result
   *
   * @return
   */
  @Override
  public boolean hasNext() {
    if (pageCounter >= 0 && rowCounter < this.numberOfRows[pageCounter]) {
      return true;
    } else if (pageCounter > 0) {
      incrementPageCounter();
      return hasNext();
    }
    return false;
  }

  @Override
  public void setIndexes(int[][] indexes) {
    super.setIndexes(indexes);
    if (indexes != null && indexes.length > 0) {
      pageCounter = indexes.length - 1;

      if (indexes[pageCounter] != null) {
        currentRow = indexes[pageCounter].length;
      }

    } else {
      pageCounter = -1;
    }
  }

  /**
   * Just increment the counter incase of query only on measures.
   */
  @Override
  public void incrementCounter() {
    rowCounter++;
    currentRow--;
  }

  /**
   * Just increment the page counter and reset the remaining counters.
   */
  @Override
  public void incrementPageCounter() {
    pageCounter--;
    rowCounter = 0;
    currentRow = this.numberOfRows[pageCounter];
  }
}
