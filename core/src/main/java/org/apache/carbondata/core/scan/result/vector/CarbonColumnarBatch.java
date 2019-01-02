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

package org.apache.carbondata.core.scan.result.vector;

import java.util.Arrays;

public class CarbonColumnarBatch {

  public CarbonColumnVector[] columnVectors;

  private int batchSize;

  private int actualSize;

  private int rowCounter;

  private boolean[] filteredRows;

  private int rowsFiltered;

  public CarbonColumnarBatch(CarbonColumnVector[] columnVectors, int batchSize,
      boolean[] filteredRows) {
    this.columnVectors = columnVectors;
    this.batchSize = batchSize;
    this.filteredRows = filteredRows;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getActualSize() {
    return actualSize;
  }

  public void setActualSize(int actualSize) {
    this.actualSize = actualSize;
  }

  public void reset() {
    actualSize = 0;
    rowCounter = 0;
    rowsFiltered = 0;
    if (filteredRows != null) {
      Arrays.fill(filteredRows, false);
    }
    for (int i = 0; i < columnVectors.length; i++) {
      columnVectors[i].reset();
    }
  }

  public int getRowCounter() {
    return rowCounter;
  }

  public void setRowCounter(int rowCounter) {
    this.rowCounter = rowCounter;
  }

  /**
   * Mark the rows as filterd first before filling the batch, so that these rows will not be added
   * to vector batches.
   * @param rowId
   */
  public void markFiltered(int rowId) {
    if (!filteredRows[rowId]) {
      filteredRows[rowId] = true;
      rowsFiltered++;
    }
    if (rowsFiltered == 1) {
      for (int i = 0; i < columnVectors.length; i++) {
        columnVectors[i].setFilteredRowsExist(true);
      }
    }
  }
}
