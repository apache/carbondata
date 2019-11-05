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

package org.apache.carbondata.processing.loading.row;

import java.util.NoSuchElementException;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.row.CarbonRow;

import org.apache.commons.lang.ArrayUtils;

/**
 * Batch of rows.
 */
public class CarbonRowBatch extends CarbonIterator<CarbonRow> {

  private CarbonRow[] rowBatch;

  private int size = 0;

  private int index = 0;

  public CarbonRowBatch(int batchSize) {
    this.rowBatch = new CarbonRow[batchSize];
  }

  public void addRow(CarbonRow carbonRow) {
    rowBatch[size++] = carbonRow;
  }

  public int getSize() {
    return size;
  }

  @Override
  public boolean hasNext() {
    return index < size;
  }

  @Override
  public CarbonRow next() throws NoSuchElementException {
    if (hasNext()) {
      return rowBatch[index++];
    }
    throw new NoSuchElementException("no more elements to iterate");
  }

  @Override
  public void remove() {
    rowBatch = (CarbonRow[]) ArrayUtils.remove(rowBatch, index - 1);
    --size;
    --index;
  }

  /**
   * set previous row, this can be used to set value for the RowBatch after iterating it. The
   * `index` here is `index-1` because after we iterate this value, the `index` has increased by 1.
   * @param row row
   */
  public void setPreviousRow(CarbonRow row) {
    if (index == 0) {
      throw new RuntimeException("Unable to set a row in RowBatch before index 0");
    }
    rowBatch[index - 1] = row;
  }

  /**
   * rewind to the head, this can be used for reuse the origin batch instead of generating a new one
   */
  public void rewind() {
    index = 0;
  }
}
