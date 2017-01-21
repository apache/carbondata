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

package org.apache.carbondata.core.scan.result.iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;

/**
 * Iterator over row result
 */
public class VectorChunkRowIterator extends CarbonIterator<Object[]> {

  /**
   * iterator over chunk result
   */
  private AbstractDetailQueryResultIterator iterator;

  /**
   * currect chunk
   */
  private CarbonColumnarBatch columnarBatch;

  private int batchSize;

  private int currentIndex;

  public VectorChunkRowIterator(AbstractDetailQueryResultIterator iterator,
      CarbonColumnarBatch columnarBatch) {
    this.iterator = iterator;
    this.columnarBatch = columnarBatch;
    if (iterator.hasNext()) {
      iterator.processNextBatch(columnarBatch);
      batchSize = columnarBatch.getActualSize();
      currentIndex = 0;
    }
  }

  /**
   * Returns {@code true} if the iteration has more elements. (In other words,
   * returns {@code true} if {@link #next} would return an element rather than
   * throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    if (currentIndex < batchSize) {
      return true;
    } else {
      while (iterator.hasNext()) {
        columnarBatch.reset();
        iterator.processNextBatch(columnarBatch);
        batchSize = columnarBatch.getActualSize();
        currentIndex = 0;
        if (currentIndex < batchSize) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public Object[] next() {
    Object[] row = new Object[columnarBatch.columnVectors.length];
    for (int i = 0; i < row.length; i++) {
      row[i] = columnarBatch.columnVectors[i].getData(currentIndex);
    }
    currentIndex++;
    return row;
  }

}
