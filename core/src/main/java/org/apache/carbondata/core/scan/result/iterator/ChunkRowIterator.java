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

import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.result.RowBatch;

/**
 * Iterator over row result
 */
public class ChunkRowIterator extends CarbonIterator<Object[]> {

  /**
   * iterator over chunk result
   */
  private CarbonIterator<RowBatch> iterator;

  /**
   * current chunk
   */
  private RowBatch currentChunk;

  public ChunkRowIterator(CarbonIterator<RowBatch> iterator) {
    this.iterator = iterator;
  }

  /**
   * Returns {@code true} if the iteration has more elements. (In other words,
   * returns {@code true} if {@link #next} would return an element rather than
   * throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    if (currentChunk != null && currentChunk.hasNext()) {
      return true;
    } else if (iterator != null && iterator.hasNext()) {
      currentChunk = iterator.next();
      return hasNext();
    }
    return false;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public Object[] next() {
    return currentChunk.next();
  }

  /**
   * read next batch
   *
   * @return list of batch result
   */
  public List<Object[]> nextBatch() {
    return currentChunk.nextBatch();
  }

}
