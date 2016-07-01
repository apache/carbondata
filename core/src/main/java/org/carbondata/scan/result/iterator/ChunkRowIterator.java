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

package org.carbondata.scan.result.iterator;

import org.carbondata.common.CarbonIterator;
import org.carbondata.scan.result.BatchResult;

/**
 * Iterator over row result
 */
public class ChunkRowIterator extends CarbonIterator<Object[]> {

  /**
   * iterator over chunk result
   */
  private CarbonIterator<BatchResult> iterator;

  /**
   * currect chunk
   */
  private BatchResult currentchunk;

  public ChunkRowIterator(CarbonIterator<BatchResult> iterator) {
    this.iterator = iterator;
    if (iterator.hasNext()) {
      currentchunk = iterator.next();
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
    if (null != currentchunk) {
      if ((currentchunk.hasNext())) {
        return true;
      } else if (!currentchunk.hasNext()) {
        while (iterator.hasNext()) {
          currentchunk = iterator.next();
          if (currentchunk != null && currentchunk.hasNext()) {
            return true;
          }
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
    return currentchunk.next();
  }

}
