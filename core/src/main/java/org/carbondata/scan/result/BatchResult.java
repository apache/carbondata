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

package org.carbondata.scan.result;

import java.util.NoSuchElementException;

import org.carbondata.common.CarbonIterator;

/**
 * Below class holds the query result
 */
public class BatchResult extends CarbonIterator<Object[]> {

  /**
   * list of keys
   */
  protected Object[][] rows;

  /**
   * counter to check whether all the records are processed or not
   */
  protected int counter;

  public BatchResult() {
    this.rows = new Object[0][];
  }

  /**
   * Below method will be used to get the rows
   *
   * @return
   */
  public Object[][] getRows() {
    return rows;
  }

  /**
   * Below method will be used to get the set the values
   *
   * @param rows
   */
  public void setRows(Object[][] rows) {
    this.rows = rows;
  }


  /**
   * Returns {@code true} if the iteration has more elements.
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return counter < rows.length;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public Object[] next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Object[] row = rows[counter];
    counter++;
    return row;
  }
}
