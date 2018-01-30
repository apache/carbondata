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

package org.apache.carbondata.core.scan.result;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.carbondata.common.CarbonIterator;

/**
 * Below class holds the query result
 */
public class RowBatch extends CarbonIterator<Object[]> {

  /**
   * list of keys
   */
  protected List<Object[]> rows;

  /**
   * counter to check whether all the records are processed or not
   */
  protected int counter;

  public RowBatch() {
    this.rows = new ArrayList<>();
  }

  /**
   * Below method will be used to get the rows
   *
   * @return
   */
  public List<Object[]> getRows() {
    return rows;
  }

  /**
   * Below method will be used to get the set the values
   *
   * @param rows
   */
  public void setRows(List<Object[]> rows) {
    this.rows = rows;
  }

  /**
   * This method will return one row at a time based on the counter given.
   * @param counter
   * @return
   */
  public Object[] getRawRow(int counter) {
    return rows.get(counter);
  }

  /**
   * For getting the total size.
   * @return
   */
  public int getSize() {
    return rows.size();
  }


  /**
   * Returns {@code true} if the iteration has more elements.
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return counter < rows.size();
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
    Object[] row = rows.get(counter);
    counter++;
    return row;
  }
}
