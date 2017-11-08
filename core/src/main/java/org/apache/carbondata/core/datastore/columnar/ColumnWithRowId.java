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

package org.apache.carbondata.core.datastore.columnar;

import java.util.Arrays;

import org.apache.carbondata.core.util.ByteUtil;

public class ColumnWithRowId<T> implements Comparable<ColumnWithRowId<T>> {
  protected byte[] column;

  private T index;

  ColumnWithRowId(byte[] column, T index) {
    this.column = column;
    this.index = index;
  }

  /**
   * @return the column
   */
  public byte[] getColumn() {
    return column;
  }

  /**
   * @return the index
   */
  public T getIndex() {
    return index;
  }

  @Override public int compareTo(ColumnWithRowId o) {
    return ByteUtil.UnsafeComparer.INSTANCE.compareTo(column, o.column);
  }

  @Override public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ColumnWithRowId o = (ColumnWithRowId)obj;
    return Arrays.equals(column, o.column) && index == o.index;
  }

  @Override public int hashCode() {
    return Arrays.hashCode(column) + index.hashCode();
  }
}
