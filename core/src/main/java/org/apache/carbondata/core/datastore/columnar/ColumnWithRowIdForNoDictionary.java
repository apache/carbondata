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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class ColumnWithRowIdForNoDictionary<T>
    implements Comparable<ColumnWithRowIdForNoDictionary<T>> {

  Object column;

  T index;

  DataType dataType;

  ColumnWithRowIdForNoDictionary(Object column, T index, DataType dataType) {
    this.column = column;
    this.index = index;
    this.dataType = dataType;
  }

  @Override public int compareTo(ColumnWithRowIdForNoDictionary o) {
    // use the data type based comparator for the no dictionary encoded columns
    SerializableComparator comparator =
        org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataType);
    return comparator.compare(column, o.column);
  }

  @Override public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ColumnWithRowIdForNoDictionary o = (ColumnWithRowIdForNoDictionary)obj;
    return column.equals(o.column) && getIndex() == o.getIndex();
  }

  @Override public int hashCode() {
    return getColumn().hashCode() + getIndex().hashCode();
  }

  /**
   * @return the index
   */
  public T getIndex() {
    return index;
  }


  /**
   * @return the column
   */
  public Object getColumn() {
    return column;
  }

}
