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

public class ObjectColumnWithRowId implements Comparable<ObjectColumnWithRowId> {
  private Object column;

  private short rowId;

  private DataType dataType;

  ObjectColumnWithRowId(Object column, short rowId, DataType dataType) {
    this.column = column;
    this.rowId = rowId;
    this.dataType = dataType;
  }

  public Object getColumn() {
    return column;
  }

  public short getRowId() {
    return rowId;
  }

  @Override
  public int compareTo(ObjectColumnWithRowId o) {
    // use the data type based comparator for the no dictionary encoded columns
    SerializableComparator comparator =
        org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataType);
    return comparator.compare(column, o.column);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ObjectColumnWithRowId o = (ObjectColumnWithRowId)obj;
    return column.equals(o.column) && rowId == o.rowId;
  }

  @Override
  public int hashCode() {
    return column.hashCode() + Short.hashCode(rowId);
  }
}
