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

import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;

public class ByteArrayColumnWithRowId implements Comparable<ByteArrayColumnWithRowId> {
  private byte[] column;

  private short rowId;

  private boolean isNoDictionary;

  ByteArrayColumnWithRowId(byte[] column, short rowId, boolean isNoDictionary) {
    this.column = column;
    this.rowId = rowId;
    this.isNoDictionary = isNoDictionary;
  }

  public byte[] getColumn() {
    return column;
  }

  public short getRowId() {
    return rowId;
  }

  @Override
  public int compareTo(ByteArrayColumnWithRowId o) {
    if (isNoDictionary) {
      return UnsafeComparer.INSTANCE
          .compareTo(column, 2, column.length - 2, o.column, 2, o.column.length - 2);
    } else {
      return UnsafeComparer.INSTANCE
          .compareTo(column, 0, column.length, o.column, 0, o.column.length);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ByteArrayColumnWithRowId o = (ByteArrayColumnWithRowId)obj;
    return Arrays.equals(column, o.column) && rowId == o.rowId;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(column) + Short.hashCode(rowId);
  }
}
