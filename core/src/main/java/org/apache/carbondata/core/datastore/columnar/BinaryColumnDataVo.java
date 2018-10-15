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

/**
 * Hold the data for binary column type like string
 */
public class BinaryColumnDataVo implements ColumnDataVo<byte[]> {

  /**
   * length of the data
   */
  private final int length;

  /**
   * data
   */
  protected byte[] column;

  /**
   * actual index
   */
  private Short index;

  BinaryColumnDataVo(byte[] column, short index, int length) {
    this.column = column;
    this.index = index;
    this.length = length;
  }

  /**
   * @return the column
   */
  public byte[] getData() {
    return column;
  }

  /**
   * @return the index
   */
  public short getIndex() {
    return index;
  }

  public int getLength() {
    return length;
  }

  @Override public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    BinaryColumnDataVo o = (BinaryColumnDataVo)obj;
    return Arrays.equals(column, o.column) && index.equals(o.index);
  }

  @Override public int hashCode() {
    return Arrays.hashCode(column) + index.hashCode();
  }

  @Override public int compareTo(Object o) {
    return ByteUtil.UnsafeComparer.INSTANCE.compareTo(column, ((BinaryColumnDataVo) o).column);
  }
}
