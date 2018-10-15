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

import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * primitive column data holder
 */
public class PrimitiveColumnDataVO implements ColumnDataVo<Object> {

  /**
   * data
   */
  private Object column;

  /**
   * index in data
   */
  private Short index;

  /**
   * data type based comparator
   */
  private SerializableComparator serializableComparator;

  PrimitiveColumnDataVO(Object column, short index, SerializableComparator serializableComparator) {
    this.column = column;
    this.index = index;
    this.serializableComparator = serializableComparator;
  }

  @Override public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PrimitiveColumnDataVO o = (PrimitiveColumnDataVO) obj;
    return column.equals(o.column) && index.equals(o.index);
  }

  @Override public int hashCode() {
    return this.column.hashCode() + index.hashCode();
  }

  /**
   * @return the index
   */
  public short getIndex() {
    return index;
  }

  @Override public int getLength() {
    return 0;
  }

  /**
   * @return the column
   */
  public Object getData() {
    return column;
  }

  @Override public int compareTo(Object other) {
    return serializableComparator.compare(column, ((PrimitiveColumnDataVO)other).column);
  }
}
