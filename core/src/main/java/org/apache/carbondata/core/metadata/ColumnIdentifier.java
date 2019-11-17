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

package org.apache.carbondata.core.metadata;

import java.io.Serializable;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Column unique identifier
 */
public class ColumnIdentifier implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * column id
   */
  private String columnId;

  /**
   * column properties
   */
  private Map<String, String> columnProperties;

  private DataType dataType;

  /**
   * @param columnId
   * @param columnProperties
   */
  public ColumnIdentifier(String columnId, Map<String, String> columnProperties,
      DataType dataType) {
    this.columnId = columnId;
    this.columnProperties = columnProperties;
    this.dataType = dataType;
  }

  /**
   * @return columnId
   */
  public String getColumnId() {
    return columnId;
  }

  /**
   * @param columnProperty
   * @return
   */
  public String getColumnProperty(String columnProperty) {
    String property = null;
    if (null != columnProperties) {
      property = columnProperties.get(columnProperty);
    }
    return property;
  }

  public DataType getDataType() {
    return this.dataType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnId == null) ? 0 : columnId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ColumnIdentifier other = (ColumnIdentifier) obj;
    if (columnId == null) {
      if (other.columnId != null) {
        return false;
      }
    } else if (!columnId.equals(other.columnId)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "ColumnIdentifier [columnId=" + columnId + "]";
  }

}
