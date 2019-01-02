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

package org.apache.carbondata.core.cache.dictionary;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * dictionary column identifier which includes table identifier and column identifier
 */
public class DictionaryColumnUniqueIdentifier implements Serializable {

  private static final long serialVersionUID = -1231234567L;

  /**
   * table fully qualified name
   */
  private AbsoluteTableIdentifier dictionarySourceAbsoluteTableIdentifier;

  /**
   * unique column id
   */
  private ColumnIdentifier columnIdentifier;

  private DataType dataType;

  private String dictionaryLocation;

  /**
   * Will be used in case of reverse dictionary cache which will be used
   * in case of data loading.
   *
   * @param dictionarySourceAbsoluteTableIdentifier
   * @param columnIdentifier
   */
  public DictionaryColumnUniqueIdentifier(
      AbsoluteTableIdentifier dictionarySourceAbsoluteTableIdentifier,
      ColumnIdentifier columnIdentifier) {
    if (dictionarySourceAbsoluteTableIdentifier == null) {
      throw new IllegalArgumentException("carbonTableIdentifier is null");
    }
    if (columnIdentifier == null) {
      throw new IllegalArgumentException("columnIdentifier is null");
    }
    this.dictionarySourceAbsoluteTableIdentifier = dictionarySourceAbsoluteTableIdentifier;
    this.columnIdentifier = columnIdentifier;
    this.dataType = columnIdentifier.getDataType();
    this.dictionaryLocation =
        CarbonTablePath.getMetadataPath(dictionarySourceAbsoluteTableIdentifier.getTablePath());
  }

  /**
   * Will be used in case of forward dictionary cache in case
   * of query execution.
   *
   * @param dictionarySourceAbsoluteTableIdentifier
   * @param columnIdentifier
   * @param dataType
   */
  public DictionaryColumnUniqueIdentifier(
      AbsoluteTableIdentifier dictionarySourceAbsoluteTableIdentifier,
      ColumnIdentifier columnIdentifier, DataType dataType) {
    this(dictionarySourceAbsoluteTableIdentifier, columnIdentifier);
    this.dataType = dataType;
  }

  public DictionaryColumnUniqueIdentifier(
      AbsoluteTableIdentifier dictionarySourceAbsoluteTableIdentifier,
      ColumnIdentifier columnIdentifier, DataType dataType, String dictionaryLocation) {
    this(dictionarySourceAbsoluteTableIdentifier, columnIdentifier, dataType);
    if (null != dictionaryLocation) {
      this.dictionaryLocation = dictionaryLocation;
    }
  }

  public DataType getDataType() {
    return dataType;
  }

  /**
   * @return columnIdentifier
   */
  public ColumnIdentifier getColumnIdentifier() {
    return columnIdentifier;
  }

  /**
   * @return dictionary file path
   */
  public String getDictionaryFilePath() {
    return CarbonTablePath.getExternalDictionaryFilePath(
        dictionaryLocation, columnIdentifier.getColumnId());
  }

  /**
   * @return dictionary metadata file path
   */
  public String getDictionaryMetaFilePath() {
    return CarbonTablePath.getExternalDictionaryMetaFilePath(
        dictionaryLocation, columnIdentifier.getColumnId());
  }

  /**
   * @return sort index file path
   */
  public String getSortIndexFilePath() {
    return CarbonTablePath.getExternalSortIndexFilePath(
        dictionaryLocation, columnIdentifier.getColumnId());
  }

  /**
   * @param offset
   * @return sort index file path with given offset
   */
  public String getSortIndexFilePath(long offset) {
    return CarbonTablePath.getExternalSortIndexFilePath(
        dictionaryLocation, columnIdentifier.getColumnId(), offset);
  }

  /**
   * overridden equals method
   *
   * @param other
   * @return
   */
  @Override public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    DictionaryColumnUniqueIdentifier that = (DictionaryColumnUniqueIdentifier) other;
    if (!dictionarySourceAbsoluteTableIdentifier
        .equals(that.dictionarySourceAbsoluteTableIdentifier)) {
      return false;
    }
    return columnIdentifier.equals(that.columnIdentifier);
  }

  /**
   * overridden hashcode method
   *
   * @return
   */
  @Override public int hashCode() {
    int result = dictionarySourceAbsoluteTableIdentifier.hashCode();
    result = 31 * result + columnIdentifier.hashCode();
    return result;
  }
}
