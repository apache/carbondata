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

package org.apache.carbondata.core.metadata.schema.table.column;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;

public class CarbonColumn implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 3648269871256322681L;

  /**
   * column schema
   */
  protected ColumnSchema columnSchema;

  /**
   * table ordinal
   */
  protected int ordinal;

  /**
   * order in which user has created table
   */
  private int schemaOrdinal;

  /**
   * Column identifier
   */
  protected ColumnIdentifier columnIdentifier;

  public CarbonColumn(ColumnSchema columnSchema, int ordinal, int schemaOrdinal) {
    this.columnSchema = columnSchema;
    this.ordinal = ordinal;
    this.schemaOrdinal = schemaOrdinal;
    this.columnIdentifier =
     new ColumnIdentifier(getColumnId(), getColumnProperties(), getDataType());
  }
  /**
   * @return columnar or row based
   */
  public boolean isColumnar() {
    return columnSchema.isColumnar();
  }

  /**
   * @return column unique id
   */
  public String getColumnId() {
    return columnSchema.getColumnUniqueId();
  }

  /**
   * @return the dataType
   */
  public DataType getDataType() {
    return columnSchema.getDataType();
  }

  /**
   * @return the colName
   */
  public String getColName() {
    return columnSchema.getColumnName();
  }

  /**
   * @return the ordinal
   */
  public int getOrdinal() {
    return ordinal;
  }

  /**
   * @return the list of encoder used in dimension
   */
  public List<Encoding> getEncoder() {
    return columnSchema.getEncodingList();
  }

  /**
   * @return row group id if it is row based
   */
  public int columnGroupId() {
    return columnSchema.getColumnGroupId();
  }

  /**
   * @return the defaultValue
   */
  public byte[] getDefaultValue() {
    return columnSchema.getDefaultValue();
  }

  /**
   * @param encoding
   * @return true if contains the passing encoding
   */
  public boolean hasEncoding(Encoding encoding) {
    return columnSchema.hasEncoding(encoding);
  }

  /**
   * @return if DataType is ARRAY or STRUCT, this method return true, else
   * false.
   */
  public Boolean isComplex() {
    return columnSchema.getDataType().isComplexType();
  }

  /**
   * @return if column is dimension return true, else false.
   */
  public Boolean isDimension() {
    return columnSchema.isDimensionColumn();
  }

  /**
   * @return true if column is measure, otherwise false
   */
  public Boolean isMeasure() {
    return !isDimension();
  }

  /**
   * return the visibility
   * @return
   */
  public boolean isInvisible() {
    return columnSchema.isInvisible();
  }

  /**
   * @return if column use inverted index return true, else false.
   */
  public Boolean isUseInvertedIndex() {
    return columnSchema.isUseInvertedIndex();
  }
  public ColumnSchema getColumnSchema() {
    return this.columnSchema;
  }

  /**
   * @return columnproperty
   */
  public Map<String, String> getColumnProperties() {
    return this.columnSchema.getColumnProperties();
  }

  /**
   * @return columnIdentifier
   */
  public ColumnIdentifier getColumnIdentifier() {
    return this.columnIdentifier;
  }

  public int getSchemaOrdinal() {
    return this.schemaOrdinal;
  }
}
