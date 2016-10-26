/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.carbon.metadata.schema.table.column;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;

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
   * default value for in case of restructuring will be used when older
   * segment does not have particular column
   */
  protected byte[] defaultValue;

  /**
   * Column identifier
   */
  protected ColumnIdentifier columnIdentifier;

  public CarbonColumn(ColumnSchema columnSchema, int ordinal) {
    this.columnSchema = columnSchema;
    this.ordinal = ordinal;
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
    return defaultValue;
  }

  /**
   * @param defaultValue the defaultValue to set
   */
  public void setDefaultValue(byte[] defaultValue) {
    this.defaultValue = defaultValue;
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
    return columnSchema.isComplex();
  }

  /**
   * @return if column is dimension return true, else false.
   */
  public Boolean isDimesion() {
    return columnSchema.isDimensionColumn();
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
}
