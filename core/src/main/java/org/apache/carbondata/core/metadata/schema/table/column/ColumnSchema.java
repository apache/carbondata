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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;

/**
 * Store the information about the column meta data present the table
 */
public class ColumnSchema implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 7676766554874863763L;

  /**
   * dataType
   */
  private DataType dataType;
  /**
   * Name of the column. If it is a complex data type, we follow a naming rule
   * grand_parent_column.parent_column.child_column
   * For Array types, two columns will be stored one for
   * the array type and one for the primitive type with
   * the name parent_column.value
   */
  private String columnName;

  /**
   * Unique ID for a column. if this is dimension,
   * it is an unique ID that used in dictionary
   */
  private String columnUniqueId;

  /**
   * column reference id
   */
  private String columnReferenceId;

  /**
   * whether it is stored as columnar format or row format
   */
  private boolean isColumnar = true;

  /**
   * List of encoding that are chained to encode the data for this column
   */
  private List<Encoding> encodingList;

  /**
   * Whether the column is a dimension or measure
   */
  private boolean isDimensionColumn;

  /**
   * The group ID for column used for row format columns,
   * where in columns in each group are chunked together.
   */
  private int columnGroupId = -1;

  /**
   * Used when this column contains decimal data.
   */
  private int scale;

  private int precision;

  private int schemaOrdinal;
  /**
   * Nested fields.  Since thrift does not support nested fields,
   * the nesting is flattened to a single list by a depth-first traversal.
   * The children count is used to construct the nested relationship.
   * This field is not set when the element is a primitive type
   */
  private int numberOfChild;

  /**
   * used in case of schema restructuring
   */
  private byte[] defaultValue;

  /**
   * Column properties
   */
  private Map<String, String> columnProperties;

  /**
   * used to define the column visibility of column default is false
   */
  private boolean invisible = false;

  private boolean isSortColumn = false;

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @param columnName the columnName to set
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  /**
   * @return the columnUniqueId
   */
  public String getColumnUniqueId() {
    return columnUniqueId;
  }

  /**
   * @param columnUniqueId the columnUniqueId to set
   */
  public void setColumnUniqueId(String columnUniqueId) {
    this.columnUniqueId = columnUniqueId;
  }

  /**
   * @return the isColumnar
   */
  public boolean isColumnar() {
    return isColumnar;
  }

  /**
   * @param isColumnar the isColumnar to set
   */
  public void setColumnar(boolean isColumnar) {
    this.isColumnar = isColumnar;
  }

  /**
   * @return the isDimensionColumn
   */
  public boolean isDimensionColumn() {
    return isDimensionColumn;
  }

  /**
   * @param isDimensionColumn the isDimensionColumn to set
   */
  public void setDimensionColumn(boolean isDimensionColumn) {
    this.isDimensionColumn = isDimensionColumn;
  }

  /**
   * the isUseInvertedIndex
   */
  public boolean isUseInvertedIndex() {
    return this.hasEncoding(Encoding.INVERTED_INDEX);
  }

  /**
   * @param useInvertedIndex the useInvertedIndex to set
   */
  public void setUseInvertedIndex(boolean useInvertedIndex) {
    if (useInvertedIndex) {
      if (!hasEncoding(Encoding.INVERTED_INDEX)) {
        this.getEncodingList().add(Encoding.INVERTED_INDEX);
      }
    } else {
      if (hasEncoding(Encoding.INVERTED_INDEX)) {
        this.getEncodingList().remove(Encoding.INVERTED_INDEX);
      }
    }
  }

  /**
   * @return the columnGroup
   */
  public int getColumnGroupId() {
    return columnGroupId;
  }

  /**
   */
  public void setColumnGroup(int columnGroupId) {
    this.columnGroupId = columnGroupId;
  }

  /**
   * @return the scale
   */
  public int getScale() {
    return scale;
  }

  /**
   * @param scale the scale to set
   */
  public void setScale(int scale) {
    this.scale = scale;
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision the precision to set
   */
  public void setPrecision(int precision) {
    this.precision = precision;
  }

  /**
   * @return the getNumberOfChild
   */
  public int getNumberOfChild() {
    return numberOfChild;
  }

  /**
   * @param numberOfChild the getNumberOfChild to set
   */
  public void setNumberOfChild(int numberOfChild) {
    this.numberOfChild = numberOfChild;
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
   * hash code method to check get the hashcode based.
   * for generating the hash code only column name and column unique id will considered
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode()) +
      ((dataType == null) ? 0 : dataType.hashCode());
    return result;
  }

  /**
   * Overridden equals method for columnSchema
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ColumnSchema)) {
      return false;
    }
    ColumnSchema other = (ColumnSchema) obj;
    if (columnName == null) {
      if (other.columnName != null) {
        return false;
      }
    } else if (!columnName.equals(other.columnName)) {
      return false;
    }
    if (dataType == null) {
      if (other.dataType != null) {
        return false;
      }
    } else if (!dataType.equals(other.dataType)) {
      return false;
    }
    return true;
  }

  /**
   * @return the dataType
   */
  public DataType getDataType() {
    return dataType;
  }

  /**
   * @param dataType the dataType to set
   */
  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  /**
   * @return the encoderList
   */
  public List<Encoding> getEncodingList() {
    return encodingList;
  }

  /**
   */
  public void setEncodingList(List<Encoding> encodingList) {
    this.encodingList = encodingList;
  }

  /**
   * @param encoding
   * @return true if contains the passing encoding
   */
  public boolean hasEncoding(Encoding encoding) {
    if (encodingList == null || encodingList.isEmpty()) {
      return false;
    } else {
      return encodingList.contains(encoding);
    }
  }

  /**
   * @return if DataType is ARRAY or STRUCT, this method return true, else
   * false.
   */
  public Boolean isComplex() {
    if (DataType.ARRAY.equals(this.getDataType()) || DataType.STRUCT.equals(this.getDataType())) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * @param columnProperties
   */
  public void setColumnProperties(Map<String, String> columnProperties) {
    this.columnProperties = columnProperties;
  }

  /**
   * @param property
   * @return
   */
  public String getColumnProperty(String property) {
    if (null != columnProperties) {
      return columnProperties.get(property);
    }
    return null;
  }

  /**
   * return columnproperties
   */
  public Map<String, String> getColumnProperties() {
    return columnProperties;
  }
  /**
   * return the visibility
   * @return
   */
  public boolean isInvisible() {
    return invisible;
  }

  /**
   * set the visibility
   * @param invisible
   */
  public void setInvisible(boolean invisible) {
    this.invisible = invisible;
  }

  /**
   * @return columnReferenceId
   */
  public String getColumnReferenceId() {
    return columnReferenceId;
  }

  /**
   * @param columnReferenceId
   */
  public void setColumnReferenceId(String columnReferenceId) {
    this.columnReferenceId = columnReferenceId;
  }

  public int getSchemaOrdinal() {
    return schemaOrdinal;
  }

  public void setSchemaOrdinal(int schemaOrdinal) {
    this.schemaOrdinal = schemaOrdinal;
  }

  public boolean isSortColumn() {
    return isSortColumn;
  }

  public void setSortColumn(boolean sortColumn) {
    isSortColumn = sortColumn;
  }
}
