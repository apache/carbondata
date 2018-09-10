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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.WritableUtil;
import org.apache.carbondata.core.preagg.TimeSeriesUDF;

/**
 * Store the information about the column meta data present the table
 */
public class ColumnSchema implements Serializable, Writable {

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
   * List of encoding that are chained to apply the data for this column
   */
  private List<Encoding> encodingList;

  /**
   * Whether the column is a dimension or measure
   */
  private boolean isDimensionColumn;

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
   * aggregate function used in pre aggregate table
   */
  private String aggFunction = "";

  /**
   * list of parent column relations
   */
  private List<ParentColumnTableRelation> parentColumnTableRelations;

  /**
   * timeseries function applied on column
   */
  private String timeSeriesFunction = "";

  /**
   * set whether the column is local dictionary column or not.
   */
  private boolean isLocalDictColumn = false;

  /**
   * @return isLocalDictColumn
   */
  public boolean isLocalDictColumn() {
    return isLocalDictColumn;
  }

  /**
   * @param localDictColumn whether column is local dictionary column
   */
  public void setLocalDictColumn(boolean localDictColumn) {
    isLocalDictColumn = localDictColumn;
  }

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
   * Set the scale if it is decimal type
   */
  public void setScale(int scale) {
    this.scale = scale;
    if (DataTypes.isDecimal(dataType)) {
      ((DecimalType) dataType).setScale(scale);
    }
  }

  /**
   * @return the scale
   */
  public int getScale() {
    return scale;
  }

  /**
   * Set the precision if it is decimal type
   */
  public void setPrecision(int precision) {
    this.precision = precision;
    if (DataTypes.isDecimal(dataType)) {
      ((DecimalType) dataType).setPrecision(precision);
    }
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return precision;
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

  public List<ParentColumnTableRelation> getParentColumnTableRelations() {
    return parentColumnTableRelations;
  }

  public void setParentColumnTableRelations(
      List<ParentColumnTableRelation> parentColumnTableRelations) {
    this.parentColumnTableRelations = parentColumnTableRelations;
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
    } else if (!columnName.equalsIgnoreCase(other.columnName)) {
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
   * method to compare columnSchema,
   * other parameters along with just column name and column data type
   * @param obj
   * @return
   */
  public boolean equalsWithStrictCheck(Object obj) {
    if (!this.equals(obj)) {
      return false;
    }
    ColumnSchema other = (ColumnSchema) obj;
    if (!columnUniqueId.equals(other.columnUniqueId) ||
        (isDimensionColumn != other.isDimensionColumn) ||
        (isSortColumn != other.isSortColumn)) {
      return false;
    }
    if (encodingList.size() != other.encodingList.size()) {
      return false;
    }
    for (int i = 0; i < encodingList.size(); i++) {
      if (encodingList.get(i).compareTo(other.encodingList.get(i)) != 0) {
        return false;
      }
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
   * @param columnProperties
   */
  public void setColumnProperties(Map<String, String> columnProperties) {
    this.columnProperties = columnProperties;
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

  public String getAggFunction() {
    return aggFunction;
  }

  public void setFunction(String function) {
    if (null == function) {
      return;
    }
    if (TimeSeriesUDF.INSTANCE.TIMESERIES_FUNCTION.contains(function.toLowerCase())) {
      this.timeSeriesFunction = function;
    } else {
      this.aggFunction = function;
    }
  }

  public String getTimeSeriesFunction() {
    return timeSeriesFunction;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(dataType.getId());
    out.writeUTF(columnName);
    out.writeUTF(columnUniqueId);
    out.writeUTF(columnReferenceId);
    if (encodingList == null) {
      out.writeShort(0);
    } else {
      out.writeShort(encodingList.size());
      for (Encoding encoding : encodingList) {
        out.writeShort(encoding.ordinal());
      }
    }
    out.writeBoolean(isDimensionColumn);
    if (DataTypes.isDecimal(dataType)) {
      out.writeInt(((DecimalType) dataType).getScale());
      out.writeInt(((DecimalType) dataType).getPrecision());
    } else {
      out.writeInt(-1);
      out.writeInt(-1);
    }
    out.writeInt(schemaOrdinal);
    out.writeInt(numberOfChild);
    WritableUtil.writeByteArray(out, defaultValue);
    if (columnProperties == null) {
      out.writeShort(0);
    } else {
      out.writeShort(columnProperties.size());
      for (Map.Entry<String, String> entry : columnProperties.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeUTF(entry.getValue());
      }
    }
    out.writeBoolean(invisible);
    out.writeBoolean(isSortColumn);
    out.writeUTF(null != aggFunction ? aggFunction : "");
    out.writeUTF(timeSeriesFunction);
    boolean isParentTableColumnRelationExists =
        null != parentColumnTableRelations && parentColumnTableRelations.size() > 0;
    out.writeBoolean(isParentTableColumnRelationExists);
    if (isParentTableColumnRelationExists) {
      out.writeShort(parentColumnTableRelations.size());
      for (int i = 0; i < parentColumnTableRelations.size(); i++) {
        parentColumnTableRelations.get(i).write(out);
      }
    }
    out.writeBoolean(isLocalDictColumn);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int id = in.readShort();
    this.dataType = DataTypes.valueOf(id);
    this.columnName = in.readUTF();
    this.columnUniqueId = in.readUTF();
    this.columnReferenceId = in.readUTF();
    int encodingListSize = in.readShort();
    this.encodingList = new ArrayList<>(encodingListSize);
    for (int i = 0; i < encodingListSize; i++) {
      id = in.readShort();
      encodingList.add(Encoding.valueOf(id));
    }
    this.isDimensionColumn = in.readBoolean();
    this.scale = in.readInt();
    this.precision = in.readInt();
    if (DataTypes.isDecimal(dataType)) {
      DecimalType decimalType = (DecimalType) dataType;
      decimalType.setPrecision(precision);
      decimalType.setScale(scale);
    }
    this.schemaOrdinal = in.readInt();
    this.numberOfChild = in.readInt();
    this.defaultValue = WritableUtil.readByteArray(in);
    int mapSize = in.readShort();
    this.columnProperties = new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      this.columnProperties.put(key, value);
    }
    this.invisible = in.readBoolean();
    this.isSortColumn = in.readBoolean();
    this.aggFunction = in.readUTF();
    this.timeSeriesFunction = in.readUTF();
    boolean isParentTableColumnRelationExists = in.readBoolean();
    if (isParentTableColumnRelationExists) {
      short parentColumnTableRelationSize = in.readShort();
      this.parentColumnTableRelations = new ArrayList<>(parentColumnTableRelationSize);
      for (int i = 0; i < parentColumnTableRelationSize; i++) {
        ParentColumnTableRelation parentColumnTableRelation =
            new ParentColumnTableRelation(null, null, null);
        parentColumnTableRelation.readFields(in);
        parentColumnTableRelations.add(parentColumnTableRelation);
      }
    }
    this.isLocalDictColumn = in.readBoolean();
  }
}
