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

package org.apache.carbondata.sdk.file;

import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * A field represent one column
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class Field {

  private String name;
  private DataType type;
  private List<StructField> children;
  private String parent;
  private String storeType = "columnnar";
  private int schemaOrdinal = -1;
  private int precision = 0;
  private int scale = 0;
  private String rawSchema = "";
  private String columnComment = "";

  /**
   * Field Constructor
   * @param name name of the field
   * @param type datatype of field, specified in strings.
   */
  public Field(String name, String type) {
    this.name = name;
    if (type.equalsIgnoreCase("string")) {
      this.type = DataTypes.STRING;
    } else if (type.equalsIgnoreCase("varchar")) {
      this.type = DataTypes.VARCHAR;
    } else if (type.equalsIgnoreCase("date")) {
      this.type = DataTypes.DATE;
    } else if (type.equalsIgnoreCase("timestamp")) {
      this.type = DataTypes.TIMESTAMP;
    } else if (type.equalsIgnoreCase("boolean")) {
      this.type = DataTypes.BOOLEAN;
    } else if (type.equalsIgnoreCase("byte")) {
      this.type = DataTypes.BYTE;
    } else if (type.equalsIgnoreCase("short")) {
      this.type = DataTypes.SHORT;
    } else if (type.equalsIgnoreCase("int")) {
      this.type = DataTypes.INT;
    } else if (type.equalsIgnoreCase("long")) {
      this.type = DataTypes.LONG;
    } else if (type.equalsIgnoreCase("float")) {
      this.type = DataTypes.FLOAT;
    } else if (type.equalsIgnoreCase("double")) {
      this.type = DataTypes.DOUBLE;
    } else if (type.equalsIgnoreCase("array")) {
      this.type = DataTypes.createDefaultArrayType();
    } else if (type.equalsIgnoreCase("struct")) {
      this.type = DataTypes.createDefaultStructType();
    }
    else {
      throw new IllegalArgumentException("unsupported data type: " + type);
    }
  }

  public Field(String name, String type, List<StructField> fields) {
    this.name = name;
    this.children = fields;
    if (type.equalsIgnoreCase("string")) {
      this.type = DataTypes.STRING;
    } else if (type.equalsIgnoreCase("varchar")) {
      this.type = DataTypes.VARCHAR;
    } else if (type.equalsIgnoreCase("date")) {
      this.type = DataTypes.DATE;
    } else if (type.equalsIgnoreCase("timestamp")) {
      this.type = DataTypes.TIMESTAMP;
    } else if (type.equalsIgnoreCase("boolean")) {
      this.type = DataTypes.BOOLEAN;
    } else if (type.equalsIgnoreCase("byte")) {
      this.type = DataTypes.BYTE;
    } else if (type.equalsIgnoreCase("short")) {
      this.type = DataTypes.SHORT;
    } else if (type.equalsIgnoreCase("int")) {
      this.type = DataTypes.INT;
    } else if (type.equalsIgnoreCase("long")) {
      this.type = DataTypes.LONG;
    } else if (type.equalsIgnoreCase("float")) {
      this.type = DataTypes.FLOAT;
    } else if (type.equalsIgnoreCase("double")) {
      this.type = DataTypes.DOUBLE;
    } else if (type.equalsIgnoreCase("array")) {
      this.type = DataTypes.createArrayType(fields.get(0).getDataType());
    } else if (type.equalsIgnoreCase("struct")) {
      this.type = DataTypes.createStructType(fields);
    }
    else {
      throw new IllegalArgumentException("unsupported data type: " + type);
    }
  }



  public Field(String name, DataType type, List<StructField> fields) {
    this.name = name;
    this.type = type;
    this.children = fields;
  }

  public Field(String name, DataType type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Construct Field from ColumnSchema
   *
   * @param columnSchema ColumnSchema, Store the information about the column meta data
   */
  public Field(ColumnSchema columnSchema) {
    this.name = columnSchema.getColumnName();
    this.type = columnSchema.getDataType();
    children = new LinkedList<>();
    schemaOrdinal = columnSchema.getSchemaOrdinal();
    precision = columnSchema.getPrecision();
    scale = columnSchema.getScale();
  }

  public String getFieldName() {
    return name;
  }

  public DataType getDataType() {
    return type;
  }

  public List<StructField> getChildren() {
    return children;
  }

  public void setChildren(List<StructField> children) {
    this.children = children;
  }

  public String getParent() {
    return parent;
  }

  public void setParent(String parent) {
    this.parent = parent;
  }

  public String getStoreType() {
    return storeType;
  }

  public int getSchemaOrdinal() {
    return schemaOrdinal;
  }

  public void setSchemaOrdinal(int schemaOrdinal) {
    this.schemaOrdinal = schemaOrdinal;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public String getRawSchema() {
    return rawSchema;
  }

  public void setRawSchema(String rawSchema) {
    this.rawSchema = rawSchema;
  }

  public String getColumnComment() {
    return columnComment;
  }

  public void setColumnComment(String columnComment) {
    this.columnComment = columnComment;
  }
}
