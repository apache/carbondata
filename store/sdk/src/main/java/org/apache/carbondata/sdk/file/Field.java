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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.MapType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
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
  private String storeType = "columnar";
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
    this.name = name.toLowerCase().trim();
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
    } else if (type.equalsIgnoreCase("binary")) {
      this.type = DataTypes.BINARY;
    } else if (type.toLowerCase().startsWith("decimal")) {
      if ("decimal".equalsIgnoreCase(type.toLowerCase())) {
        this.type = DataTypes.createDefaultDecimalType();
      } else {
        try {
          Matcher m = Pattern.compile("^decimal\\(([^)]+)\\)").matcher(type.toLowerCase());
          m.find();
          String matchedString = m.group(1);
          String[] scaleAndPrecision = matchedString.split(",");
          precision = Integer.parseInt(scaleAndPrecision[0].trim());
          scale = Integer.parseInt(scaleAndPrecision[1].trim());
          this.type = DataTypes.createDecimalType(precision, scale);
        } catch (Exception e) {
          throw new IllegalArgumentException("unsupported data type: " + type
              + ". Please use decimal or decimal(precision,scale), " +
              "precision can be 10 and scale can be 2", e);
        }
      }
    } else if (type.equalsIgnoreCase("array")) {
      this.type = DataTypes.createDefaultArrayType();
    } else if (type.equalsIgnoreCase("struct")) {
      this.type = DataTypes.createDefaultStructType();
    } else if (type.equalsIgnoreCase("map")) {
      this.type = DataTypes.createDefaultMapType();
    } else {
      throw new IllegalArgumentException("unsupported data type: " + type);
    }
  }

  public Field(String name, String type, List<StructField> fields) {
    this.name = name.toLowerCase().trim();
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
    } else if (type.equalsIgnoreCase("binary")) {
      this.type = DataTypes.BINARY;
    } else if (type.equalsIgnoreCase("array")) {
      this.type = DataTypes.createArrayType(fields.get(0).getDataType());
    } else if (type.equalsIgnoreCase("struct")) {
      this.type = DataTypes.createStructType(fields);
    } else {
      throw new IllegalArgumentException("unsupported data type: " + type);
    }
  }

  public Field(String name, DataType type, List<StructField> fields) {
    this.name = name.toLowerCase().trim();
    this.type = type;
    this.children = fields;
  }

  public Field(String name, DataType type) {
    this.name = name.toLowerCase().trim();
    this.type = type;
    initComplexTypeChildren();
  }

  /**
   * Construct Field from ColumnSchema
   *
   * @param columnSchema ColumnSchema, Store the information about the column meta data
   */
  public Field(ColumnSchema columnSchema) {
    this.name = columnSchema.getColumnName().toLowerCase().trim();
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

  /* for SDK, change string type to varchar by default for parent columns */
  public void updateDataTypeToVarchar() {
    this.type = DataTypes.VARCHAR;
  }

  private void initComplexTypeChildren() {
    if (getDataType().isComplexType()) {
      StructField subFields = prepareSubFields(getFieldName(), getDataType());
      if (DataTypes.isArrayType(getDataType()) || DataTypes.isMapType(getDataType())) {
        children = subFields.getChildren();
      } else if (DataTypes.isStructType(getDataType())) {
        children = ((StructType) subFields.getDataType()).getFields();
      }
    }
  }

  @Override
  public String toString() {
    return "Field{" +
        "name='" + name + '\'' +
        ", type=" + type +
        ", schemaOrdinal=" + schemaOrdinal +
        '}';
  }

  /**
   * prepare sub fields for complex types
   *
   * @param fieldName column name
   * @param dataType data type of column or it's children
   * @return
   */
  private StructField prepareSubFields(String fieldName, DataType dataType) {
    if (DataTypes.isArrayType(dataType)) {
      List<StructField> arrayFields = new ArrayList<>();
      StructField arrayField = prepareSubFields(fieldName, ((ArrayType) dataType).getElementType());
      arrayFields.add(arrayField);
      return new StructField(fieldName, DataTypes.createArrayType(arrayField.getDataType()),
          arrayFields);
    } else if (DataTypes.isStructType(dataType)) {
      List<StructField> structFields = new ArrayList<>();
      List<StructField> fields = ((StructType) dataType).getFields();
      for (StructField field : fields) {
        structFields.add(prepareSubFields(field.getFieldName(), field.getDataType()));
      }
      return new StructField(fieldName, DataTypes.createStructType(structFields), structFields);
    } else if (DataTypes.isMapType(dataType)) {
      // Internally Map<key, value> is stored as Array<struct<key, value>>. So the below method
      // will convert a map type into similar field structure. The columnSchema will be formed
      // as Map<Struct<key,value>>
      List<StructField> mapFields = new ArrayList<>();
      MapType mapType = (MapType) dataType;
      // key is primitive type so type can be fetched directly
      StructField keyField = new StructField(fieldName + ".key", mapType.getKeyType());
      StructField valueField = prepareSubFields(fieldName + ".value", mapType.getValueType());
      mapFields.add(keyField);
      mapFields.add(valueField);
      StructField field =
          new StructField(fieldName + ".val", DataTypes.createStructType(mapFields));
      MapType mapDataType = DataTypes.createMapType(keyField.getDataType(), field.getDataType());
      List<StructField> mapStructField = new ArrayList<>();
      mapStructField.add(field);
      return new StructField(fieldName, mapDataType, mapStructField);
    } else {
      return new StructField(fieldName, dataType);
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Field) {
      Field field = (Field) obj;
      if ((!this.getDataType().equals(field.getDataType()))
          || (!this.getFieldName().equals(field.getFieldName()))
          || (!(this.getSchemaOrdinal() == (field.getSchemaOrdinal())))
          ) {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }
}
