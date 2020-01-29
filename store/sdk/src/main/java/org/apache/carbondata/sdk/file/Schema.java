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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * A schema used to write and read data files
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class Schema {

  private Field[] fields;

  private Map<String, String> properties;

  /**
   * construct a schema with fields
   * @param fields
   */
  public Schema(Field[] fields) {
    this(fields, new HashMap<String, String>());
  }

  /**
   * construct a schema with fields
   * @param fields
   */
  public Schema(Field[] fields, Map<String, String> properties) {
    this.fields = fields;
    this.properties = properties;
  }

  /**
   * construct a schema with List<ColumnSchema>
   *
   * @param columnSchemaList column schema list
   */
  public Schema(List<ColumnSchema> columnSchemaList) {
    this(columnSchemaList, new HashMap<String, String>());
  }

  /**
   * construct a schema with List<ColumnSchema>
   *
   * @param columnSchemaList column schema list
   */
  public Schema(List<ColumnSchema> columnSchemaList, Map<String, String> properties) {
    fields = new Field[columnSchemaList.size()];
    for (int i = 0; i < columnSchemaList.size(); i++) {
      fields[i] = new Field(columnSchemaList.get(i));
    }
    this.properties = properties;
  }

  /**
   * Create a Schema using JSON string, for example:
   * [
   *   {"name":"string"},
   *   {"age":"int"}
   * ]
   * @param json specified as string
   * @return Schema
   */
  public static Schema parseJson(String json) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(Field.class, new TypeAdapter<Field>() {
      @Override
      public void write(JsonWriter out, Field field) {
        // noop
      }

      @Override
      public Field read(JsonReader in) throws IOException {
        in.beginObject();
        Field field = new Field(in.nextName(), in.nextString());
        in.endObject();
        return field;
      }
    });

    Field[] fields = gsonBuilder.create().fromJson(json, Field[].class);
    return new Schema(fields);
  }

  public Field[] getFields() {
    return fields;
  }

  /**
   * get fields length of schema
   *
   * @return fields length
   */
  public int getFieldsLength() {
    return fields.length;
  }

  /**
   * get field name by ordinal
   *
   * @param ordinal the data index of carbon schema
   * @return ordinal field name
   */
  public String getFieldName(int ordinal) {
    return fields[ordinal].getFieldName();
  }

  /**
   * get  field data type name by ordinal
   *
   * @param ordinal the data index of carbon schema
   * @return ordinal field data type name
   */
  public String getFieldDataTypeName(int ordinal) {
    return fields[ordinal].getDataType().getName();
  }

  /**
   * get  array child element data type name by ordinal
   *
   * @param ordinal the data index of carbon schema
   * @return ordinal array child element data type name
   */
  public String getArrayElementTypeName(int ordinal) {
    if (getFieldDataTypeName(ordinal).equalsIgnoreCase("ARRAY")) {
      return ((ArrayType) fields[ordinal].getDataType()).getElementType().getName();
    }
    throw new RuntimeException("Only support Array type.");
  }

  /**
   * Sort the schema order as original order
   *
   * @return Schema object
   */
  public Schema asOriginOrder() {
    Arrays.sort(fields, new Comparator<Field>() {
      @Override
      public int compare(Field o1, Field o2) {
        return Integer.compare(o1.getSchemaOrdinal(), o2.getSchemaOrdinal());
      }
    });
    return this;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Schema) {
      Schema schema = (Schema) obj;
      for (int i = 0; i < this.fields.length; i++) {
        if (!(schema.fields)[i].equals((this.fields)[i])) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }
}
