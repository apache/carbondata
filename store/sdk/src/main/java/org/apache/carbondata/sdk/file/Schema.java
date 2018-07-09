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
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
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

  /**
   * construct a schema with fields
   * @param fields
   */
  public Schema(Field[] fields) {
    this.fields = fields;
  }

  /**
   * construct a schema with List<ColumnSchema>
   *
   * @param columnSchemaList column schema list
   */
  public Schema(List<ColumnSchema> columnSchemaList) {
    fields = new Field[columnSchemaList.size()];
    for (int i = 0; i < columnSchemaList.size(); i++) {
      fields[i] = new Field(columnSchemaList.get(i));
    }
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
      public void write(JsonWriter out, Field field) throws IOException {
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
}
