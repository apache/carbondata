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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang.StringUtils;

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

  public List<String> prepareSortColumns(Map<String, String> properties)
      throws MalformedCarbonCommandException {

    List<String> sortColumnsList = new ArrayList<>();
    Set<Map.Entry<String, String>> entries = properties.entrySet();
    String sortKeyString = null;
    for (Map.Entry<String, String> entry : entries) {
      if (CarbonCommonConstants.SORT_COLUMNS.equalsIgnoreCase(entry.getKey())) {
        sortKeyString = CarbonUtil.unquoteChar(entry.getValue()).trim();
      }
    }

    if (sortKeyString != null) {
      String[] sortKeys = sortKeyString.split(",", -1);
      for (int i = 0; i < sortKeys.length; i++) {
        sortKeys[i] = sortKeys[i].trim().toLowerCase();
        if (StringUtils.isEmpty(sortKeys[i])) {
          throw new MalformedCarbonCommandException("SORT_COLUMNS contains illegal argument.");
        }
      }

      for (int i = sortKeys.length - 2; i >= 0; i--) {
        for (int j = i + 1; j < sortKeys.length; j++) {
          if (sortKeys[i].equals(sortKeys[j])) {
            throw new MalformedCarbonCommandException(
                "SORT_COLUMNS Either having duplicate columns : " + sortKeys[i]);
          }
        }
      }

      for (int i = sortKeys.length - 1; i >= 0; i--) {
        boolean isExists = false;
        for (int j = fields.length - 1; j >= 0; j--) {
          if (sortKeys[i].equalsIgnoreCase(fields[j].getFieldName())) {
            sortKeys[i] = fields[j].getFieldName();
            isExists = true;
            break;
          }
        }
        if (!isExists) {
          String message = "sort_columns: " + sortKeys[i]
              + " does not exist in table. Please check create table statement.";
          throw new MalformedCarbonCommandException(message);
        }
      }
      sortColumnsList = Arrays.asList(sortKeys);
    } else {
      for (Field field : fields) {
        if (null != field) {
          if (field.getDataType() == DataTypes.STRING || field.getDataType() == DataTypes.DATE
              || field.getDataType() == DataTypes.TIMESTAMP) {
            sortColumnsList.add(field.getFieldName());
          }
        }
      }
    }
    return sortColumnsList;
  }
}
