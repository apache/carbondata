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
package org.apache.carbondata.core.metadata.schema.table;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypeDeserializer;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

public class ColumnSchemaTest {

  @Test
  public void testDataTypeOnColumnSchemaObject() {
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setDataType(DataTypes.createDefaultArrayType());

    // without DataTypeDeserializer, after deserialization, data type is main class type instead
    // of child class type
    Gson gson = new Gson();
    String serializedColumnSchema = gson.toJson(columnSchema);
    ColumnSchema newColumnSchema = gson.fromJson(serializedColumnSchema, ColumnSchema.class);
    Assert.assertFalse(newColumnSchema.getDataType().isComplexType());

    // using DataTypeDeserializer
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(DataType.class, new DataTypeDeserializer());
    Gson newGson = gsonBuilder.create();
    ColumnSchema newColumnSchemaObj = newGson.fromJson(serializedColumnSchema, ColumnSchema.class);
    Assert.assertTrue(newColumnSchemaObj.getDataType().isComplexType());
  }

}
