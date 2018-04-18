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

import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.junit.Assert;
import org.junit.Test;

public class TableSchemaBuilderSuite {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    TableSchemaBuilder builder = TableSchema.builder();
    builder.addColumn(null, true);
  }

  @Test
  public void testBuilder() {
    TableSchemaBuilder builder = TableSchema.builder();
    builder.addColumn(new StructField("a", DataTypes.INT), true);
    builder.addColumn(new StructField("b", DataTypes.DOUBLE), false);
    TableSchema schema = builder.build();
    Assert.assertEquals(2, schema.getListOfColumns().size());
    List<ColumnSchema> columns = schema.getListOfColumns();
    Assert.assertEquals("a", columns.get(0).getColumnName());
    Assert.assertEquals("b", columns.get(1).getColumnName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRepeatedColumn() {
    TableSchemaBuilder builder = TableSchema.builder();
    builder.addColumn(new StructField("a", DataTypes.INT), true);
    builder.addColumn(new StructField("a", DataTypes.DOUBLE), false);
    TableSchema schema = builder.build();
  }
}
