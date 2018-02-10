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

import org.junit.Assert;
import org.junit.Test;

public class CarbonTableBuilderSuite {

  TableSchema schema = CarbonTableTest.getTableSchema("t1");

  @Test(expected = NullPointerException.class)
  public void testNullTableName() {
    TableSchema schema = CarbonTableTest.getTableSchema(null);
    CarbonTable table = CarbonTable.builder()
        .tableName(null)
        .databaseName("db1")
        .tableSchema(schema)
        .tablePath("_temp")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testNullDbName() {
    CarbonTable table = CarbonTable.builder()
        .tableName(schema.getTableName())
        .databaseName(null)
        .tableSchema(schema)
        .tablePath("_temp")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testNullSchema() {
    CarbonTable table = CarbonTable.builder()
        .tableName(schema.getTableName())
        .databaseName("db1")
        .tableSchema(null)
        .tablePath("_temp")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testNullTablePath() {
    CarbonTable table = CarbonTable.builder()
        .tableName(schema.getTableName())
        .databaseName("db1")
        .tableSchema(schema)
        .tablePath(null)
        .build();
  }

  @Test
  public void testBuilder() {
    CarbonTable table = CarbonTable.builder()
        .tableName(schema.getTableName())
        .databaseName("db1")
        .tableSchema(schema)
        .tablePath("_temp")
        .build();
    Assert.assertEquals(schema.getTableName(), table.getTableName());
    Assert.assertEquals("db1", table.getDatabaseName());
    Assert.assertEquals("_temp", table.getTablePath());
    Assert.assertEquals(schema.getTableName(), table.getAbsoluteTableIdentifier().getTableName());
    Assert.assertEquals("db1", table.getAbsoluteTableIdentifier().getDatabaseName());
    Assert.assertEquals("_temp", table.getAbsoluteTableIdentifier().getTablePath());
    Assert.assertEquals("db1_t1", table.getTableUniqueName());
    Assert.assertEquals(schema, table.getTableInfo().getFactTable());
  }

}
