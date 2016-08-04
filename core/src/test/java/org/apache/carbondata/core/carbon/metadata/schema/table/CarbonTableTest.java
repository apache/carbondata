/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.metadata.schema.table;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonTableTest extends TestCase {

  private CarbonTable carbonTable;

  @BeforeClass public void setUp() {
    carbonTable = new CarbonTable();
    carbonTable.loadCarbonTable(getTableInfo(1000L));
  }

  @AfterClass public void tearDown() {
    carbonTable = null;
  }

  @Test public void testNumberOfDimensionReturnsProperCount() {
    assertEquals(1, carbonTable.getNumberOfDimensions("carbonTestTable"));
  }

  @Test public void testNumberOfMeasureReturnsProperCount() {
    assertEquals(1, carbonTable.getNumberOfMeasures("carbonTestTable"));
  }

  @Test public void testGetDatabaseNameResturnsDatabaseName() {
    assertEquals("carbonTestDatabase", carbonTable.getDatabaseName());
  }

  @Test public void testFactTableNameReturnsProperFactTableName() {
    assertEquals("carbonTestTable", carbonTable.getFactTableName());
  }

  @Test public void testTableUniqueNameIsProper() {
    assertEquals("carbonTestDatabase_carbonTestTable", carbonTable.getTableUniqueName());
  }

  @Test public void testDimensionPresentInTableIsProper() {
    CarbonDimension dimension = new CarbonDimension(getColumnarDimensionColumn(), 0, -1, -1,-1);
    assertTrue(carbonTable.getDimensionByName("carbonTestTable", "IMEI").equals(dimension));
  }

  private ColumnSchema getColumnarDimensionColumn() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataType.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getColumnarMeasureColumn() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setAggregateFunction("SUM");
    dimColumn.setColumnName("IMEI_COUNT");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataType.STRING);
    return dimColumn;
  }

  private TableSchema getTableSchema() {
    TableSchema tableSchema = new TableSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    columnSchemaList.add(getColumnarDimensionColumn());
    columnSchemaList.add(getColumnarMeasureColumn());
    tableSchema.setListOfColumns(columnSchemaList);
    tableSchema.setTableId(UUID.randomUUID().toString());
    tableSchema.setTableName("carbonTestTable");
    return tableSchema;
  }

  private TableInfo getTableInfo(long timeStamp) {
    TableInfo info = new TableInfo();
    info.setDatabaseName("carbonTestDatabase");
    info.setLastUpdatedTime(timeStamp);
    info.setTableUniqueName("carbonTestDatabase_carbonTestTable");
    info.setFactTable(getTableSchema());
    info.setStorePath("testore");
    return info;
  }

}
