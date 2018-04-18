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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonTableWithComplexTypesTest extends TestCase {

  private CarbonTable carbonTable;

  @BeforeClass public void setUp() {
    carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(1000L));
  }

  @AfterClass public void tearDown() {
    carbonTable = null;
  }

  @Test public void testNumberOfDimensionReturnsProperCount() {
    assertEquals(2, carbonTable.getNumberOfDimensions("carbonTestTable"));
  }

  @Test public void testNumberOfMeasureReturnsProperCount() {
    assertEquals(1, carbonTable.getNumberOfMeasures("carbonTestTable"));
  }

  @Test public void testGetDatabaseNameResturnsDatabaseName() {
    assertEquals("carbonTestDatabase", carbonTable.getDatabaseName());
  }

  @Test public void testFactTableNameReturnsProperFactTableName() {
    assertEquals("carbonTestTable", carbonTable.getTableName());
  }

  @Test public void testTableUniqueNameIsProper() {
    assertEquals("carbonTestDatabase_carbonTestTable", carbonTable.getTableUniqueName());
  }

  private List<ColumnSchema> getColumnarDimensionColumn() {

    List<ColumnSchema> cols = new ArrayList<ColumnSchema>();

    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    cols.add(dimColumn);

    ColumnSchema structColumn = new ColumnSchema();
    structColumn.setColumnar(true);
    structColumn.setColumnName("mobile");
    structColumn.setColumnUniqueId(UUID.randomUUID().toString());
    structColumn.setDataType(DataTypes.createDefaultStructType());
    structColumn.setDimensionColumn(true);
    structColumn.setEncodingList(encodeList);
    structColumn.setNumberOfChild(2);
    cols.add(structColumn);

    ColumnSchema primitiveColumn = new ColumnSchema();
    primitiveColumn.setColumnar(true);
    primitiveColumn.setColumnName("mobile.stdcode");
    primitiveColumn.setColumnUniqueId(UUID.randomUUID().toString());
    primitiveColumn.setDataType(DataTypes.STRING);
    primitiveColumn.setDimensionColumn(true);
    primitiveColumn.setEncodingList(encodeList);
    primitiveColumn.setNumberOfChild(0);
    cols.add(primitiveColumn);

    ColumnSchema arrayColumn = new ColumnSchema();
    arrayColumn.setColumnar(true);
    arrayColumn.setColumnName("mobile.val");
    arrayColumn.setColumnUniqueId(UUID.randomUUID().toString());
    arrayColumn.setDataType(DataTypes.createDefaultArrayType());
    arrayColumn.setDimensionColumn(true);
    arrayColumn.setEncodingList(encodeList);
    arrayColumn.setNumberOfChild(1);
    cols.add(arrayColumn);

    ColumnSchema primitiveColumn1 = new ColumnSchema();
    primitiveColumn1.setColumnar(true);
    primitiveColumn1.setColumnName("mobile.val.phoneno");
    primitiveColumn1.setColumnUniqueId(UUID.randomUUID().toString());
    primitiveColumn1.setDataType(DataTypes.STRING);
    primitiveColumn1.setDimensionColumn(true);
    primitiveColumn1.setEncodingList(encodeList);
    primitiveColumn1.setNumberOfChild(0);
    cols.add(primitiveColumn1);

    return cols;
  }

  private ColumnSchema getColumnarMeasureColumn() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI_COUNT");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(false);
    return dimColumn;
  }

  private TableSchema getTableSchema() {
    TableSchema tableSchema = new TableSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    columnSchemaList.addAll(getColumnarDimensionColumn());
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
    info.setTablePath("testStore");
    return info;
  }

}
