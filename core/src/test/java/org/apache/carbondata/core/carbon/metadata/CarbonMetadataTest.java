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
package org.apache.carbondata.core.carbon.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonMetadataTest extends TestCase {

  private CarbonMetadata carbonMetadata;

  private String tableUniqueName;

  @BeforeClass public void setUp() {
    carbonMetadata = CarbonMetadata.getInstance();
    carbonMetadata.loadTableMetadata(getTableInfo(10000));
    tableUniqueName = "carbonTestDatabase_carbonTestTable";
  }

  @AfterClass public void tearDown() {
    carbonMetadata.removeTable(tableUniqueName);
    carbonMetadata = null;
  }

  @Test public void testOnlyOneInstanceIsCreatedofCarbonMetadata() {
    CarbonMetadata carbonMetadata1 = CarbonMetadata.getInstance();
    assertTrue(carbonMetadata == carbonMetadata1);
  }

  @Test public void testNumberOfTablesPresentInTheMetadata() {
    assertEquals(1, carbonMetadata.getNumberOfTables());
  }

  @Test public void testGetCarbonTableReturingProperCarbonTable() {
    assertTrue(null != carbonMetadata.getCarbonTable(tableUniqueName));
  }

  @Test public void testGetCarbonTableReturingNullWhenInvalidNameIsPassed() {
    assertTrue(null == carbonMetadata.getCarbonTable("notpresent"));
  }

  @Test public void testGetCarbonTableReturingProperTableWithProperDimensionCount() {
    assertEquals(1,
        carbonMetadata.getCarbonTable(tableUniqueName).getNumberOfDimensions("carbonTestTable"));
  }

  @Test public void testGetCarbonTableReturingProperTableWithProperMeasureCount() {
    assertEquals(1,
        carbonMetadata.getCarbonTable(tableUniqueName).getNumberOfMeasures("carbonTestTable"));
  }

  @Test public void testGetCarbonTableReturingProperTableWithProperDatabaseName() {
    assertEquals("carbonTestDatabase",
        carbonMetadata.getCarbonTable(tableUniqueName).getDatabaseName());
  }

  @Test public void testGetCarbonTableReturingProperTableWithProperFactTableName() {
    assertEquals("carbonTestTable",
        carbonMetadata.getCarbonTable(tableUniqueName).getFactTableName());
  }

  @Test public void testGetCarbonTableReturingProperTableWithProperTableUniqueName() {
    assertEquals("carbonTestDatabase_carbonTestTable",
        carbonMetadata.getCarbonTable(tableUniqueName).getTableUniqueName());
  }

  @Test public void testloadMetadataTableWhenTableIsAlreadyExistsAndTimeStampIsChanged() {
    assertEquals(10000, carbonMetadata.getCarbonTable(tableUniqueName).getTableLastUpdatedTime());
    CarbonMetadata carbonMetadata1 = CarbonMetadata.getInstance();
    carbonMetadata1.loadTableMetadata(getTableInfo(100001));
    assertEquals(100001, carbonMetadata.getCarbonTable(tableUniqueName).getTableLastUpdatedTime());
    assertEquals(1, carbonMetadata.getNumberOfTables());
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
    info.setStorePath("/test/store");
    return info;
  }
}
