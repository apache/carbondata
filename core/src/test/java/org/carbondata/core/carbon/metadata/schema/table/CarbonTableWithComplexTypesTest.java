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
package org.carbondata.core.carbon.metadata.schema.table;

import junit.framework.TestCase;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class CarbonTableWithComplexTypesTest extends TestCase {

    private CarbonTable carbonTable;

    @BeforeClass public void setUp() {
        carbonTable = new CarbonTable();
        carbonTable.loadCarbonTable(getTableInfo(1000l));
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
        assertEquals("carbonTestTable", carbonTable.getFactTableName());
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
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        cols.add(dimColumn);
        
        ColumnSchema structColumn = new ColumnSchema();
        structColumn.setColumnar(true);
        structColumn.setColumnName("mobile");
        structColumn.setColumnUniqueId(UUID.randomUUID().toString());
        structColumn.setDataType(DataType.STRUCT);
        structColumn.setDimensionColumn(true);
        structColumn.setEncodintList(encodeList);
        structColumn.setNumberOfChild(2);
        cols.add(structColumn);
        
        ColumnSchema primitiveColumn = new ColumnSchema();
        primitiveColumn.setColumnar(true);
        primitiveColumn.setColumnName("mobile.stdcode");
        primitiveColumn.setColumnUniqueId(UUID.randomUUID().toString());
        primitiveColumn.setDataType(DataType.STRING);
        primitiveColumn.setDimensionColumn(true);
        primitiveColumn.setEncodintList(encodeList);
        primitiveColumn.setNumberOfChild(0);
        cols.add(primitiveColumn);
        
        ColumnSchema arrayColumn = new ColumnSchema();
        arrayColumn.setColumnar(true);
        arrayColumn.setColumnName("mobile.val");
        arrayColumn.setColumnUniqueId(UUID.randomUUID().toString());
        arrayColumn.setDataType(DataType.ARRAY);
        arrayColumn.setDimensionColumn(true);
        arrayColumn.setEncodintList(encodeList);
        arrayColumn.setNumberOfChild(1);
        cols.add(arrayColumn);
        
        ColumnSchema primitiveColumn1 = new ColumnSchema();
        primitiveColumn1.setColumnar(true);
        primitiveColumn1.setColumnName("mobile.val.phoneno");
        primitiveColumn1.setColumnUniqueId(UUID.randomUUID().toString());
        primitiveColumn1.setDataType(DataType.STRING);
        primitiveColumn1.setDimensionColumn(true);
        primitiveColumn1.setEncodintList(encodeList);
        primitiveColumn1.setNumberOfChild(0);
        cols.add(primitiveColumn1);
        
        return cols;
    }

    private ColumnSchema getColumnarMeasureColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setAggregateFunction("SUM");
        dimColumn.setColumnName("IMEI_COUNT");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(false);
        return dimColumn;
    }

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.addAll(getColumnarDimensionColumn());
        columnSchemaList.add(getColumnarMeasureColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableId(1);
        tableSchema.setTableName("carbonTestTable");
        return tableSchema;
    }

    private TableInfo getTableInfo(long timeStamp) {
        TableInfo info = new TableInfo();
        info.setDatabaseName("carbonTestDatabase");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("carbonTestDatabase_carbonTestTable");
        info.setFactTable(getTableSchema());
        return info;
    }

}
