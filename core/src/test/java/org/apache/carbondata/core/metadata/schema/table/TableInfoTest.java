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

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.util.CarbonUtil;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableInfoTest extends TestCase {

  private TableInfo tableInfo;

  @BeforeClass public void setUp() {
    tableInfo = getTableInfo("tableInfoTestDatabase", "equalsTableInfoTestTable");
  }

  @AfterClass public void tearDown() {
    tableInfo = null;
  }

  @Test public void testTableInfoEquals() {
    TableInfo cmpEqualsTableInfo =
        getTableInfo("tableInfoTestDatabase", "equalsTableInfoTestTable");
    TableInfo cmpNotEqualsTableInfo =
        getTableInfo("tableInfoTestDatabase", "notEqualsTableInfoTestTable");
    assertTrue(tableInfo.equals(cmpEqualsTableInfo));
    assertTrue(!(tableInfo.equals(cmpNotEqualsTableInfo)));
  }

  private TableInfo getTableInfo(String databaseName, String tableName) {
    TableInfo info = new TableInfo();
    info.setDatabaseName("tableInfoTestDatabase");
    info.setLastUpdatedTime(1000L);
    info.setTableUniqueName(CarbonTable.buildUniqueName(databaseName, tableName));
    return info;
  }

  /**
   * test with field name schemaEvalution
   */
  @Test public void testTableInfoDeserializationWithSchemaEvalution() {
    Map<String, String> properties = new HashMap<>();
    properties.put("carbonSchemaPartsNo", "1");
    properties.put("carbonSchema0", "{\"databaseName\":\"carbonversion_1_1\",\"tableUniqueName\":"
        + "\"carbonversion_1_1_testinttype1\",\"factTable\":{\"tableId\":"
        + "\"8ef75d32-b5b7-4c6b-9b1c-16059b4f5f26\",\"tableName\":\"testinttype1\","
        + "\"listOfColumns\":[{\"dataType\":{\"id\":0,\"precedenceOrder\":0,\"name\":"
        + "\"STRING\",\"sizeInBytes\":-1},\"columnName\":\"c1\",\"columnUniqueId\":"
        + "\"5347ee48-d3ca-43a4-bd88-a818fa42a2a3\",\"columnReferenceId\":"
        + "\"5347ee48-d3ca-43a4-bd88-a818fa42a2a3\",\"isColumnar\":true,"
        + "\"encodingList\":[\"INVERTED_INDEX\"],\"isDimensionColumn\":true,"
        + "\"columnGroupId\":-1,\"scale\":0,\"precision\":0,\"schemaOrdinal\":0,"
        + "\"numberOfChild\":0,\"invisible\":false,\"isSortColumn\":true,"
        + "\"aggFunction\":\"\",\"timeSeriesFunction\":\"\",\"isLocalDictColumn\":true},"
        + "{\"dataType\":{\"id\":5,\"precedenceOrder\":3,\"name\":\"INT\","
        + "\"sizeInBytes\":4},\"columnName\":\"c2\",\"columnUniqueId\":"
        + "\"9c38b74d-5b56-4554-adbf-b6c7ead63ee2\",\"columnReferenceId\":"
        + "\"9c38b74d-5b56-4554-adbf-b6c7ead63ee2\",\"isColumnar\":true,"
        + "\"encodingList\":[],\"isDimensionColumn\":false,\"columnGroupId\":-1,"
        + "\"scale\":0,\"precision\":0,\"schemaOrdinal\":1,\"numberOfChild\":0,"
        + "\"invisible\":false,\"isSortColumn\":false,\"aggFunction\":\"\","
        + "\"timeSeriesFunction\":\"\",\"isLocalDictColumn\":false}],\"schemaEvalution\":"
        + "{\"schemaEvolutionEntryList\":[{\"timeStamp\":1530534235537}]},"
        + "\"tableProperties\":{\"sort_columns\":\"c1\",\"comment\":\"\","
        + "\"local_dictionary_enable\":\"true\"}},\"lastUpdatedTime\":1530534235537,"
        + "\"tablePath\":\"/store/carbonversion_1_1/testinttype1\","
        + "\"isTransactionalTable\":true,\"isSchemaModified\":false}");
    TableInfo tableInfo = CarbonUtil.convertGsonToTableInfo(properties);
    // the schema evolution should not be null
    assertTrue(null != tableInfo.getFactTable().getSchemaEvolution());
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
    ThriftWrapperSchemaConverterImpl schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(carbonTable.getTableInfo(), carbonTable.getDatabaseName(),
            carbonTable.getTableName());
    assertTrue(null != thriftTable);
  }

  /**
   * test with field name schemaEvolution
   */
  @Test public void testTableInfoDeserializationWithSchemaEvolution() {
    Map<String, String> properties = new HashMap<>();
    properties.put("carbonSchemaPartsNo", "1");
    properties.put("carbonSchema0", "{\"databaseName\":\"carbonversion_1_1\",\"tableUniqueName\":"
        + "\"carbonversion_1_1_testinttype1\",\"factTable\":{\"tableId\":"
        + "\"8ef75d32-b5b7-4c6b-9b1c-16059b4f5f26\",\"tableName\":\"testinttype1\","
        + "\"listOfColumns\":[{\"dataType\":{\"id\":0,\"precedenceOrder\":0,\"name\":"
        + "\"STRING\",\"sizeInBytes\":-1},\"columnName\":\"c1\",\"columnUniqueId\":"
        + "\"5347ee48-d3ca-43a4-bd88-a818fa42a2a3\",\"columnReferenceId\":"
        + "\"5347ee48-d3ca-43a4-bd88-a818fa42a2a3\",\"isColumnar\":true,"
        + "\"encodingList\":[\"INVERTED_INDEX\"],\"isDimensionColumn\":true,"
        + "\"columnGroupId\":-1,\"scale\":0,\"precision\":0,\"schemaOrdinal\":0,"
        + "\"numberOfChild\":0,\"invisible\":false,\"isSortColumn\":true,"
        + "\"aggFunction\":\"\",\"timeSeriesFunction\":\"\",\"isLocalDictColumn\":true},"
        + "{\"dataType\":{\"id\":5,\"precedenceOrder\":3,\"name\":\"INT\","
        + "\"sizeInBytes\":4},\"columnName\":\"c2\",\"columnUniqueId\":"
        + "\"9c38b74d-5b56-4554-adbf-b6c7ead63ee2\",\"columnReferenceId\":"
        + "\"9c38b74d-5b56-4554-adbf-b6c7ead63ee2\",\"isColumnar\":true,"
        + "\"encodingList\":[],\"isDimensionColumn\":false,\"columnGroupId\":-1,"
        + "\"scale\":0,\"precision\":0,\"schemaOrdinal\":1,\"numberOfChild\":0,"
        + "\"invisible\":false,\"isSortColumn\":false,\"aggFunction\":\"\","
        + "\"timeSeriesFunction\":\"\",\"isLocalDictColumn\":false}],\"schemaEvolution\":"
        + "{\"schemaEvolutionEntryList\":[{\"timeStamp\":1530534235537}]},"
        + "\"tableProperties\":{\"sort_columns\":\"c1\",\"comment\":\"\","
        + "\"local_dictionary_enable\":\"true\"}},\"lastUpdatedTime\":1530534235537,"
        + "\"tablePath\":\"/store/carbonversion_1_1/testinttype1\","
        + "\"isTransactionalTable\":true,"
        + "\"parentRelationIdentifiers\":[],\"isSchemaModified\":false}");
    TableInfo tableInfo = CarbonUtil.convertGsonToTableInfo(properties);
    // the schema evolution should not be null
    assertTrue(null != tableInfo.getFactTable().getSchemaEvolution());
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
    ThriftWrapperSchemaConverterImpl schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(carbonTable.getTableInfo(), carbonTable.getDatabaseName(),
            carbonTable.getTableName());
    assertTrue(null != thriftTable);
  }

  @Test public void testIndexSchemaDeserializationWithClassName() {
    Map<String, String> properties = new HashMap<>();
    properties.put("carbonSchemaPartsNo", "1");
    properties.put("carbonSchema0", "{\"databaseName\":\"carbonversion_1_3\",\"tableUniqueName\":"
        + "\"carbonversion_1_3_testinttype3\",\"factTable\":{\"tableId\":"
        + "\"453fa0dd-721d-41b7-9378-f6d6122daf36\",\"tableName\":\"testinttype3\","
        + "\"listOfColumns\":[{\"dataType\":{\"id\":0,\"precedenceOrder\":0,\"name\":"
        + "\"STRING\",\"sizeInBytes\":-1},\"columnName\":\"c1\",\"columnUniqueId\":"
        + "\"c84e7e3b-5682-4b46-8c72-0f2f341a0a49\",\"columnReferenceId\":"
        + "\"c84e7e3b-5682-4b46-8c72-0f2f341a0a49\",\"isColumnar\":true,\"encodingList"
        + "\":[\"INVERTED_INDEX\"],\"isDimensionColumn\":true,\"columnGroupId\":-1,"
        + "\"scale\":-1,\"precision\":-1,\"schemaOrdinal\":0,\"numberOfChild\":0,"
        + "\"invisible\":false,\"isSortColumn\":true,\"aggFunction\":\"\","
        + "\"timeSeriesFunction\":\"\"},{\"dataType\":{\"id\":5,\"precedenceOrder\":3,"
        + "\"name\":\"INT\",\"sizeInBytes\":4},\"columnName\":\"c2\",\"columnUniqueId\":"
        + "\"008dc283-beca-4a3e-ad40-b7916aa67795\",\"columnReferenceId\":"
        + "\"008dc283-beca-4a3e-ad40-b7916aa67795\",\"isColumnar\":true,"
        + "\"encodingList\":[],\"isDimensionColumn\":false,\"columnGroupId\":-1,"
        + "\"scale\":-1,\"precision\":-1,\"schemaOrdinal\":1,\"numberOfChild\":0,"
        + "\"invisible\":false,\"isSortColumn\":false,\"aggFunction\":\"\","
        + "\"timeSeriesFunction\":\"\"}],\"schemaEvalution\":{\"schemaEvolutionEntryList"
        + "\":[{\"timeStamp\":1531389794988,\"added\":[],\"removed\":[]}]},"
        + "\"tableProperties\":{\"sort_columns\":\"c1\",\"comment\":\"\"}},"
        + "\"lastUpdatedTime\":1531389794988,\"tablePath\":"
        + "\"/opt/store/carbonversion_1_3/testinttype3\"}");
    TableInfo tableInfo = CarbonUtil.convertGsonToTableInfo(properties);
    // the schema evolution should not be null
    assertTrue(null != tableInfo.getFactTable());
    assertTrue(null != tableInfo.getFactTable().getListOfColumns());
    assertTrue(null != tableInfo.getFactTable().getTableProperties());
  }
}
