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

package org.apache.carbondata.core.dictionary.client;

import java.io.File;
import java.util.Arrays;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test dictionary client functionality.
 */
public class DictionaryClientTest {

  private ColumnSchema empColumnSchema;
  private CarbonDimension empDimension;
  private ColumnSchema ageColumnSchema;
  private CarbonDimension ageDimension;
  private TableSchema tableSchema;
  private TableInfo tableInfo;
  private String storePath;
  private DictionaryServer server;

  @Before public void setUp() throws Exception {
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");

    // Create two column schemas and dimensions for the table
    empColumnSchema = new ColumnSchema();
    empColumnSchema.setColumnName("empNameCol");
    empColumnSchema.setColumnUniqueId("empNameCol");
    empColumnSchema.setDimensionColumn(true);
    empColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    empDimension = new CarbonDimension(empColumnSchema, 0, 0, 0, 0, 0);

    ageColumnSchema = new ColumnSchema();
    ageColumnSchema.setColumnName("empNameCol");
    ageColumnSchema.setColumnUniqueId("empNameCol");
    ageColumnSchema.setDimensionColumn(true);
    ageColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    ageDimension = new CarbonDimension(ageColumnSchema, 0, 0, 0, 0, 0);

    // Create a Table
    tableSchema = new TableSchema();
    tableSchema.setTableName("TestTable");
    tableSchema.setListOfColumns(Arrays.asList(empColumnSchema, ageColumnSchema));
    CarbonMetadata metadata = CarbonMetadata.getInstance();

    tableInfo = new TableInfo();
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTableUniqueName("TestTable");
    tableInfo.setDatabaseName("test");
    storePath = System.getProperty("java.io.tmpdir") + "/tmp";
    tableInfo.setStorePath(storePath);
    CarbonTable carbonTable = new CarbonTable();
    carbonTable.loadCarbonTable(tableInfo);

    // Add the created table to metadata
    metadata.addCarbonTable(carbonTable);

    // Start the server for testing the client
    server = new DictionaryServer();
    server.startServer(5678);
  }

  @Test public void testClient() throws Exception {
    DictionaryClient client = new DictionaryClient();
    client.startClient("localhost", 5678);

    Thread.sleep(1000);
    // Create a dictionary key
    DictionaryMessage empKey = new DictionaryMessage();
    empKey.setTableUniqueName(tableInfo.getTableUniqueName());
    empKey.setColumnName(empColumnSchema.getColumnName());
    empKey.setData("FirstKey");

    // Test dictionary initialization call
    empKey.setType(DictionaryMessageType.TABLE_INTIALIZATION);
    client.getDictionary(empKey);
    int count = 2;
    // Test dictionary generation
    for (; count <= 10000; count++) {
      empKey.setType(DictionaryMessageType.DICT_GENERATION);
      empKey.setData("FirstKey" + count);
      DictionaryMessage val = client.getDictionary(empKey);
      Assert.assertEquals(count, val.getDictionaryValue());
    }

    // Test dictionary generation with big messages
    for (; count <= 10010; count++) {
      empKey.setType(DictionaryMessageType.DICT_GENERATION);
      empKey.setData(
          "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + "FirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKeyFirstKey"
              + count);
      DictionaryMessage val = client.getDictionary(empKey);
      Assert.assertEquals(count, val.getDictionaryValue());
    }
    // Test size function
    empKey.setType(DictionaryMessageType.SIZE);
    DictionaryMessage val = client.getDictionary(empKey);
    Assert.assertEquals(10010, val.getDictionaryValue());


    client.shutDown();

    // Shutdown the server
    server.shutdown();
  }

  @After public void tearDown() {
    // Cleanup created files
    CarbonMetadata.getInstance().removeTable(tableInfo.getTableUniqueName());
    cleanUpDirectory(new File(storePath));
  }

  private void cleanUpDirectory(File path) {
    File[] files = path.listFiles();
    if (null == files) {
      return;
    }
    for (File file : files) {
      if (file.isDirectory()) cleanUpDirectory(file);
      else file.delete();
    }
    path.delete();
  }

}
