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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.core.dictionary.server.NonSecureDictionaryServer;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;

import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class to test dictionary client functionality.
 */
public class DictionaryClientTest {

  private static ColumnSchema empColumnSchema;
  private static CarbonDimension empDimension;
  private static ColumnSchema ageColumnSchema;
  private static CarbonDimension ageDimension;
  private static TableSchema tableSchema;
  private static TableInfo tableInfo;
  private static String storePath;
  private static DictionaryServer server;
  private static String host;

  @BeforeClass public static void setUp() throws Exception {
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");

    // Create two column schemas and dimensions for the table
    empColumnSchema = new ColumnSchema();
    empColumnSchema.setColumnName("empNameCol");
    empColumnSchema.setColumnUniqueId("empNameCol");
    empColumnSchema.setDimensionColumn(true);
    empColumnSchema.setDataType(DataTypes.STRING);
    empColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    empDimension = new CarbonDimension(empColumnSchema, 0, 0, 0, 0);

    ageColumnSchema = new ColumnSchema();
    ageColumnSchema.setColumnName("ageNameCol");
    ageColumnSchema.setDataType(DataTypes.SHORT_INT);
    ageColumnSchema.setColumnUniqueId("ageNameCol");
    ageColumnSchema.setDimensionColumn(true);
    ageColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    ageDimension = new CarbonDimension(ageColumnSchema, 0, 0, 0, 0);

    // Create a Table
    tableSchema = new TableSchema();
    tableSchema.setTableName("TestTable");
    tableSchema.setTableId("1");
    tableSchema.setListOfColumns(Arrays.asList(empColumnSchema, ageColumnSchema));
    CarbonMetadata metadata = CarbonMetadata.getInstance();

    tableInfo = new TableInfo();
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTableUniqueName("TestTable");
    tableInfo.setDatabaseName("test");
    storePath = System.getProperty("java.io.tmpdir") + "/tmp";
    tableInfo.setTablePath(storePath + "/" + "test" + "/" + "TestTable");
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);

    // Add the created table to metadata
    metadata.addCarbonTable(carbonTable);

    // Start the server for testing the client
    server = NonSecureDictionaryServer.getInstance(5678, carbonTable);
    host = server.getHost();
  }

  @Test public void testClient() throws Exception {
    NonSecureDictionaryClient client = new NonSecureDictionaryClient();
    client.startClient(null, host, 5678, false);

    Thread.sleep(1000);
    // Create a dictionary key
    DictionaryMessage empKey = new DictionaryMessage();
    empKey.setColumnName(empColumnSchema.getColumnName());
    empKey.setData("FirstKey");

    // Test dictionary initialization call
    int count = 2;
    // Test dictionary generation
    for (; count <= 10000; count++) {
      empKey.setType(DictionaryMessageType.DICT_GENERATION);
      empKey.setTableUniqueId("1");
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
  }

  @Test public void testToCheckIfCorrectTimeOutExceptionMessageIsThrown() {
    new MockUp<LinkedBlockingQueue<DictionaryMessage>>() {
      @SuppressWarnings("unused")
      @Mock
      DictionaryMessage poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
      }
    };
    try {
      testClient();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertFalse(e.getMessage().contains("data"));
    }
  }

  @After public void tearDown() {
    // Cleanup created files
    CarbonMetadata.getInstance().removeTable(tableInfo.getTableUniqueName());
    cleanUpDirectory(new File(storePath));
  }

  private static void cleanUpDirectory(File path) {
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
