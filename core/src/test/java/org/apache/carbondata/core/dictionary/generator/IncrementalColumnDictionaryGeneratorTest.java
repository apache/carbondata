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

package org.apache.carbondata.core.dictionary.generator;

import java.io.File;
import java.util.Arrays;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class to test incremental column dictionary generator functionality
 */
public class IncrementalColumnDictionaryGeneratorTest {

  private CarbonTable carbonTable;
  private CarbonDimension carbonDimension;

  @Before public void setUp() throws Exception {
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("empName");
    columnSchema.setDataType(DataTypes.STRING);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName("TestTable");
    tableSchema.setListOfColumns(Arrays.asList(columnSchema));
    TableInfo tableInfo = new TableInfo();
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTableUniqueName("TestTable");
    tableInfo.setDatabaseName("test");
    String storePath = System.getProperty("java.io.tmpdir") + "/tmp";
    tableInfo.setTablePath(storePath + "/test" + "/TestTable");
    carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
    carbonDimension = new CarbonDimension(columnSchema,0,0,0,0);
  }

  @Test public void generateKeyOnce() throws Exception {
    // Create the generator and add the key to dictionary
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);
    Integer key = generator.generateKey("First");
    assertEquals(new Integer(11), key);
  }

  @Test public void generateKeyTwice() throws Exception {
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);
    Integer key = generator.generateKey("First");

    // Add one more key and check if it works fine.
    key = generator.generateKey("Second");
    assertEquals(new Integer(12), key);
  }

  @Test public void generateKeyAgain() throws Exception {
    // Create the generator and add the key to dictionary
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);
    Integer key = generator.generateKey("First");

    // Add the same key again anc check if the value is correct
    key = generator.generateKey("First");
    assertEquals(new Integer(11), key);
  }

  @Test public void getKey() throws Exception {
    // Create the generator and add the key to dictionary
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);
    Integer generatedKey = generator.generateKey("First");

    // Get the value of the key from dictionary and check if it matches with the created value
    Integer obtainedKey = generator.getKey("First");
    assertEquals(generatedKey, obtainedKey);
  }

  @Test public void getKeyInvalid() throws Exception {
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);

    // Try to get value for an invalid key
    Integer obtainedKey = generator.getKey("Second");
    assertNull(obtainedKey);
  }

  @Test public void getOrGenerateKey() throws Exception {
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);

    // Test first with generating a key and then trying geOrGenerate
    Integer generatedKey = generator.generateKey("First");
    Integer obtainedKey = generator.getOrGenerateKey("First");
    assertEquals(generatedKey, obtainedKey);

    // Test directly with getOrGenerate for another key
    obtainedKey = generator.getOrGenerateKey("Second");
    assertEquals(new Integer(12), obtainedKey);

  }

  @Test public void writeDictionaryData() throws Exception {
    //Create required column schema
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("empNameCol");
    columnSchema.setDataType(DataTypes.STRING);
    columnSchema.setColumnUniqueId("empNameCol");
    CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 0, 0, 0, 0);

    // Create the generator and add the keys to dictionary
    IncrementalColumnDictionaryGenerator generator =
        new IncrementalColumnDictionaryGenerator(carbonDimension, 10, carbonTable);

    // Create a table schema for saving the dictionary
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName("TestTable");
    tableSchema.setListOfColumns(Arrays.asList(columnSchema));
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    TableInfo tableInfo = new TableInfo();
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTableUniqueName("TestTable");
    tableInfo.setDatabaseName("test");

    String storePath = System.getProperty("java.io.tmpdir") + "/tmp";
    File dictPath = new File(storePath + "/test/TestTable/Metadata/");
    System.out.print(dictPath.mkdirs());

    tableInfo.setTablePath(storePath + "/test" + "/TestTable");
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);

    // Add the table to metadata
    metadata.addCarbonTable(carbonTable);

    /// Write the dictionary and verify whether its written successfully
    generator.writeDictionaryData();
    File dictionaryFile = new File(dictPath, "empNameCol.dict");
    System.out.println(dictionaryFile.getCanonicalPath());
    assertTrue(dictionaryFile.exists());
    dictionaryFile.delete();

    // cleanup created files
    metadata.removeTable(carbonTable.getTableUniqueName());
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