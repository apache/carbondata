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
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class to test table column dictionary generator functionality
 */
public class TableDictionaryGeneratorTest {

  private ColumnSchema empColumnSchema;
  private CarbonDimension empDimension;
  private ColumnSchema ageColumnSchema;
  private CarbonDimension ageDimension;
  private TableSchema tableSchema;
  private TableInfo tableInfo;
  private String storePath;

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
    ageColumnSchema.setColumnName("ageNameCol");
    ageColumnSchema.setColumnUniqueId("ageNameCol");
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
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);

    // Add the created table to metadata
    metadata.addCarbonTable(carbonTable);
  }

  @Test public void generateKeyOnce() throws Exception {
    TableDictionaryGenerator tableDictionaryGenerator = new TableDictionaryGenerator(empDimension);

    // Generate dictionary for one key
    DictionaryMessage empKey = new DictionaryMessage();
    empKey.setTableUniqueName(tableInfo.getTableUniqueName());
    empKey.setColumnName(empColumnSchema.getColumnName());
    empKey.setData("FirstKey");
    Integer value = tableDictionaryGenerator.generateKey(empKey);
    assertEquals(new Integer(2), value);

  }

  @Test public void generateKeyTwice() throws Exception {
    TableDictionaryGenerator tableDictionaryGenerator = new TableDictionaryGenerator(empDimension);

    // Generate dictionary for same key twice
    DictionaryMessage firstKey = new DictionaryMessage();
    firstKey.setTableUniqueName(tableInfo.getTableUniqueName());
    firstKey.setColumnName(empColumnSchema.getColumnName());
    firstKey.setData("FirstKey");
    Integer value = tableDictionaryGenerator.generateKey(firstKey);
    assertEquals(new Integer(2), value);
    DictionaryMessage secondKey = new DictionaryMessage();
    secondKey.setTableUniqueName(tableInfo.getTableUniqueName());
    secondKey.setColumnName(empColumnSchema.getColumnName());
    secondKey.setData("FirstKey");
    value = tableDictionaryGenerator.generateKey(secondKey);
    assertEquals(new Integer(2), value);
  }

  @Test public void generateKeyAgain() throws Exception {
    TableDictionaryGenerator tableDictionaryGenerator = new TableDictionaryGenerator(empDimension);

    // Generate dictionary for two different keys
    DictionaryMessage firstKey = new DictionaryMessage();
    firstKey.setTableUniqueName(tableInfo.getTableUniqueName());
    firstKey.setColumnName(empColumnSchema.getColumnName());
    firstKey.setData("FirstKey");
    Integer value = tableDictionaryGenerator.generateKey(firstKey);
    assertEquals(new Integer(2), value);
    DictionaryMessage secondKey = new DictionaryMessage();
    secondKey.setTableUniqueName(tableInfo.getTableUniqueName());
    secondKey.setColumnName(empColumnSchema.getColumnName());
    secondKey.setData("SecondKey");
    value = tableDictionaryGenerator.generateKey(secondKey);
    assertEquals(new Integer(3), value);
  }

  @Test public void updateGenerator() throws Exception {
    TableDictionaryGenerator tableDictionaryGenerator = new TableDictionaryGenerator(empDimension);
    DictionaryMessage firstKey = new DictionaryMessage();
    firstKey.setTableUniqueName(tableInfo.getTableUniqueName());
    firstKey.setColumnName(empColumnSchema.getColumnName());
    firstKey.setData("FirstKey");
    Integer value = tableDictionaryGenerator.generateKey(firstKey);
    assertEquals(new Integer(2), value);

    tableDictionaryGenerator.updateGenerator(ageDimension);
    //Update generator with a new dimension

    DictionaryMessage secondKey = new DictionaryMessage();
    secondKey.setTableUniqueName(tableInfo.getTableUniqueName());
    secondKey.setColumnName(ageColumnSchema.getColumnName());
    secondKey.setData("SecondKey");
    value = tableDictionaryGenerator.generateKey(secondKey);
    assertEquals(new Integer(2), value);
  }

  @Test public void size() throws Exception {
    TableDictionaryGenerator tableDictionaryGenerator = new TableDictionaryGenerator(empDimension);
    //Add keys for first Column
    DictionaryMessage empKey = new DictionaryMessage();
    //Add key 1
    empKey.setTableUniqueName(tableInfo.getTableUniqueName());
    empKey.setColumnName(empColumnSchema.getColumnName());
    empKey.setData("FirstKey");
    tableDictionaryGenerator.generateKey(empKey);
    assertEquals(new Integer(2), tableDictionaryGenerator.size(empKey));

    //Add key 2
    empKey = new DictionaryMessage();
    empKey.setTableUniqueName(tableInfo.getTableUniqueName());
    empKey.setColumnName(empColumnSchema.getColumnName());
    empKey.setData("SecondKey");
    tableDictionaryGenerator.generateKey(empKey);
    assertEquals(new Integer(3), tableDictionaryGenerator.size(empKey));

    //Add key 3
    empKey = new DictionaryMessage();
    empKey.setTableUniqueName(tableInfo.getTableUniqueName());
    empKey.setColumnName(empColumnSchema.getColumnName());
    empKey.setData("ThirdKey");
    tableDictionaryGenerator.generateKey(empKey);
    assertEquals(new Integer(4), tableDictionaryGenerator.size(empKey));
  }

  @Test public void writeDictionaryData() throws Exception {
    TableDictionaryGenerator tableDictionaryGenerator = new TableDictionaryGenerator(empDimension);
    DictionaryMessage firstKey = new DictionaryMessage();
    firstKey.setTableUniqueName(tableInfo.getTableUniqueName());
    firstKey.setColumnName(empColumnSchema.getColumnName());
    firstKey.setData("FirstKey");
    Integer value = tableDictionaryGenerator.generateKey(firstKey);

    tableDictionaryGenerator.updateGenerator(ageDimension);
    //Update generator with a new dimension

    DictionaryMessage secondKey = new DictionaryMessage();
    secondKey.setTableUniqueName(tableInfo.getTableUniqueName());
    secondKey.setColumnName(ageColumnSchema.getColumnName());
    secondKey.setData("SecondKey");
    tableDictionaryGenerator.generateKey(secondKey);
    File dictPath = new File(storePath + "/test/TestTable/Metadata/");
    System.out.print(dictPath.mkdirs());
    tableDictionaryGenerator.writeDictionaryData(tableInfo.getTableUniqueName());

    File empDictionaryFile = new File(dictPath, empColumnSchema.getColumnName() + ".dict");
    assertTrue(empDictionaryFile.exists());

    File ageDictionaryFile = new File(dictPath, ageColumnSchema.getColumnName() + ".dict");
    assertTrue(ageDictionaryFile.exists());
  }

  @After public void tearDown() {
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