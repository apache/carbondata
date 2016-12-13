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
package org.apache.carbondata.processing.newflow.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class CarbonParserFactoryTest {

  private static ColumnSchema colSchema;
  private static String[] complexDelimiters = { "!", "}", "{" };
  private static CarbonColumn carbonColumn;
  private static CarbonDimension carbonDimension;

  @BeforeClass public static void setUp() {
    carbonColumn = null;
    carbonDimension = null;
    colSchema = null;
  }

  @AfterClass public static void tearDown() {
    carbonColumn = null;
    carbonDimension = null;
    colSchema = null;
  }

  @Test public void testCreateParserForArrayDataType() {
    colSchema = getColumnSchema(DataType.ARRAY);
    carbonColumn = new CarbonDimension(colSchema, 1, 1, 1, 1);
    carbonDimension = (CarbonDimension) carbonColumn;
    carbonDimension.initializeChildDimensionsList(3);
    GenericParser parser = CarbonParserFactory.createParser(carbonDimension, complexDelimiters, "");
    assertNotNull(parser);
  }

  @Test public void testCreateParserForStructDataType() {
    colSchema = getColumnSchema(DataType.STRUCT);
    carbonColumn = new CarbonDimension(colSchema, 3, 2, 1, 1);
    carbonDimension = (CarbonDimension) carbonColumn;
    carbonDimension.initializeChildDimensionsList(5);
    GenericParser parser = CarbonParserFactory.createParser(carbonDimension, complexDelimiters, "");
    assertNotNull(parser);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateParserForMapDataType() {
    colSchema = getColumnSchema(DataType.MAP);
    carbonColumn = new CarbonDimension(colSchema, 3, 2, 1, 1);
    carbonDimension = (CarbonDimension) carbonColumn;
    carbonDimension.initializeChildDimensionsList(5);
    CarbonParserFactory.createParser(carbonDimension, complexDelimiters, "");
  }

  @Test public void testCreateParserForOtherDataTypes() {
    colSchema = getColumnSchema(DataType.INT);
    carbonColumn = new CarbonDimension(colSchema, 3, 2, 1, 1);
    carbonDimension = (CarbonDimension) carbonColumn;
    carbonDimension.initializeChildDimensionsList(5);
    GenericParser parser = CarbonParserFactory.createParser(carbonDimension, complexDelimiters, "");
    assertNotNull(parser);
  }

  private ColumnSchema getColumnSchema(DataType dataType) {
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setDimensionColumn(true);
    columnSchema.setColumnar(true);
    columnSchema.setColumnName("Test" + UUID.randomUUID().toString());
    columnSchema.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema.setDataType(dataType);
    columnSchema.setColumnGroup(2);
    columnSchema.setPrecision(1);
    columnSchema.setScale(1);
    columnSchema.setUseInvertedIndex(true);
    columnSchema.setSchemaOrdinal(2);
    columnSchema.setDimensionColumn(true);
    List<Encoding> list = new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    list.add(Encoding.DICTIONARY);
    columnSchema.setEncodingList(list);
    columnSchema.setNumberOfChild(4);
    return columnSchema;
  }

}
