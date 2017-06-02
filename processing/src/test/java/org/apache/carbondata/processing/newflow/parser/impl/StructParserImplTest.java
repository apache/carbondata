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
package org.apache.carbondata.processing.newflow.parser.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.newflow.complexobjects.StructObject;
import org.apache.carbondata.processing.newflow.parser.CarbonParserFactory;
import org.apache.carbondata.processing.newflow.parser.GenericParser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class StructParserImplTest {

  private static StructParserImpl structParser;
  private static StructParserImpl structParserTwo;
  private static ColumnSchema columnSchema;

  @BeforeClass public static void setUp() {
    structParser = new StructParserImpl("!", ";");
    structParserTwo = new StructParserImpl("?", ":");
    columnSchema = getColumnSchema(DataType.STRUCT);
  }

  @AfterClass public static void tearDown() {
    structParser = null;
  }

  @Test public void testStructParserForValidData() {
    String[] data = { "0!Test!Struct!Parser!For!Valid!Data!Type;",
        "1}Test}Struct}Parser}For}Valid}Data}Type;" };
    String[] complexDelimiters = { "!", "}", "{" };
    CarbonColumn carbonColumn = new CarbonDimension(columnSchema, 1, 1, 1, 1);
    CarbonDimension carbonDimension = (CarbonDimension) carbonColumn;
    carbonDimension.initializeChildDimensionsList(3);
    GenericParser gp = CarbonParserFactory.createParser(carbonDimension, complexDelimiters, ";");
    structParser.addChildren(structParserTwo);
    structParser.addChildren(structParserTwo);
    StructObject structResult = structParser.parse(gp);
    int expected = 2;
    int actual = structResult.getData().length;
    assertEquals(expected, actual);
  }

  @Test public void testStructParserForNull() {
    StructObject structResult = structParser.parse(null);
    assertNull(structResult);
  }

  private static ColumnSchema getColumnSchema(DataType dataType) {
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
