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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;

import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class RowParserImplTest {

  private static RowParserImpl rowParser = null;
  private static DataField[] dataFieldList = null;
  private static CarbonDataLoadConfiguration configuration = null;
  private static String[] header = { "Test", "Test", "Test" };
  private static String[] complexDelimiters = { "!", ";", "$" };

  @BeforeClass public static void setUp() {
    ColumnSchema var = getColumnSchema(DataType.ARRAY);
    CarbonDimension carbonDimOne = new CarbonDimension(var, 1, 1, 1, 1);
    CarbonColumn carbonCol = carbonDimOne;
    CarbonColumn carbonDimTwo = new CarbonDimension(var, 1, 1, 1, 1);
    DataField dataFieldOne = new DataField((CarbonDimension) carbonCol);
    DataField dataFieldTwo = new DataField((CarbonDimension) carbonDimTwo);
    dataFieldList = new DataField[] { dataFieldOne, dataFieldTwo, dataFieldOne };
    configuration = new CarbonDataLoadConfiguration();
    configuration.setDataFields(dataFieldList);
    configuration
        .setDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS, complexDelimiters);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT, "");
    configuration.setHeader(header);
  }

  @AfterClass public static void tearDown() {
    rowParser = null;
  }

  @Test public void testParseRow() {
    new MockUp<CarbonDimension>() {
      @Mock public List<CarbonDimension> getListOfChildDimensions() {
        ColumnSchema var = getColumnSchema(DataType.ARRAY);
        CarbonDimension temp = new CarbonDimension(var, 1, 1, 1, 1);
        List<CarbonDimension> listOfCD = new ArrayList<CarbonDimension>(2);
        return listOfCD;
      }
    };
    rowParser = new RowParserImpl(dataFieldList, configuration);
    String[] row = new String[] { "", "", "" };
    Object[] value = rowParser.parseRow(row);
    int expected = 3;
    int actual = value.length;
    assertEquals(expected, actual);
  }

  @Test public void testGetInput() {
    String expected = "Test";
    new MockUp<CarbonDimension>() {
      @Mock public List<CarbonDimension> getListOfChildDimensions() {
        ColumnSchema var = getColumnSchema(DataType.ARRAY);
        CarbonDimension temp = new CarbonDimension(var, 1, 1, 1, 1);
        List<CarbonDimension> listOfCD = new ArrayList<CarbonDimension>(2);
        return listOfCD;
      }
    };
    rowParser = new RowParserImpl(dataFieldList, configuration);
    DataField[] dataField = rowParser.getInput(configuration);
    List<DataField> listOfDataField = Arrays.asList(dataField);

    for (DataField data : listOfDataField) {
      String actual = data.getColumn().getColName();
      assertEquals(expected, actual);
    }
  }

  private static ColumnSchema getColumnSchema(DataType dataType) {
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setDimensionColumn(true);
    columnSchema.setColumnar(true);
    columnSchema.setColumnName("Test");
    columnSchema.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema.setDataType(dataType);
    columnSchema.setColumnGroup(2);
    columnSchema.setPrecision(1);
    columnSchema.setScale(1);
    columnSchema.setUseInvertedIndex(true);
    columnSchema.setSchemaOrdinal(2);
    columnSchema.setDimensionColumn(true);
    columnSchema.setNumberOfChild(2);
    List<Encoding> list = new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    list.add(Encoding.DICTIONARY);
    columnSchema.setEncodingList(list);
    columnSchema.setNumberOfChild(4);
    return columnSchema;
  }
}