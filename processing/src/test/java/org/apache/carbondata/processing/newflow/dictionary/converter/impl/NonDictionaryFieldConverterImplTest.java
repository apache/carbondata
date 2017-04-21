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
package org.apache.carbondata.processing.newflow.dictionary.converter.impl;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.impl.NonDictionaryFieldConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class NonDictionaryFieldConverterImplTest {
  private static NonDictionaryFieldConverterImpl nonDictionaryFieldConverterImpl;
  private static DataField dataField;
  private static BadRecordLogHolder badRecordLogHolder;
  private static ColumnSchema columnSchema;

  @BeforeClass public static void setUp() {
    int ordinal = 1;
    int schemaOrdinal = 1;
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    CarbonColumn carbonColumn = new CarbonColumn(columnSchema, ordinal, schemaOrdinal);
    dataField = new DataField(carbonColumn);
    nonDictionaryFieldConverterImpl =
        new NonDictionaryFieldConverterImpl(dataField, "nullFormat", 1);
    badRecordLogHolder = new BadRecordLogHolder();
    badRecordLogHolder.setReason("reason");
  }

  @Test public void testConvertWithDimensionNull() {
    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.INT;
      }
    };
    Object[] data = { null, null };
    CarbonRow carbonRow = new CarbonRow(data);
    nonDictionaryFieldConverterImpl =
        new NonDictionaryFieldConverterImpl(dataField, "nullFormat", 1);
    nonDictionaryFieldConverterImpl.convert(carbonRow, badRecordLogHolder);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }

  @Test public void testConvertWithDimensionNotNull() {
    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.INT;
      }
    };
    Object[] data = { "test", "test1" };
    CarbonRow carbonRow = new CarbonRow(data);
    nonDictionaryFieldConverterImpl =
        new NonDictionaryFieldConverterImpl(dataField, "nullFormat", 1);
    nonDictionaryFieldConverterImpl.convert(carbonRow, badRecordLogHolder);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }

}
