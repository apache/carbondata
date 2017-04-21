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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.impl.ComplexFieldConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ComplexFieldConverterImplTest {
  private static ComplexFieldConverterImpl complexFieldConverter;
  private static ColumnSchema columnSchema;
  private static BadRecordLogHolder badRecordLogHolder;

  @BeforeClass public static void setUp() {
    PrimitiveDataType primitiveDataType =
        new PrimitiveDataType("name", "parentName", "columnID", 1);
    complexFieldConverter = new ComplexFieldConverterImpl(primitiveDataType, 1);
    badRecordLogHolder = new BadRecordLogHolder();
  }

  @Test public void testConvert() {
    Object[] data = { "test", "test1" };
    CarbonRow carbonRow = new CarbonRow(data);
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    Object[] expected = { "test", "test1" };
    complexFieldConverter.convert(carbonRow, badRecordLogHolder);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }

  @Test(expected = Exception.class) public void testConvertForException() {
    new MockUp<DataOutputStream>() {
      @Mock public void close() throws IOException {
        throw new IOException();

      }
    };
    Object[] data = { "test", "test1" };
    CarbonRow carbonRow = new CarbonRow(data);
    complexFieldConverter.convert(carbonRow, badRecordLogHolder);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }
}
