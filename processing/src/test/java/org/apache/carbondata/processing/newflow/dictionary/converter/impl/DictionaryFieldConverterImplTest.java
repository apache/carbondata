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

import java.util.List;

import org.apache.carbondata.core.cache.dictionary.ColumnReverseDictionaryInfo;
import org.apache.carbondata.core.cache.dictionary.ReverseDictionary;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.impl.DictionaryFieldConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DictionaryFieldConverterImplTest {
  private static DictionaryFieldConverterImpl dictionaryFieldConverter;
  private static DataField dataField;
  private static CarbonTableIdentifier carbonTableIdentifier;
  private static org.apache.carbondata.core.cache.Cache cache;
  private static ColumnSchema columnSchema;
  private static BadRecordLogHolder badRecordLogHolder;
  private static ColumnReverseDictionaryInfo columnReverseDictionaryInfo;
  private static ReverseDictionary reverseDictionary;

  @BeforeClass public static void setUp() {
    int ordinal = 1;
    int keyOrdinal = 1;
    int columnGroupOrdinal = 1;
    int complexTypeOrdinal = 1;
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    CarbonColumn carbonColumn =
        new CarbonDimension(columnSchema, ordinal, keyOrdinal, columnGroupOrdinal,
            complexTypeOrdinal);
    dataField = new DataField(carbonColumn);
    carbonTableIdentifier = new CarbonTableIdentifier("databaseName", "tableName", "tableId");
    columnReverseDictionaryInfo = new ColumnReverseDictionaryInfo();
    reverseDictionary = new ReverseDictionary(columnReverseDictionaryInfo);

    badRecordLogHolder = new BadRecordLogHolder();
    badRecordLogHolder.setReason("reason");
  }

  @Test public void testConvert() {
    cache = new org.apache.carbondata.core.cache.Cache() {
      @Override public Object get(Object key) throws CarbonUtilException {
        return reverseDictionary;
      }

      @Override public List getAll(List keys) throws CarbonUtilException {
        return null;
      }

      @Override public Object getIfPresent(Object key) {
        return null;
      }

      @Override public void invalidate(Object key) {

      }
    };

    new MockUp<CarbonRow>() {
      @Mock public void update(Object value, int ordinal) throws DictionaryGenerationException {
      }
    };

    Object[] data = { "test", "test1" };
    CarbonRow carbonRow = new CarbonRow(data);
    dictionaryFieldConverter =
        new DictionaryFieldConverterImpl(dataField, cache, carbonTableIdentifier, "nullFormat", 1);
    dictionaryFieldConverter.convert(carbonRow, badRecordLogHolder);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }

  @Test(expected = Exception.class) public void testConvertWithException() {
    Object[] data = { "test", "test1" };
    CarbonRow carbonRow = new CarbonRow(data);
    cache = new org.apache.carbondata.core.cache.Cache() {
      @Override public Object get(Object key) throws CarbonUtilException {
        return "test";
      }

      @Override public List getAll(List keys) throws CarbonUtilException {
        return null;
      }

      @Override public Object getIfPresent(Object key) {
        return null;
      }

      @Override public void invalidate(Object key) {

      }
    };

    new MockUp<CarbonRow>() {
      @Mock public void update(Object value, int ordinal) throws DictionaryGenerationException {
        String msg = "DictionaryGenerationExceptionmessage";
        throw new DictionaryGenerationException(msg);
      }
    };

    dictionaryFieldConverter =
        new DictionaryFieldConverterImpl(dataField, cache, carbonTableIdentifier, "nullFormat", 1);
    dictionaryFieldConverter.convert(carbonRow, badRecordLogHolder);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }
}
