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

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RowConverterImplTest {
  private static RowConverterImpl rowConverterImpl;
  private static DataField[] fields;
  private static ColumnSchema columnSchema;
  private static CarbonDataLoadConfiguration carbonDataLoadConfiguration;
  private static BadRecordsLogger badRecordsLogger;
  private static CarbonColumn carbonColumn;

  @BeforeClass public static void setUp() {
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    int ordinal = 1;
    carbonColumn = new CarbonMeasure(columnSchema, ordinal, 1);
    DataField dataField = new DataField(carbonColumn);
    fields = new DataField[] { dataField };
    carbonDataLoadConfiguration = new CarbonDataLoadConfiguration();
    carbonDataLoadConfiguration.setHeader(new String[] { "test" });
    badRecordsLogger = new BadRecordsLogger("key", "fileName", "storePath", true, true, true);
    rowConverterImpl = new RowConverterImpl(fields, carbonDataLoadConfiguration, badRecordsLogger);

  }

  @Test public void testConvert() {
    new MockUp<CarbonMeasure>() {
      @Mock public int hashCode() {
        return 1;
      }
    };
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };

    new MockUp<CarbonMeasure>() {
      @Mock public DataType getDataType() {
        return DataType.TIMESTAMP;
      }
    };

    CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier("databaseName", "tableName", "tableId");
    final AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("storePath", carbonTableIdentifier);

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public AbsoluteTableIdentifier getTableIdentifier() {
        return absoluteTableIdentifier;
      }
    };

    Object[] data = { "1" };
    CarbonRow carbonRow = new CarbonRow(data);
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    int ordinal = 1;
    carbonColumn = new CarbonMeasure(columnSchema, ordinal, 1);
    carbonDataLoadConfiguration = new CarbonDataLoadConfiguration();
    carbonDataLoadConfiguration.setDataLoadProperty("SERIALIZATION_NULL_FORMAT", 1);
    rowConverterImpl.initialize();
    rowConverterImpl.convert(carbonRow);
    Object[] actualValue = carbonRow.getData();
    Object[] expectedValue = data;
    assertThat(actualValue, is(expectedValue));
  }

  @Test public void testCreateCopyForNewThread() {
    assertNotNull(rowConverterImpl.createCopyForNewThread());
  }
}

