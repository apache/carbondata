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

package org.apache.carbondata.processing.newflow;

import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CarbonDataLoadConfigurationTest {
  private static CarbonDataLoadConfiguration carbonDataLoadConfiguration;

  @BeforeClass public static void setUp() {
    carbonDataLoadConfiguration = new CarbonDataLoadConfiguration();
  }

  @Test public void testGetDimensionCount() {
    int ordinal = 1, schemaOrdinal = 1;
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    final CarbonColumn carbonColumn = new CarbonColumn(columnSchema, ordinal, schemaOrdinal);
    DataField dataField = new DataField(carbonColumn);
    DataField[] dataFields = new DataField[] { dataField };
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public Boolean isDimesion() {
        return true;
      }
    };
    carbonDataLoadConfiguration.setDataFields(dataFields);
    int actualValue = carbonDataLoadConfiguration.getDimensionCount();
    int expectedValue = 1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetNoDictionaryCount() {
    int ordinal = 1, schemaOrdinal = 1;
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    final CarbonColumn carbonColumn = new CarbonColumn(columnSchema, ordinal, schemaOrdinal);
    DataField dataField = new DataField(carbonColumn);
    DataField[] dataFields = new DataField[] { dataField };
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };
    new MockUp<DataField>() {
      @Mock public boolean hasDictionaryEncoding() {
        return false;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isDimesion() {
        return true;
      }
    };
    carbonDataLoadConfiguration.setDataFields(dataFields);
    int actualValue = carbonDataLoadConfiguration.getNoDictionaryCount();
    int expectedValue = 1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetComplexDimensionCount() {
    int ordinal = 1, schemaOrdinal = 1;
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    final CarbonColumn carbonColumn = new CarbonColumn(columnSchema, ordinal, schemaOrdinal);
    DataField dataField = new DataField(carbonColumn);
    DataField[] dataFields = new DataField[] { dataField };
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isComplex() {
        return true;
      }
    };
    carbonDataLoadConfiguration.setDataFields(dataFields);
    int actualValue = carbonDataLoadConfiguration.getComplexDimensionCount();
    int expectedValue = 1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetMeasureCount() {
    int ordinal = 1, schemaOrdinal = 1;
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    final CarbonColumn carbonColumn = new CarbonColumn(columnSchema, ordinal, schemaOrdinal);
    DataField dataField = new DataField(carbonColumn);
    DataField[] dataFields = new DataField[] { dataField };
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isDimesion() {
        return false;
      }
    };
    carbonDataLoadConfiguration.setDataFields(dataFields);
    int actualValue = carbonDataLoadConfiguration.getMeasureCount();
    int expectedValue = 1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetDataLoadProperty() {
    String key = "keyValue";
    Object defaultValue = "defaultValue";
    carbonDataLoadConfiguration.getDataLoadProperty(key, defaultValue);
  }

}