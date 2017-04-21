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

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.impl.FieldEncoderFactory;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class FieldEncoderFactoryTest {
  private static FieldEncoderFactory fieldEncoderFactory;
  private static DataField dataField;
  private static CarbonTableIdentifier carbonTableIdentifier;
  private static org.apache.carbondata.core.cache.Cache cache;
  private static ColumnSchema columnSchema;

  @BeforeClass public static void setUp() {
    fieldEncoderFactory = FieldEncoderFactory.getInstance();
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    int ordinal = 1;
    dataField = new DataField(new CarbonMeasure(columnSchema, ordinal));
    carbonTableIdentifier = new CarbonTableIdentifier("databaseName", "tableName", "tableId");
    org.apache.carbondata.core.cache.Cache cache = new org.apache.carbondata.core.cache.Cache() {
      @Override public Object get(Object key) throws CarbonUtilException {
        return null;
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
  }

  @Test public void testCreateFieldEncoderWithGetColumnAndIsDimension() {
    final CarbonMeasure carbonColumn = new CarbonMeasure(columnSchema, 1, 1);
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

    new MockUp<CarbonColumn>() {
      @Mock public boolean hasEncoding(Encoding encoding) {
        return true;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.TIMESTAMP;
      }
    };

    assertNotNull(fieldEncoderFactory
        .createFieldEncoder(dataField, cache, carbonTableIdentifier, 1, "nullFormat"));
  }

  @Test
  public void testCreateFieldEncoderWithGetColumnAndIsDimensionWithoutNoHasEncodingAndIsComplex() {
    final CarbonMeasure carbonColumn = new CarbonMeasure(columnSchema, 1, 1);
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

    new MockUp<CarbonColumn>() {
      @Mock public boolean hasEncoding(Encoding encoding) {
        return false;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.TIMESTAMP;
      }
    };

    assertNotNull(fieldEncoderFactory
        .createFieldEncoder(dataField, cache, carbonTableIdentifier, 1, "nullFormat"));
  }

  @Test public void testCreateFieldEncoder() {
    new MockUp<CarbonMeasure>() {
      @Mock public int hashCode() {
        return 1;
      }
    };
    assertNotNull(fieldEncoderFactory
        .createFieldEncoder(dataField, cache, carbonTableIdentifier, 1, "nullFormat"));
  }

  @Test
  public void testCreateFieldEncoderWithGetColumnAndIsDimensionWithoutNoHasEncodingAndWithIsComplex() {
    final CarbonMeasure carbonColumn = new CarbonMeasure(columnSchema, 1, 1);
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

    new MockUp<CarbonColumn>() {
      @Mock public boolean hasEncoding(Encoding encoding) {
        return true;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.TIMESTAMP;
      }
    };

    new MockUp<CarbonMeasure>() {
      @Mock public Boolean isComplex() {
        return false;
      }
    };
    assertNotNull(fieldEncoderFactory
        .createFieldEncoder(dataField, cache, carbonTableIdentifier, 1, "nullFormat"));
  }

}
