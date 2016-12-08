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
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.impl.DirectDictionaryFieldConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DirectDictionaryFieldConverterImplTest {
  private static DirectDictionaryFieldConverterImpl directDictionaryFieldConverter;
  private static DataField dataField;
  private static ColumnSchema columnSchema;
  private static BadRecordLogHolder badRecordLogHolder;

  @BeforeClass public static void setUp() {
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    int ordinal = 1;
    CarbonColumn carbonColumn = new CarbonMeasure(columnSchema, ordinal, 1);
    dataField = new DataField(carbonColumn);
    badRecordLogHolder = new BadRecordLogHolder();
    badRecordLogHolder.setReason("reason");

  }

  @Test public void testConvertWithValueEqualToNull() {
    new MockUp<CarbonMeasure>() {
      @Mock public int hashCode() {
        return 1;
      }
    };

    new MockUp<DataField>() {
      @Mock public String getDateFormat() {
        return "dateFormat";
      }

    };
    new MockUp<CarbonMeasure>() {
      @Mock public DataType getDataType() {
        return DataType.INT;
      }
    };
    Object[] data = { null, null };
    CarbonRow carbonRow = new CarbonRow(data);
    directDictionaryFieldConverter =
        new DirectDictionaryFieldConverterImpl(dataField, "nullFormat", 1);
    directDictionaryFieldConverter.convert(carbonRow, badRecordLogHolder);
    assertThat(carbonRow.getData(), is(data));

  }

  @Test public void testConvertWithValueNotEqualToNull() {
    new MockUp<CarbonMeasure>() {
      @Mock public int hashCode() {
        return 1;
      }
    };

    new MockUp<DataField>() {
      @Mock public String getDateFormat() {
        return "dateFormat";
      }

    };
    new MockUp<CarbonMeasure>() {
      @Mock public DataType getDataType() {
        return DataType.INT;
      }
    };
    Object[] data = { "nullFormat", "nullFormat" };
    CarbonRow carbonRow = new CarbonRow(data);
    directDictionaryFieldConverter =
        new DirectDictionaryFieldConverterImpl(dataField, "nullFormat", 1);
    directDictionaryFieldConverter.convert(carbonRow, badRecordLogHolder);
    assertThat(carbonRow.getData(), is(data));

  }

  @Test public void testConvertWithValueNotEqualToNullAndValueNotEqualToNullFormat() {
    new MockUp<CarbonMeasure>() {
      @Mock public int hashCode() {
        return 1;
      }
    };

    new MockUp<DataField>() {
      @Mock public String getDateFormat() {
        return "yyyy-mm-dd hh:mm:ss";
      }

    };
    new MockUp<CarbonMeasure>() {
      @Mock public DataType getDataType() {
        return DataType.TIMESTAMP;
      }
    };

    new MockUp<DirectDictionaryGenerator>() {
      @Mock int generateDirectSurrogateKey(String member) {
        return 1;
      }
    };

    Object[] data = { "1970-01-01 00:00:01", "1970-01-01 00:00:01" };
    CarbonRow carbonRow = new CarbonRow(data);
    directDictionaryFieldConverter =
        new DirectDictionaryFieldConverterImpl(dataField, "nullFormat", 1);
    directDictionaryFieldConverter.convert(carbonRow, badRecordLogHolder);
    assertThat(carbonRow.getData(), is(data));
  }

}
