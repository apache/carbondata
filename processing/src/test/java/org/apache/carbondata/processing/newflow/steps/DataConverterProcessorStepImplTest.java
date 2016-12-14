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
package org.apache.carbondata.processing.newflow.steps;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.converter.RowConverter;
import org.apache.carbondata.processing.newflow.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.apache.carbondata.processing.constants.LoggerAction.IGNORE;
import static org.apache.carbondata.processing.constants.LoggerAction.REDIRECT;
import static org.junit.Assert.assertTrue;

public class DataConverterProcessorStepImplTest {

  private static CarbonRow row;
  private static RowConverter converter;
  private static CarbonIterator iterator[];
  private static DataField dataField, dataField2;
  private static ColumnSchema columnSchema, columnSchema2;
  private static CarbonDataLoadConfiguration config;
  private static CarbonTableIdentifier tIdentifier;
  private static Iterator<CarbonRowBatch> childIter;
  private static CarbonRowBatch batch1 = new CarbonRowBatch();
  private static AbstractDataLoadProcessorStep dataLoadProcessorStep;
  private static DataConverterProcessorStepImpl dataConverterProcessorStep;
  private static Object inputData[] = { "harsh", "sharma", 26 };

  @BeforeClass public static void setUp() {
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("name");
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setDimensionColumn(true);

    columnSchema2 = new ColumnSchema();
    columnSchema2.setColumnName("name");
    columnSchema2.setDataType(DataType.STRING);
    columnSchema2.setDimensionColumn(true);

    dataField = new DataField(new CarbonColumn(columnSchema, 0, 0));
    dataField2 = new DataField(new CarbonColumn(columnSchema2, 0, 0));

    config = new CarbonDataLoadConfiguration();
    config.setDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE, false);
    tIdentifier = new CarbonTableIdentifier("derby", "test", "TB100");
    config.setTableIdentifier(new AbsoluteTableIdentifier("/tmp", tIdentifier));
    dataLoadProcessorStep = new InputProcessorStepImpl(config, iterator);
    dataConverterProcessorStep = new DataConverterProcessorStepImpl(config, dataLoadProcessorStep);

    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation =
        badLogStoreLocation + File.separator + tIdentifier.getDatabaseName() + File.separator
            + tIdentifier.getTableName() + File.separator + config.getTaskNo();
    boolean badRecordsLoggerEnable = Boolean.parseBoolean(
        config.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE)
            .toString());
    BadRecordsLogger badRecordsLogger = new BadRecordsLogger(tIdentifier.getBadRecordLoggerKey(),
        tIdentifier.getTableName() + '_' + System.currentTimeMillis(), badLogStoreLocation, false,
        badRecordsLoggerEnable, false);

    converter = new RowConverterImpl(dataLoadProcessorStep.getOutput(), config, badRecordsLogger);

    batch1.addRow(new CarbonRow(inputData));
    batch1.addRow(new CarbonRow(inputData));
    childIter = new ArrayList<CarbonRowBatch>(Arrays.asList(batch1)).iterator();
    row = new CarbonRow(inputData);
  }

  @Test public void testGetOutput() {
    new MockUp<InputProcessorStepImpl>() {
      @Mock public DataField[] getOutput() {
        return new DataField[] { dataField, dataField2 };
      }
    };
    assertEquals(dataConverterProcessorStep.getOutput().length, 2);
  }

  public void generateMocksForBadRecords() {
    new MockUp<InputProcessorStepImpl>() {
      @Mock public void initialize() {
      }

      @Mock public DataField[] getOutput() {
        return new DataField[] { dataField, dataField2 };
      }
    };

    new MockUp<RowConverterImpl>() {
      @Mock public RowConverter createCopyForNewThread() {
        return converter;
      }

      @Mock public void initialize() {
      }

      @Mock public CarbonRow convert(CarbonRow row) {
        return row;
      }
    };
  }

  @Test public void testGetIteratorWithDefaultBadRecordsLoggerActionAsFORCE() {
    generateMocksForBadRecords();
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public Object getDataLoadProperty(String key) {
        return false;
      }
    };
    dataConverterProcessorStep.initialize();
    assertNotNull(dataConverterProcessorStep.getIterator(childIter));
  }

  @Test public void testGetIteratorWithBadRecordsLoggerActionAsREDIRECT() {
    generateMocksForBadRecords();
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public Object getDataLoadProperty(String key) {
        return REDIRECT;
      }

    };
    dataConverterProcessorStep.initialize();
    assertNotNull(dataConverterProcessorStep.getIterator(childIter));
  }

  @Test public void testGetIteratorWithBadRecordsLoggerActionAsIGNORE() {
    generateMocksForBadRecords();
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public Object getDataLoadProperty(String key) {
        return IGNORE;
      }

    };
    dataConverterProcessorStep.initialize();
    assertNotNull(dataConverterProcessorStep.getIterator(childIter));
  }

  @Test public void testCarbonHasNextFunctionality() {
    generateMocksForBadRecords();
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public Object getDataLoadProperty(String key) {
        return false;
      }
    };
    dataConverterProcessorStep.initialize();
    assertTrue(dataConverterProcessorStep.getIterator(childIter).hasNext());
  }

  @Test public void testCarbonNextElementFunctionality() {
    generateMocksForBadRecords();
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public Object getDataLoadProperty(String key) {
        return false;
      }
    };

    final List<CarbonRow> rowBatch = new ArrayList<>();
    rowBatch.add(row);
    new MockUp<CarbonRowBatch>() {
      @Mock public Iterator<CarbonRow> getBatchIterator() {
        return rowBatch.iterator();
      }
    };

    dataConverterProcessorStep.initialize();
    assertNotNull(dataConverterProcessorStep.getIterator(childIter));
  }

  @Test public void testProcessRowBatch() {
    final List<CarbonRow> rowBatch = new ArrayList<>();
    rowBatch.add(row);
    new MockUp<CarbonRowBatch>() {
      @Mock public Iterator<CarbonRow> getBatchIterator() {
        return rowBatch.iterator();
      }
    };

    new MockUp<RowConverterImpl>() {
      @Mock public CarbonRow convert(CarbonRow row) {
        return row;
      }
    };
    assertNotNull(dataConverterProcessorStep.processRowBatch(batch1, converter));
  }

  @Test(expected = UnsupportedOperationException.class) public void testProcessRow() {
    assertNotNull(dataConverterProcessorStep.processRow(row));
  }

  @Test public void testClose() {
    new MockUp<InputProcessorStepImpl>() {
      @Mock public void initialize() {
      }

      @Mock public DataField[] getOutput() {
        return new DataField[] { dataField, dataField2 };
      }
    };

    new MockUp<RowConverterImpl>() {
      @Mock public RowConverter createCopyForNewThread() {
        return converter;
      }

      @Mock public void initialize() {
      }
    };

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public Object getDataLoadProperty(String key) {
        return false;
      }
    };
    new MockUp<InputProcessorStepImpl>() {
      @Mock public void close() {
      }
    };
    new MockUp<RowConverterImpl>() {
      @Mock public void finish() {
      }
    };

    dataConverterProcessorStep.initialize();
    dataConverterProcessorStep.close();
  }

}
