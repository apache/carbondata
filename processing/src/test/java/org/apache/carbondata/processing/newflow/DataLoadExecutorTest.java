/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.newflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.exception.BadRecordFoundException;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.steps.DataConverterProcessorStepImpl;
import org.apache.carbondata.processing.newflow.steps.DataWriterProcessorStepImpl;
import org.apache.carbondata.processing.newflow.steps.InputProcessorStepImpl;
import org.apache.carbondata.processing.newflow.steps.SortProcessorStepImpl;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataLoadExecutorTest {
  private static DataLoadExecutor dataLoadExecutor;
  private static CarbonLoadModel loadModel;
  private static CarbonDataLoadConfiguration config;
  private static CarbonIterator iterator[];
  private static AbstractDataLoadProcessorStep inputProcessorStep, converterProcessorStep,
      sortProcessorStep, writerProcessorStep;
  private static ColumnSchema columnSchema = new ColumnSchema();
  private static ColumnSchema columnSchema2 = new ColumnSchema();
  private static DataField dataField, dataField2;
  private static CarbonTableIdentifier tIdentifier;
  private static Iterator<CarbonRowBatch>[] iterators = new Iterator[2];
  private static CarbonRowBatch batch1 = new CarbonRowBatch();
  private static Object inputData[] = { "harsh", "sharma", 26 };

  @BeforeClass public static void setUp() {
    loadModel = new CarbonLoadModel();
    dataLoadExecutor = new DataLoadExecutor();
    config = new CarbonDataLoadConfiguration();
    columnSchema.setColumnName("name");
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setDimensionColumn(true);

    columnSchema2.setColumnName("name");
    columnSchema2.setDataType(DataType.STRING);
    columnSchema2.setDimensionColumn(true);
    dataField = new DataField(new CarbonColumn(columnSchema, 0, 0));
    dataField2 = new DataField(new CarbonColumn(columnSchema2, 0, 0));
    config.setDataFields(new DataField[] { dataField, dataField2 });
    tIdentifier = new CarbonTableIdentifier("derby", "test", "TB100");
    config.setTableIdentifier(new AbsoluteTableIdentifier("/tmp", tIdentifier));
    inputProcessorStep = new InputProcessorStepImpl(config, iterator);
    converterProcessorStep = new DataConverterProcessorStepImpl(config, inputProcessorStep);
    sortProcessorStep = new SortProcessorStepImpl(config, converterProcessorStep);
    writerProcessorStep = new DataWriterProcessorStepImpl(config, sortProcessorStep);
    batch1.addRow(new CarbonRow(inputData));
    batch1.addRow(new CarbonRow(inputData));
    iterators[0] = new ArrayList<CarbonRowBatch>(Arrays.asList(batch1)).iterator();
  }

  @Test public void testExecute() throws Exception {
    new MockUp<DataLoadProcessBuilder>() {
      @Mock
      public AbstractDataLoadProcessorStep build(CarbonLoadModel loadModel, String storeLocation,
          CarbonIterator[] inputIterators) throws Exception {
        return writerProcessorStep;
      }
    };

    new MockUp<DataWriterProcessorStepImpl>() {
      @Mock public void initialize() {
      }

      @Mock public Iterator<CarbonRowBatch>[] execute() {
        return iterators;
      }

      @Mock public void close() {
      }
    };

    dataLoadExecutor.execute(loadModel, "/tmp", iterator);
  }

  @Test(expected = BadRecordFoundException.class) public void testExecuteBadRecords()
      throws Exception {
    new MockUp<DataLoadProcessBuilder>() {
      @Mock
      public AbstractDataLoadProcessorStep build(CarbonLoadModel loadModel, String storeLocation,
          CarbonIterator[] inputIterators) throws Exception {
        return writerProcessorStep;
      }
    };

    new MockUp<DataWriterProcessorStepImpl>() {
      @Mock public void initialize() {
      }

      @Mock public Iterator<CarbonRowBatch>[] execute() {
        return iterators;
      }

      @Mock public void close() {
      }
    };

    new MockUp<CarbonTableIdentifier>() {
      @Mock public String getBadRecordLoggerKey() {
        return "testKey";
      }
    };

    new MockUp<BadRecordsLogger>() {
      @Mock String hasBadRecord(String key) {
        return "testValue";
      }
    };

    dataLoadExecutor.execute(loadModel, "/tmp", iterator);
  }

  @Test(expected = CarbonDataLoadingException.class)
  public void testExecuteWithCarbonDataLoadingException() throws Exception {
    new MockUp<DataWriterProcessorStepImpl>() {
      @Mock public void initialize() {
      }

      @Mock public Iterator<CarbonRowBatch>[] execute() {
        return null;
      }

      @Mock public void close() {
      }
    };
    dataLoadExecutor.execute(loadModel, "/tmp", null);
  }

}
