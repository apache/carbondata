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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.impl.ParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class SortProcessorStepImplTest {

  private static Object inputData[] = { "harsh", "sharma", 26 };
  private static Object inputData2[] = { "vishnu", "sharma", 24 };
  private static DataField dataField;
  private static ColumnSchema columnSchema;
  private static SortProcessorStepImpl sortProcessorStep;
  private static CarbonIterator iterator[];
  private static SortParameters parameters;
  private static CarbonTableIdentifier tableIdentifier;
  private static CarbonDataLoadConfiguration config;
  private static Iterator<CarbonRowBatch>[] iterators = new Iterator[2];
  private static Iterator<CarbonRowBatch>[] resultIterators = new Iterator[2];
  private static AbstractDataLoadProcessorStep dataLoadProcessorStep =
      new InputProcessorStepImpl(new CarbonDataLoadConfiguration(), iterator);
  private static CarbonRowBatch batch1 = new CarbonRowBatch();
  private static CarbonRowBatch batch2 = new CarbonRowBatch();

  @BeforeClass public static void setUp() {

    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("name");
    config = new CarbonDataLoadConfiguration();
    tableIdentifier = new CarbonTableIdentifier("derby", "test", "TB100");
    config.setTableIdentifier(new AbsoluteTableIdentifier("/tmp", tableIdentifier));
    dataField = new DataField(new CarbonColumn(columnSchema, 0, 0));
    batch1.addRow(new CarbonRow(inputData));
    batch2.addRow(new CarbonRow(inputData2));
    iterators[0] = new ArrayList<CarbonRowBatch>(Arrays.asList(batch1)).iterator();
    iterators[1] = new ArrayList<CarbonRowBatch>(Arrays.asList(batch2)).iterator();
    sortProcessorStep = new SortProcessorStepImpl(config, dataLoadProcessorStep);
    parameters = new SortParameters();
  }

  @Test public void testGetOutput() {
    new MockUp<InputProcessorStepImpl>() {
      @Mock public DataField[] getOutput() {
        return new DataField[] { dataField };
      }
    };

    DataField result[] = sortProcessorStep.getOutput();
    assertEquals(result.length, 1);
  }

  @Test public void testProcessRow() {
    assertNull(sortProcessorStep.processRow(new CarbonRow(inputData)));
  }

  /**
   * Test case to execute the Sort Processing steps
   */
  @Test public void testExecute() throws CarbonDataLoadingException {
    new MockUp<InputProcessorStepImpl>() {
      @Mock public Iterator<CarbonRowBatch>[] execute() {
        return iterators;
      }

      @Mock public void initialize() {
      }

      @Mock public void close() {
      }
    };

    new MockUp<ParallelReadMergeSorterImpl>() {
      @Mock public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iter) {
        return resultIterators;
      }

      @Mock public void initialize(SortParameters sortParameters) {
      }

      @Mock public void close() {
      }
    };

    new MockUp<SortParameters>() {
      @Mock SortParameters createSortParameters(CarbonDataLoadConfiguration configuration) {
        return parameters;
      }
    };

    sortProcessorStep.initialize();
    sortProcessorStep.close();
    Iterator<CarbonRowBatch> result[] = sortProcessorStep.execute();
    assertEquals(result.length, 2);

  }

}
