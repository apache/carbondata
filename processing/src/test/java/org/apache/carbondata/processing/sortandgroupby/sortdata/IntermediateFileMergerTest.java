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
package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;

public class IntermediateFileMergerTest {

  private IntermediateFileMerger intermediateFileMerger;
  private SortParameters parameters;
  private File file1, file2;

  @Before public void init() throws IOException {
    parameters = new SortParameters();
    parameters.setBufferSize(20000);
    parameters.setMeasureColCount(2);
    parameters.setDimColCount(2);
    parameters.setComplexDimColCount(0);
    parameters.setFileBufferSize(5242880);
    parameters.setNumberOfIntermediateFileToBeMerged(2);
    parameters.setFileWriteBufferSize(50000);

    SortObserver observer = new SortObserver();
    observer.setFailed(false);

    parameters.setObserver(observer);
    parameters.setSortTempFileNoOFRecordsInCompression(50);
    parameters.setSortFileCompressionEnabled(false);
    parameters.setPrefetch(false);
    parameters.setDatabaseName("default");
    parameters.setTableName("t3");
    parameters.setAggType(new char[] { 'l', 'l' });
    parameters.setNoDictionaryCount(0);
    parameters.setPartitionID("0");
    parameters.setSegmentId("0");
    parameters.setTaskNo("0");
    parameters.setNoDictionaryDimnesionColumn(new boolean[] { false, false });
    parameters.setNumberOfCores(1);
    parameters.setUseKettle(true);

    file1 = new File(new File("src/test/resources/tempfiles/file1").getCanonicalPath());
    file2 = new File(new File("src/test/resources/tempfiles/file2").getCanonicalPath());
  }

  @Test public void testCallWithKettleI() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    File file = new File("kettle1.merge");
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
      boolean expectedResult = true;

      assertThat(file.exists(), is(equalTo(expectedResult)));
      boolean result = file.delete();
      assertThat(result, is(equalTo(expectedResult)));
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test public void testCallWithKettleII() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock
      public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }
    };

    parameters.setPrefetch(true);

    File file = new File("kettle2.merge");
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
      boolean expectedResult = true;

      assertThat(file.exists(), is(equalTo(expectedResult)));
      boolean result = file.delete();
      assertThat(result, is(equalTo(expectedResult)));
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test public void testCallWithKettleIII() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock
      public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }
    };

    parameters.setBufferSize(2);
    parameters.setPrefetch(true);

    File file = new File("kettle3.merge");
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
      boolean expectedResult = true;

      assertThat(file.exists(), is(equalTo(expectedResult)));
      boolean result = file.delete();
      assertThat(result, is(equalTo(expectedResult)));
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test public void testCallWithoutKettleI() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    parameters.setUseKettle(false);

    File file = new File("withoutkettle1.merge");
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
      boolean expectedResult = true;

      assertThat(file.exists(), is(equalTo(expectedResult)));
      boolean result = file.delete();
      assertThat(result, is(equalTo(expectedResult)));
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test public void testCallWithoutKettleII() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock
      public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock
      public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    parameters.setPrefetch(true);
    parameters.setUseKettle(false);

    File file = new File("withoutkettle2.merge");
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
      boolean expectedResult = true;

      assertThat(file.exists(), is(equalTo(expectedResult)));
      boolean result = file.delete();
      assertThat(result, is(equalTo(expectedResult)));
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test
  public void testIsFailTrue() {
    new MockUp<SortTempFileChunkHolder>() {
      @Mock
      public void readRow() throws CarbonSortKeyAndGroupByException {
        throw new CarbonSortKeyAndGroupByException("exception");
      }
    };

    File file = new File("fail1.merge");

    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test
  public void testFinishException() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
        throw new CarbonUtilException("exception");
      }
    };

    File file = new File("kettle1.merge");
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
      boolean expectedResult = true;

      assertThat(file.exists(), is(equalTo(expectedResult)));
      boolean result = file.delete();
      assertThat(result, is(equalTo(expectedResult)));
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }
}
