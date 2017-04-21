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
import java.math.BigDecimal;
import java.util.concurrent.Semaphore;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.math.BigDecimal.valueOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class SortDataRowsTest {
  private IntermediateFileMerger intermediateFileMerger;
  private SortParameters parameters;
  private File file1, file2;
  private SortIntermediateFileMerger sortIntermediateFileMerger;
  private SortDataRows sortDataRows;

  @Before public void init() throws IOException {

    String file1Path = new File("target/file1" + System.currentTimeMillis()).getCanonicalPath();
    String file2Path = new File("target/file2" + System.currentTimeMillis()).getCanonicalPath();

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
    parameters.setTempFileLocation(file1Path);

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "V1");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "2");
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.SORT_SIZE, "2");

    file1 = new File(file1Path);
    file2 = new File(file2Path);

    sortIntermediateFileMerger = new SortIntermediateFileMerger(parameters);

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);
  }

  @After public void destruct() throws IOException {
    FileUtils.forceDelete(file1);
  }

  @Test public void testInitializeI() throws CarbonSortKeyAndGroupByException {

    new MockUp<CarbonDataProcessorUtil>() {
      @Mock public void deleteSortLocationIfExists(String tempFileLocation)
          throws CarbonSortKeyAndGroupByException {
      }
    };

    sortDataRows.initialize();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testInitializeII() throws CarbonSortKeyAndGroupByException, IOException {

    new MockUp<CarbonDataProcessorUtil>() {
      @Mock public void deleteSortLocationIfExists(String tempFileLocation)
          throws CarbonSortKeyAndGroupByException {
      }
    };

    file1.mkdirs();
    file1.createNewFile();

    sortDataRows.initialize();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test(expected = CarbonSortKeyAndGroupByException.class) public void testInitializeIII()
      throws CarbonSortKeyAndGroupByException, IOException {
    file1.mkdirs();
    file1.createNewFile();

    new MockUp<CarbonDataProcessorUtil>() {
      @Mock public void deleteSortLocationIfExists(String tempFileLocation)
          throws CarbonSortKeyAndGroupByException {
        throw new CarbonSortKeyAndGroupByException("exception");
      }
    };

    sortDataRows.initialize();
  }

  @Test(expected = CarbonSortKeyAndGroupByException.class) public void testAddRowI()
      throws CarbonSortKeyAndGroupByException, IOException {

    new MockUp<Semaphore>() {
      @Mock public void acquire() throws InterruptedException {
        throw new InterruptedException();
      }
    };

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] {});
  }

  @Test public void testStartSortingWithKettleI() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'i', 'i' });

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2, 3, 4, 5 });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithKettleII() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'n', 'n' });
    parameters.setNoDictionaryDimnesionColumn(new boolean[] {});
    parameters.setComplexDimColCount(0);
    parameters.setDimColCount(0);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2.2, 3.2, 4.2, 5.5 });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithKettleIII() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'b', 'b' });
    parameters.setNoDictionaryDimnesionColumn(new boolean[] {});
    parameters.setComplexDimColCount(0);
    parameters.setDimColCount(0);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return BigDecimal.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { valueOf(2), valueOf(3), valueOf(4), valueOf(5) });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithKettleIV() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'b', 'b' });
    parameters.setNoDictionaryDimnesionColumn(new boolean[] {});
    parameters.setComplexDimColCount(0);
    parameters.setDimColCount(0);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return null;
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { valueOf(2), valueOf(3), valueOf(4), valueOf(5) });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithKettleSortCompression()
      throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'i', 'i' });
    parameters.setSortFileCompressionEnabled(true);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2, 3, 4, 5 });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithoutKettleI() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(false);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'i', 'i' });

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2, 3, 4, 5 });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithoutKettleII() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(false);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'n', 'n' });
    parameters.setNoDictionaryDimnesionColumn(new boolean[] {});
    parameters.setComplexDimColCount(0);
    parameters.setDimColCount(0);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2.2, 3.2, 4.2, 5.2 });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithoutKettleIII() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(false);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'l', 'l' });
    parameters.setNoDictionaryDimnesionColumn(new boolean[] {});
    parameters.setComplexDimColCount(0);
    parameters.setDimColCount(0);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2l, 3l, 4l, 5l });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithoutKettleIV() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(false);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'b', 'b' });
    parameters.setNoDictionaryDimnesionColumn(new boolean[] {});
    parameters.setComplexDimColCount(0);
    parameters.setDimColCount(0);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { valueOf(2), valueOf(3), valueOf(4), valueOf(5) });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithoutKettleV() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(false);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 't', 't' });

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2, 3, null, null });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testStartSortingWithoutKettleSortCompression()
      throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(false);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'i', 'i' });
    parameters.setSortFileCompressionEnabled(true);

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRow(new Object[] { 2, 3, 4, 5 });
    sortDataRows.startSorting();

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testAddRowBatchI() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(5);
    parameters.setAggType(new char[] { 'i', 'i' });
    Object[][] recordHolderList = new Object[][] {
        new Object[] { new Integer[] { 2, 5 }, new Long[] {}, new Object[] { 2l, 15001l } },
        new Object[] { new Integer[] { 2, 9 }, new Long[] {}, new Object[] { 1l, 15000l } } };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRowBatch(recordHolderList, 2);

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }

  @Test public void testAddRowBatchII() throws CarbonSortKeyAndGroupByException {
    parameters.setUseKettle(true);
    parameters.setSortBufferSize(2);
    parameters.setAggType(new char[] { 'i', 'i' });
    Object[][] recordHolderList = new Object[][] {
        new Object[] { new Integer[] { 2, 5 }, new Long[] {}, new Object[] { 2l, 15001l } },
        new Object[] { new Integer[] { 2, 9 }, new Long[] {}, new Object[] { 1l, 15000l } } };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    sortDataRows = new SortDataRows(parameters, sortIntermediateFileMerger);

    sortDataRows.initialize();
    sortDataRows.addRowBatch(recordHolderList, 2);

    boolean expectedResult = true;
    assertThat(file1.exists(), is(equalTo(expectedResult)));
  }
}
