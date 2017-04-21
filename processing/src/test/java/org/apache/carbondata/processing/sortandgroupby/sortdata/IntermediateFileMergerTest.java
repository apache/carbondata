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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;
import org.apache.carbondata.test.util.StoreCreator;

import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "V1");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "2");
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.SORT_SIZE, "2");

    file1 = new File(new File("target/file1" + System.currentTimeMillis()).getCanonicalPath());
    writeTempFiles(parameters, file1);

    file2 = new File(new File("target/file2" + System.currentTimeMillis()).getCanonicalPath());
    writeTempFiles(parameters, file2);
  }

  @After public void destruct() {
    file1.delete();
    file2.delete();
  }

  @Test public void testCallWithKettleI() {

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

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
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

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
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

  @Test public void testCallWithKettleByteValueMeasure() {

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }
    };

    parameters.setAggType(new char[] { 'c', 'c' });
    File file = new File("kettle4.merge");
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

  @Test public void testCallWithKettleCountValueMeasure() {

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }
    };

    parameters.setAggType(new char[] { 'n', 'n' });
    File file = new File("kettle5.merge");
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

  @Test public void testCallWithKettleBigDecimalMeasure() {

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return new byte[] { 3, 3 };
      }
    };

    parameters.setAggType(new char[] { 'b', 'b' });
    File file = new File("kettle6.merge");
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

    parameters.setUseKettle(false);
    parameters.setNoDictionaryCount(1);
    parameters.setNoDictionaryDimnesionColumn(new boolean[] { false, true });
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

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }

      @Mock public Integer getDimension(int index, Object[] row) {
        return Integer.valueOf(3);
      }
    };

    parameters.setPrefetch(true);
    parameters.setUseKettle(false);
    parameters.setBufferSize(2);
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

  @Test public void testCallWithoutKettleByteValueMeasure() {

    new MockUp<SortTempFileChunkHolder>() {
      @Mock public int compareTo(SortTempFileChunkHolder other) {
        return -1;
      }
    };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }
    };

    parameters.setUseKettle(false);
    parameters.setAggType(new char[] { 'c', 'c' });
    File file = new File("withoutkettle3.merge");
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

  @Test public void testCallWithoutKettleCountValueMeasure() {

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return Double.valueOf(3);
      }
    };
    parameters.setUseKettle(false);
    parameters.setAggType(new char[] { 'n', 'n' });
    File file = new File("withoutkettle4.merge");
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

  @Test public void testCallWithoutKettleBigDecimalMeasure() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<RemoveDictionaryUtil>() {
      @Mock public Object getMeasure(int index, Object[] row) {
        return new byte[] { 3, 3 };
      }
    };
    parameters.setUseKettle(false);
    parameters.setAggType(new char[] { 'b', 'b' });
    File file = new File("withoutkettle5.merge");
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

  @Test public void testIsFailTrueI() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<SortTempFileChunkHolder>() {
      @Mock public void readRow() throws CarbonSortKeyAndGroupByException {
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

  @Test public void testIsFailTrueII() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<DataOutputStream>() {
      @Mock public synchronized void write(int b) throws IOException {
        throw new IOException("exception");
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

  @Test public void testIsFailTrueIII() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<DataOutputStream>() {
      @Mock public synchronized void write(int b) throws IOException {
        throw new IOException("exception");
      }
    };

    File file = new File("fail1.merge");
    parameters.setUseKettle(false);
    intermediateFileMerger =
        new IntermediateFileMerger(parameters, new File[] { file1, file2 }, file);

    try {
      intermediateFileMerger.call();
    } catch (Exception ex) {
      fail("Test case fail, exception not expected .... ");
    }
  }

  @Test public void testFinishException() {
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

  @Test public void testInitializeExceptionI() {
    new MockUp<CarbonUtil>() {
      @Mock public void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
      }
    };

    new MockUp<DataOutputStream>() {
      @Mock public final void writeInt(int v) throws IOException {
        throw new FileNotFoundException("exception");
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

  @Test public void testInitializeExceptionII() {

    new MockUp<DataOutputStream>() {
      @Mock public final void writeInt(int v) throws IOException {
        throw new IOException("exception");
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

  private void writeTempFiles(SortParameters sortParameters, File file) throws IOException {
    Object[][] recordHolderList = new Object[][] {
        new Object[] { new Integer[] { 2, 5 }, new Long[] {}, new Object[] { 2l, 15001l } },
        new Object[] { new Integer[] { 2, 9 }, new Long[] {}, new Object[] { 1l, 15000l } } };

    DataOutputStream stream = null;
    try {
      stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file),
          parameters.getFileWriteBufferSize()));

      // write number of entries to the file
      stream.writeInt(2);
      int dimColCount = parameters.getDimColCount();
      int combinedDimCount = parameters.getNoDictionaryCount() + parameters.getComplexDimColCount();
      char[] aggType = parameters.getAggType();
      Object[] row = null;
      for (int i = 0; i < 2; i++) {
        // get row from record holder list
        row = recordHolderList[i];
        int fieldIndex = 0;

        for (int dimCount = 0; dimCount < dimColCount; dimCount++) {
          stream.writeInt(RemoveDictionaryUtil.getDimension(fieldIndex++, row));
        }

        // if any high cardinality dims are present then write it to the file.

        if (combinedDimCount > 0) {
          stream.write(RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row));
        }

        // as measures are stored in separate array.
        fieldIndex = 0;
        for (int mesCount = 0; mesCount < parameters.getMeasureColCount(); mesCount++) {
          if (null != RemoveDictionaryUtil.getMeasure(fieldIndex, row)) {
            stream.write((byte) 1);
            if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
              Double val = (Double) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeDouble(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
              Long val = (Long) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeLong(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
              BigDecimal val = (BigDecimal) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
              byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
              stream.writeInt(bigDecimalInBytes.length);
              stream.write(bigDecimalInBytes);
            }
          } else {
            stream.write((byte) 0);
          }
          fieldIndex++;
        }
      }
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
    }
  }
}


