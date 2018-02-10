/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.sort.sortdata;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

public class SortTempFileChunkHolder implements Comparable<SortTempFileChunkHolder> {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortTempFileChunkHolder.class.getName());

  /**
   * temp file
   */
  private File tempFile;

  /**
   * read stream
   */
  private DataInputStream stream;

  /**
   * entry count
   */
  private int entryCount;

  /**
   * number record read
   */
  private int numberOfObjectRead;

  /**
   * return row
   */
  private Object[] returnRow;
  private int dimCnt;
  private int noDictDimCnt;
  private int complexCnt;
  private int measureCnt;
  private boolean[] isNoDictionaryDimensionColumn;
  private boolean[] isNoDictionarySortColumn;
  private DataType[] measureDataTypes;
  private int readBufferSize;
  private String compressorName;

  private Object[][] currentBuffer;

  private Object[][] backupBuffer;

  private boolean isBackupFilled;

  private boolean prefetch;

  private int bufferSize;

  private int bufferRowCounter;

  private ExecutorService executorService;

  private Future<Void> submit;

  private int prefetchRecordsProceesed;

  /**
   * totalRecordFetch
   */
  private int totalRecordFetch;

  /**
   * Constructor to initialize
   *
   * @param tempFile
   * @param sortParameters
   * @param tableName
   */
  public SortTempFileChunkHolder(File tempFile, SortParameters sortParameters, String tableName) {
    // set temp file
    this.tempFile = tempFile;
    this.dimCnt = sortParameters.getDimColCount();
    this.noDictDimCnt = sortParameters.getNoDictionaryCount();
    this.complexCnt = sortParameters.getComplexDimColCount();
    this.measureCnt = sortParameters.getMeasureColCount();
    this.isNoDictionaryDimensionColumn = sortParameters.getNoDictionaryDimnesionColumn();
    this.isNoDictionarySortColumn = sortParameters.getNoDictionarySortColumn();
    this.measureDataTypes = sortParameters.getMeasureDataType();
    this.readBufferSize = sortParameters.getBufferSize();
    this.compressorName = sortParameters.getSortTempCompressorName();

    this.executorService = Executors
        .newFixedThreadPool(1, new CarbonThreadFactory("SafeSortTempChunkHolderPool:" + tableName));
  }

  /**
   * This method will be used to initialize
   *
   * @throws CarbonSortKeyAndGroupByException problem while initializing
   */
  public void initialize() throws CarbonSortKeyAndGroupByException {
    prefetch = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH,
            CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH_DEFAULT));
    bufferSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
            CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT));

    initialise();
  }

  private void initialise() throws CarbonSortKeyAndGroupByException {
    try {
      stream = FileFactory.getDataInputStream(tempFile.getPath(), FileFactory.FileType.LOCAL,
          readBufferSize, compressorName);
      this.entryCount = stream.readInt();
      if (prefetch) {
        new DataFetcher(false).call();
        totalRecordFetch += currentBuffer.length;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      }
    } catch (FileNotFoundException e) {
      LOGGER.error(e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " No Found", e);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " No Found", e);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " Problem while reading", e);
    }
  }

  /**
   * This method will be used to read new row from file
   *
   * @throws CarbonSortKeyAndGroupByException problem while reading
   */
  public void readRow() throws CarbonSortKeyAndGroupByException {
    if (prefetch) {
      fillDataForPrefetch();
    } else {
      this.returnRow = getRowFromStream();
    }
  }

  private void fillDataForPrefetch() {
    if (bufferRowCounter >= bufferSize) {
      if (isBackupFilled) {
        bufferRowCounter = 0;
        currentBuffer = backupBuffer;
        totalRecordFetch += currentBuffer.length;
        isBackupFilled = false;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      } else {
        try {
          submit.get();
        } catch (Exception e) {
          LOGGER.error(e);
        }
        bufferRowCounter = 0;
        currentBuffer = backupBuffer;
        isBackupFilled = false;
        totalRecordFetch += currentBuffer.length;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      }
    }
    prefetchRecordsProceesed++;
    returnRow = currentBuffer[bufferRowCounter++];
  }

  /**
   * Reads row from file
   * @return Object[]
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getRowFromStream() throws CarbonSortKeyAndGroupByException {
    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)

    Object[] holder = new Object[3];
    int index = 0;
    int nonDicIndex = 0;
    int[] dim = new int[dimCnt - noDictDimCnt];
    byte[][] nonDicArray = new byte[noDictDimCnt + complexCnt][];
    Object[] measures = new Object[measureCnt];
    try {
      // read dimension values
      for (int i = 0; i < isNoDictionaryDimensionColumn.length; i++) {
        if (isNoDictionaryDimensionColumn[i]) {
          short len = stream.readShort();
          byte[] array = new byte[len];
          stream.readFully(array);
          nonDicArray[nonDicIndex++] = array;
        } else {
          dim[index++] = stream.readInt();
        }
      }

      for (int i = 0; i < complexCnt; i++) {
        short len = stream.readShort();
        byte[] array = new byte[len];
        stream.readFully(array);
        nonDicArray[nonDicIndex++] = array;
      }

      index = 0;
      // read measure values
      for (int i = 0; i < measureCnt; i++) {
        if (stream.readByte() == 1) {
          DataType dataType = measureDataTypes[i];
          if (dataType == DataTypes.BOOLEAN) {
            measures[index++] = stream.readBoolean();
          } else if (dataType == DataTypes.SHORT) {
            measures[index++] = stream.readShort();
          } else if (dataType == DataTypes.INT) {
            measures[index++] = stream.readInt();
          } else if (dataType == DataTypes.LONG) {
            measures[index++] = stream.readLong();
          } else if (dataType == DataTypes.DOUBLE) {
            measures[index++] = stream.readDouble();
          } else if (DataTypes.isDecimal(dataType)) {
            int len = stream.readInt();
            byte[] buff = new byte[len];
            stream.readFully(buff);
            measures[index++] = DataTypeUtil.byteToBigDecimal(buff);
          } else {
            throw new IllegalArgumentException("unsupported data type:" + dataType);
          }
        } else {
          measures[index++] = null;
        }
      }

      NonDictionaryUtil.prepareOutObj(holder, dim, nonDicArray, measures);

      // increment number if record read
      this.numberOfObjectRead++;
    } catch (IOException e) {
      LOGGER.error("Problme while reading the madkey fom sort temp file");
      throw new CarbonSortKeyAndGroupByException("Problem while reading the sort temp file ", e);
    }

    //return out row
    return holder;
  }

  /**
   * below method will be used to get the row
   *
   * @return row
   */
  public Object[] getRow() {
    return this.returnRow;
  }

  /**
   * below method will be used to check whether any more records are present
   * in file or not
   *
   * @return more row present in file
   */
  public boolean hasNext() {
    if (prefetch) {
      return this.prefetchRecordsProceesed < this.entryCount;
    }
    return this.numberOfObjectRead < this.entryCount;
  }

  /**
   * Below method will be used to close streams
   */
  public void closeStream() {
    CarbonUtil.closeStreams(stream);
    if (null != executorService) {
      executorService.shutdownNow();
    }
    this.backupBuffer = null;
    this.currentBuffer = null;
  }

  /**
   * This method will number of entries
   *
   * @return entryCount
   */
  public int getEntryCount() {
    return entryCount;
  }

  @Override public int compareTo(SortTempFileChunkHolder other) {
    int diff = 0;
    int index = 0;
    int noDictionaryIndex = 0;
    int[] leftMdkArray = (int[]) returnRow[0];
    int[] rightMdkArray = (int[]) other.returnRow[0];
    byte[][] leftNonDictArray = (byte[][]) returnRow[1];
    byte[][] rightNonDictArray = (byte[][]) other.returnRow[1];
    for (boolean isNoDictionary : isNoDictionarySortColumn) {
      if (isNoDictionary) {
        diff = UnsafeComparer.INSTANCE
            .compareTo(leftNonDictArray[noDictionaryIndex], rightNonDictArray[noDictionaryIndex]);
        if (diff != 0) {
          return diff;
        }
        noDictionaryIndex++;
      } else {
        diff = leftMdkArray[index] - rightMdkArray[index];
        if (diff != 0) {
          return diff;
        }
        index++;
      }

    }
    return diff;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof SortTempFileChunkHolder)) {
      return false;
    }
    SortTempFileChunkHolder o = (SortTempFileChunkHolder) obj;

    return this == o;
  }

  @Override public int hashCode() {
    int hash = 0;
    hash += 31 * measureCnt;
    hash += 31 * dimCnt;
    hash += 31 * complexCnt;
    hash += tempFile.hashCode();
    return hash;
  }

  private final class DataFetcher implements Callable<Void> {
    private boolean isBackUpFilling;

    private int numberOfRecords;

    private DataFetcher(boolean backUp) {
      isBackUpFilling = backUp;
      calculateNumberOfRecordsToBeFetched();
    }

    private void calculateNumberOfRecordsToBeFetched() {
      int numberOfRecordsLeftToBeRead = entryCount - totalRecordFetch;
      numberOfRecords =
          bufferSize < numberOfRecordsLeftToBeRead ? bufferSize : numberOfRecordsLeftToBeRead;
    }

    @Override public Void call() throws Exception {
      try {
        if (isBackUpFilling) {
          backupBuffer = prefetchRecordsFromFile(numberOfRecords);
          isBackupFilled = true;
        } else {
          currentBuffer = prefetchRecordsFromFile(numberOfRecords);
        }
      } catch (Exception e) {
        LOGGER.error(e);
      }
      return null;
    }

  }

  /**
   * This method will read the records from sort temp file and keep it in a buffer
   *
   * @param numberOfRecords
   * @return
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[][] prefetchRecordsFromFile(int numberOfRecords)
      throws CarbonSortKeyAndGroupByException {
    Object[][] records = new Object[numberOfRecords][];
    for (int i = 0; i < numberOfRecords; i++) {
      records[i] = getRowFromStream();
    }
    return records;
  }
}
