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

package org.apache.carbondata.processing.newflow.sort.unsafe.holder;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.NewRowComparator;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

public class UnsafeSortTempFileChunkHolder implements SortTempChunkHolder {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeSortTempFileChunkHolder.class.getName());

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
   * return row
   */
  private Object[] returnRow;

  /**
   * number of measures
   */
  private int measureCount;

  /**
   * number of dimensionCount
   */
  private int dimensionCount;

  /**
   * number of complexDimensionCount
   */
  private int complexDimensionCount;

  /**
   * fileBufferSize for file reader stream size
   */
  private int fileBufferSize;

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
   * sortTempFileNoOFRecordsInCompression
   */
  private int sortTempFileNoOFRecordsInCompression;

  /**
   * isSortTempFileCompressionEnabled
   */
  private boolean isSortTempFileCompressionEnabled;

  /**
   * totalRecordFetch
   */
  private int totalRecordFetch;

  private int noDictionaryCount;

  private char[] aggType;

  private int numberOfObjectRead;
  /**
   * to store whether dimension is of dictionary type or not
   */
  private boolean[] isNoDictionaryDimensionColumn;

  private int nullSetWordsLength;

  private Comparator<Object[]> comparator;

  /**
   * Constructor to initialize
   */
  public UnsafeSortTempFileChunkHolder(File tempFile, SortParameters parameters) {
    // set temp file
    this.tempFile = tempFile;

    // set measure and dimension count
    this.measureCount = parameters.getMeasureColCount();
    this.dimensionCount = parameters.getDimColCount();
    this.complexDimensionCount = parameters.getComplexDimColCount();

    this.noDictionaryCount = parameters.getNoDictionaryCount();
    // set mdkey length
    this.fileBufferSize = parameters.getFileBufferSize();
    this.executorService = Executors.newFixedThreadPool(1);
    this.aggType = parameters.getAggType();
    this.isNoDictionaryDimensionColumn = parameters.getNoDictionaryDimnesionColumn();
    this.nullSetWordsLength = ((measureCount - 1) >> 6) + 1;
    comparator = new NewRowComparator(isNoDictionaryDimensionColumn);
    initialize();
  }

  /**
   * This method will be used to initialize
   *
   * @throws CarbonSortKeyAndGroupByException problem while initializing
   */
  public void initialize() {
    prefetch = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH,
            CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH_DEFAULT));
    bufferSize = CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE;
    this.isSortTempFileCompressionEnabled = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
            CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));
    if (this.isSortTempFileCompressionEnabled) {
      LOGGER.info("Compression was used while writing the sortTempFile");
    }

    try {
      this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
              CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
      if (this.sortTempFileNoOFRecordsInCompression < 1) {
        LOGGER.error("Invalid value for: "
            + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
            + ": Only Positive Integer value(greater than zero) is allowed.Default value will"
            + " be used");

        this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Invalid value for: " + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
              + ", only Positive Integer value is allowed.Default value will be used");
      this.sortTempFileNoOFRecordsInCompression = Integer
          .parseInt(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
    }

    initialise();
  }

  private void initialise() {
    try {
      if (isSortTempFileCompressionEnabled) {
        this.bufferSize = sortTempFileNoOFRecordsInCompression;
      }
      stream = new DataInputStream(
          new BufferedInputStream(new FileInputStream(tempFile), this.fileBufferSize));
      this.entryCount = stream.readInt();
      LOGGER.audit("Processing unsafe mode file rows with size : " + entryCount);
      if (prefetch) {
        new DataFetcher(false).call();
        totalRecordFetch += currentBuffer.length;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      } else {
        if (isSortTempFileCompressionEnabled) {
          new DataFetcher(false).call();
        }
      }

    } catch (FileNotFoundException e) {
      LOGGER.error(e);
      throw new RuntimeException(tempFile + " No Found", e);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new RuntimeException(tempFile + " No Found", e);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(tempFile + " Problem while reading", e);
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
    } else if (isSortTempFileCompressionEnabled) {
      if (bufferRowCounter >= bufferSize) {
        try {
          new DataFetcher(false).call();
          bufferRowCounter = 0;
        } catch (Exception e) {
          LOGGER.error(e);
          throw new CarbonSortKeyAndGroupByException(tempFile + " Problem while reading", e);
        }

      }
      prefetchRecordsProceesed++;
      returnRow = currentBuffer[bufferRowCounter++];
    } else {
      Object[] outRow = getRowFromStream();
      this.returnRow = outRow;
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
   * @return
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getRowFromStream() throws CarbonSortKeyAndGroupByException {
    Object[] row = new Object[dimensionCount + measureCount];
    try {
      int dimCount = 0;
      for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
        if (isNoDictionaryDimensionColumn[dimCount]) {
          short aShort = stream.readShort();
          byte[] col = new byte[aShort];
          stream.readFully(col);
          row[dimCount] = col;
        } else {
          int anInt = stream.readInt();
          row[dimCount] = anInt;
        }
      }

      // write complex dimensions here.
      for (; dimCount < dimensionCount; dimCount++) {
        short aShort = stream.readShort();
        byte[] col = new byte[aShort];
        stream.readFully(col);
        row[dimCount] = col;
      }

      long[] words = new long[nullSetWordsLength];
      for (int i = 0; i < words.length; i++) {
        words[i] = stream.readLong();
      }

      for (int mesCount = 0; mesCount < measureCount; mesCount++) {
        if (UnsafeCarbonRowPage.isSet(words, mesCount)) {
          if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
            row[dimensionCount + mesCount] = stream.readDouble();
          } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
            row[dimensionCount + mesCount] = stream.readLong();
          } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
            short aShort = stream.readShort();
            byte[] bigDecimalInBytes = new byte[aShort];
            stream.readFully(bigDecimalInBytes);
            row[dimensionCount + mesCount] = DataTypeUtil.byteToBigDecimal(bigDecimalInBytes);
          }
        }
      }
      return row;
    } catch (Exception e) {
      throw new CarbonSortKeyAndGroupByException(e);
    }
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
    if (prefetch || isSortTempFileCompressionEnabled) {
      return this.prefetchRecordsProceesed < this.entryCount;
    }
    return this.numberOfObjectRead < this.entryCount;
  }

  /**
   * Below method will be used to close streams
   */
  public void close() {
    CarbonUtil.closeStreams(stream);
    executorService.shutdown();
  }

  /**
   * This method will number of entries
   *
   * @return entryCount
   */
  public int numberOfRows() {
    return entryCount;
  }

  @Override public int compareTo(SortTempChunkHolder other) {
    return comparator.compare(returnRow, other.getRow());
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof UnsafeSortTempFileChunkHolder)) {
      return false;
    }
    UnsafeSortTempFileChunkHolder o = (UnsafeSortTempFileChunkHolder) obj;

    return o.compareTo(o) == 0;
  }

  @Override public int hashCode() {
    int hash = 0;
    hash += 31 * measureCount;
    hash += 31 * dimensionCount;
    hash += 31 * complexDimensionCount;
    hash += 31 * noDictionaryCount;
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
