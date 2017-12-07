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

package org.apache.carbondata.processing.loading.sort.unsafe.holder;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
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
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sort.sortdata.NewRowComparator;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

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

  private int numberOfObjectRead;

  private TableFieldStat tableFieldStat;
  private SortStepRowHandler sortStepRowHandler;
  private Comparator<Object[]> comparator;

  /**
   * Constructor to initialize
   */
  public UnsafeSortTempFileChunkHolder(File tempFile, SortParameters parameters) {
    // set temp file
    this.tempFile = tempFile;

    this.tableFieldStat = new TableFieldStat(parameters);
    this.sortStepRowHandler = new SortStepRowHandler(tableFieldStat);
    // set mdkey length
    this.fileBufferSize = parameters.getFileBufferSize();
    this.executorService = Executors.newFixedThreadPool(1);
    comparator = new NewRowComparator(parameters.getNoDictionarySortColumn());
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
    bufferSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
            CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT));
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
      this.numberOfObjectRead++;
      this.returnRow = sortStepRowHandler.readPartedRowFromInputStream(stream);
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
   * get a batch of row, this interface is used in reading compressed sort temp files
   * todo: optimize the code, place read and write in a single code file
   *
   * @param expected expected number in a batch
   * @return a batch of row
   * @throws CarbonSortKeyAndGroupByException if the `expected` number does not match the actually
   * number or encounter some other error
   */
  private Object[][] readBatchedRowFromStream(int expected)
      throws CarbonSortKeyAndGroupByException {
    Object[][] holders;
    if (!isSortTempFileCompressionEnabled) {
      holders = new Object[expected][];
      for (int i = 0; i < expected; i++) {
        Object[] holder = sortStepRowHandler.readPartedRowFromInputStream(stream);
        holders[i] = holder;
      }
      this.numberOfObjectRead += expected;
      return holders;
    }

    ByteArrayInputStream blockDataArray = null;
    DataInputStream dataInputStream = null;
    try {
      int actual = stream.readInt();
      if (expected != actual) {
        throw new CarbonSortKeyAndGroupByException(String.format(
            "Expected %d rows, but found %d rows while reading sort temp file", expected, actual));
      }

      holders = new Object[expected][];

      int compressedContentLength = stream.readInt();
      byte[] compressedContent = new byte[compressedContentLength];
      stream.readFully(compressedContent);
      byte[] decompressedContent = CompressorFactory.getInstance().getCompressor()
          .unCompressByte(compressedContent);

      blockDataArray = new ByteArrayInputStream(decompressedContent);
      dataInputStream = new DataInputStream(blockDataArray);
      for (int i = 0; i < expected; i++) {
        holders[i] = sortStepRowHandler.readPartedRowFromInputStream(dataInputStream);
      }
      this.numberOfObjectRead += expected;
    } catch (IOException e) {
      LOGGER.error(e, "IOException occurs while batch reading sort temp file");
      throw new CarbonSortKeyAndGroupByException(
          "IOException occurs while batch reading sort temp file", e);
    } finally {
      CarbonUtil.closeStreams(dataInputStream);
      CarbonUtil.closeStreams(blockDataArray);
    }
    return holders;
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
    if (null != executorService && !executorService.isShutdown()) {
      executorService.shutdownNow();
    }
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
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof UnsafeSortTempFileChunkHolder)) {
      return false;
    }
    UnsafeSortTempFileChunkHolder o = (UnsafeSortTempFileChunkHolder) obj;

    return this == o;
  }

  @Override public int hashCode() {
    int hash = 0;
    hash += tableFieldStat.hashCode();
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
    return readBatchedRowFromStream(numberOfRecords);
  }
}
