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
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler;
import org.apache.carbondata.processing.sort.SortTempRowUpdater;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

import org.apache.log4j.Logger;

public class SortTempFileChunkHolder implements Comparable<SortTempFileChunkHolder> {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SortTempFileChunkHolder.class.getName());
  private SortTempRowUpdater sortTempRowUpdater;

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
  protected IntermediateSortTempRow returnRow;
  private int readBufferSize;
  private String compressorName;

  private IntermediateSortTempRow[] currentBuffer;

  private IntermediateSortTempRow[] backupBuffer;

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
  protected TableFieldStat tableFieldStat;
  private SortStepRowHandler sortStepRowHandler;
  protected Comparator<IntermediateSortTempRow> comparator;
  private boolean convertToActualField;

  public SortTempFileChunkHolder(SortParameters sortParameters) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3200
    this.tableFieldStat = new TableFieldStat(sortParameters);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3552
    this.comparator = new FileMergeSortComparator(tableFieldStat.getIsSortColNoDictFlags(),
        tableFieldStat.getNoDictDataType(), tableFieldStat.getNoDictSortColumnSchemaOrderMapping());
    this.sortTempRowUpdater = tableFieldStat.getSortTempRowUpdater();
  }

  /**
   * Constructor to initialize
   *
   * @param tempFile
   * @param sortParameters
   * @param tableName
   */
  public SortTempFileChunkHolder(File tempFile, SortParameters sortParameters, String tableName,
      boolean convertToActualField) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3200
    this(sortParameters);
    // set temp file
    this.tempFile = tempFile;
    this.readBufferSize = sortParameters.getBufferSize();
    this.compressorName = sortParameters.getSortTempCompressorName();
    this.sortStepRowHandler = new SortStepRowHandler(tableFieldStat);
    this.executorService = Executors
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3304
        .newFixedThreadPool(1, new CarbonThreadFactory("SafeSortTempChunkHolderPool:" + tableName,
                true));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2836
    this.convertToActualField = convertToActualField;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3552
    if (this.convertToActualField) {
      this.comparator = new FileMergeSortComparator(
          tableFieldStat.getIsSortColNoDictFlags(), tableFieldStat.getNoDictDataType(),
          tableFieldStat.getNoDictSortColumnSchemaOrderMapping());
    } else {
      this.comparator =
          new IntermediateSortTempRowComparator(tableFieldStat.getIsSortColNoDictFlags(),
              tableFieldStat.getNoDictDataType());
    }
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
      stream = FileFactory.getDataInputStream(tempFile.getPath(),
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
      LOGGER.error(e.getMessage(), e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " No Found", e);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " No Found", e);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
      try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2836
        if (convertToActualField) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
          IntermediateSortTempRow intermediateSortTempRow =
              sortStepRowHandler.readWithNoSortFieldConvert(stream);
          this.sortTempRowUpdater
              .updateSortTempRow(intermediateSortTempRow);
          this.returnRow = intermediateSortTempRow;
        } else {
          this.returnRow = sortStepRowHandler.readWithoutNoSortFieldConvert(stream);
        }
        this.numberOfObjectRead++;
      } catch (IOException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while reading rows", e);
      }
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
          LOGGER.error(e.getMessage(), e);
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
   * Read a batch of row from stream
   *
   * @return Object[]
   * @throws IOException if error occurs while reading from stream
   */
  private IntermediateSortTempRow[] readBatchedRowFromStream(int expected) throws IOException {
    IntermediateSortTempRow[] holders = new IntermediateSortTempRow[expected];
    for (int i = 0; i < expected; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2836
      if (convertToActualField) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
        IntermediateSortTempRow intermediateSortTempRow =
            sortStepRowHandler.readWithNoSortFieldConvert(stream);
        this.sortTempRowUpdater
            .updateSortTempRow(intermediateSortTempRow);
        holders[i] = intermediateSortTempRow;
      } else {
        holders[i] = sortStepRowHandler.readWithoutNoSortFieldConvert(stream);
      }
    }
    this.numberOfObjectRead += expected;
    return holders;
  }

  /**
   * below method will be used to get the sort temp row
   *
   * @return row
   */
  public IntermediateSortTempRow getRow() {
    return this.returnRow;
  }

  /**
   * below method will be used to check whether any more records are present
   * in file or not
   *
   * @return more row present in file
   */
  public boolean hasNext() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1839
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1410
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

  @Override
  public int compareTo(SortTempFileChunkHolder other) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
    return comparator.compare(returnRow, other.getRow());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof SortTempFileChunkHolder)) {
      return false;
    }
    SortTempFileChunkHolder o = (SortTempFileChunkHolder) obj;

    return this == o;
  }

  @Override
  public int hashCode() {
    int hash = 0;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
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

    @Override
    public Void call() {
      try {
        if (isBackUpFilling) {
          backupBuffer = prefetchRecordsFromFile(numberOfRecords);
          isBackupFilled = true;
        } else {
          currentBuffer = prefetchRecordsFromFile(numberOfRecords);
        }
      } catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
        LOGGER.error(e.getMessage(), e);
      }
      return null;
    }

  }

  /**
   * This method will read the records from sort temp file and keep it in a buffer
   *
   * @param numberOfRecords number of records to be read
   * @return batch of intermediate sort temp row
   * @throws IOException if error occurs while reading reading records
   */
  private IntermediateSortTempRow[] prefetchRecordsFromFile(int numberOfRecords)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
      throws IOException {
    return readBatchedRowFromStream(numberOfRecords);
  }
}
