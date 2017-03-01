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

package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

public class SortDataRows {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortDataRows.class.getName());
  /**
   * entryCount
   */
  private int entryCount;
  /**
   * record holder array
   */
  private Object[][] recordHolderList;
  /**
   * threadStatusObserver
   */
  private ThreadStatusObserver threadStatusObserver;
  /**
   * executor service for data sort holder
   */
  private ExecutorService dataSorterAndWriterExecutorService;
  /**
   * semaphore which will used for managing sorted data object arrays
   */
  private Semaphore semaphore;

  private SortParameters parameters;

  private int sortBufferSize;

  private SortIntermediateFileMerger intermediateFileMerger;

  private final Object addRowsLock = new Object();

  public SortDataRows(SortParameters parameters,
      SortIntermediateFileMerger intermediateFileMerger) {
    this.parameters = parameters;

    this.intermediateFileMerger = intermediateFileMerger;

    int batchSize = CarbonProperties.getInstance().getBatchSize();

    this.sortBufferSize = Math.max(parameters.getSortBufferSize(), batchSize);
    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();
  }

  /**
   * This method will be used to initialize
   */
  public void initialize() throws CarbonSortKeyAndGroupByException {

    // create holder list which will hold incoming rows
    // size of list will be sort buffer size + 1 to avoid creation of new
    // array in list array
    this.recordHolderList = new Object[sortBufferSize][];
    // Delete if any older file exists in sort temp folder
    deleteSortLocationIfExists();

    // create new sort temp directory
    if (!new File(parameters.getTempFileLocation()).mkdirs()) {
      LOGGER.info("Sort Temp Location Already Exists");
    }
    this.dataSorterAndWriterExecutorService =
        Executors.newFixedThreadPool(parameters.getNumberOfCores());
    semaphore = new Semaphore(parameters.getNumberOfCores());
  }

  /**
   * This method will be used to add new row
   *
   * @param row new row
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  public void addRow(Object[] row) throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    int currentSize = entryCount;

    if (sortBufferSize == currentSize) {
      LOGGER.debug("************ Writing to temp file ********** ");
      intermediateFileMerger.startMergingIfPossible();
      Object[][] recordHolderListLocal = recordHolderList;
      try {
        semaphore.acquire();
        dataSorterAndWriterExecutorService.submit(new DataSorterAndWriter(recordHolderListLocal));
      } catch (InterruptedException e) {
        LOGGER.error(
            "exception occurred while trying to acquire a semaphore lock: " + e.getMessage());
        throw new CarbonSortKeyAndGroupByException(e.getMessage());
      }
      // create the new holder Array
      this.recordHolderList = new Object[this.sortBufferSize][];
      this.entryCount = 0;
    }
    recordHolderList[entryCount++] = row;
  }

  /**
   * This method will be used to add new row
   *
   * @param rowBatch new rowBatch
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  public void addRowBatch(Object[][] rowBatch, int size) throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    synchronized (addRowsLock) {
      int sizeLeft = 0;
      if (entryCount + size >= sortBufferSize) {
        LOGGER.debug("************ Writing to temp file ********** ");
        intermediateFileMerger.startMergingIfPossible();
        Object[][] recordHolderListLocal = recordHolderList;
        sizeLeft = sortBufferSize - entryCount ;
        if (sizeLeft > 0) {
          System.arraycopy(rowBatch, 0, recordHolderListLocal, entryCount, sizeLeft);
        }
        try {
          dataSorterAndWriterExecutorService.submit(new DataSorterAndWriter(recordHolderListLocal));
        } catch (Exception e) {
          LOGGER.error(
              "exception occurred while trying to acquire a semaphore lock: " + e.getMessage());
          throw new CarbonSortKeyAndGroupByException(e);
        }
        // create the new holder Array
        this.recordHolderList = new Object[this.sortBufferSize][];
        this.entryCount = 0;
        size = size - sizeLeft;
        if (size == 0) {
          return;
        }
      }
      System.arraycopy(rowBatch, sizeLeft, recordHolderList, entryCount, size);
      entryCount += size;
    }
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public void startSorting() throws CarbonSortKeyAndGroupByException {
    LOGGER.info("File based sorting will be used");
    if (this.entryCount > 0) {
      Object[][] toSort;
      toSort = new Object[entryCount][];
      System.arraycopy(recordHolderList, 0, toSort, 0, entryCount);

      if (parameters.isUseKettle()) {
        if (parameters.getNoDictionaryCount() > 0) {
          Arrays.sort(toSort, new RowComparator(parameters.getNoDictionaryDimnesionColumn(),
              parameters.getNoDictionaryCount()));
        } else {
          Arrays.sort(toSort, new RowComparatorForNormalDims(parameters.getDimColCount()));
        }
      } else {
        if (parameters.getNoDictionaryCount() > 0) {
          Arrays.sort(toSort, new NewRowComparator(parameters.getNoDictionaryDimnesionColumn()));
        } else {
          Arrays.sort(toSort, new NewRowComparatorForNormalDims(parameters.getDimColCount()));
        }
      }
      recordHolderList = toSort;

      // create new file
      File file = new File(
          parameters.getTempFileLocation() + File.separator + parameters.getTableName() +
              System.nanoTime() + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
      writeDataTofile(recordHolderList, this.entryCount, file);

    }

    startFileBasedMerge();
    this.recordHolderList = null;
  }

  /**
   * Below method will be used to write data to file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataTofile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    // stream
    if (parameters.isSortFileCompressionEnabled() || parameters.isPrefetch()) {
      writeSortTempFile(recordHolderList, entryCountLocal, file);
      return;
    }
    if (parameters.isUseKettle()) {
      writeData(recordHolderList, entryCountLocal, file);
    } else {
      writeDataWithOutKettle(recordHolderList, entryCountLocal, file);
    }
  }

  private void writeSortTempFile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    TempSortFileWriter writer = null;

    try {
      writer = getWriter();
      writer.initiaize(file, entryCountLocal);
      writer.writeSortTempFile(recordHolderList);
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e, "Problem while writing the sort temp file");
      throw e;
    } finally {
      if (writer != null) {
        writer.finish();
      }
    }
  }

  // TODO Remove it after kettle got removed
  private void writeData(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {
      // open stream
      stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file),
          parameters.getFileWriteBufferSize()));

      // write number of entries to the file
      stream.writeInt(entryCountLocal);
      int dimColCount = parameters.getDimColCount();
      int combinedDimCount = parameters.getNoDictionaryCount() + parameters.getComplexDimColCount();
      char[] aggType = parameters.getAggType();
      Object[] row = null;
      for (int i = 0; i < entryCountLocal; i++) {
        // get row from record holder list
        row = recordHolderList[i];
        int fieldIndex = 0;

        for (int dimCount = 0; dimCount < dimColCount; dimCount++) {
          stream.writeInt(NonDictionaryUtil.getDimension(fieldIndex++, row));
        }

        // if any high cardinality dims are present then write it to the file.

        if (combinedDimCount > 0) {
          stream.write(NonDictionaryUtil.getByteArrayForNoDictionaryCols(row));
        }

        // as measures are stored in separate array.
        fieldIndex = 0;
        for (int mesCount = 0; mesCount < parameters.getMeasureColCount(); mesCount++) {
          if (null != NonDictionaryUtil.getMeasure(fieldIndex, row)) {
            stream.write((byte) 1);
            if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
              Double val = (Double) NonDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeDouble(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
              Long val = (Long) NonDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeLong(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
              BigDecimal val = (BigDecimal) NonDictionaryUtil.getMeasure(fieldIndex, row);
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
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
    }
  }

  private void writeDataWithOutKettle(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {
      // open stream
      stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file),
          parameters.getFileWriteBufferSize()));

      // write number of entries to the file
      stream.writeInt(entryCountLocal);
      int complexDimColCount = parameters.getComplexDimColCount();
      int dimColCount = parameters.getDimColCount() + complexDimColCount;
      char[] aggType = parameters.getAggType();
      boolean[] noDictionaryDimnesionMapping = parameters.getNoDictionaryDimnesionColumn();
      Object[] row = null;
      for (int i = 0; i < entryCountLocal; i++) {
        // get row from record holder list
        row = recordHolderList[i];
        int dimCount = 0;
        // write dictionary and non dictionary dimensions here.
        for (; dimCount < noDictionaryDimnesionMapping.length; dimCount++) {
          if (noDictionaryDimnesionMapping[dimCount]) {
            byte[] col = (byte[]) row[dimCount];
            stream.writeShort(col.length);
            stream.write(col);
          } else {
            stream.writeInt((int)row[dimCount]);
          }
        }
        // write complex dimensions here.
        for (; dimCount < dimColCount; dimCount++) {
          byte[] value = (byte[])row[dimCount];
          stream.writeShort(value.length);
          stream.write(value);
        }
        // as measures are stored in separate array.
        for (int mesCount = 0;
             mesCount < parameters.getMeasureColCount(); mesCount++) {
          Object value = row[mesCount + dimColCount];
          if (null != value) {
            stream.write((byte) 1);
            if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
              Double val = (Double) value;
              stream.writeDouble(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
              Long val = (Long) value;
              stream.writeLong(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
              BigDecimal val = (BigDecimal) value;
              byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
              stream.writeInt(bigDecimalInBytes.length);
              stream.write(bigDecimalInBytes);
            }
          } else {
            stream.write((byte) 0);
          }
        }
      }
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
    }
  }

  private TempSortFileWriter getWriter() {
    TempSortFileWriter chunkWriter = null;
    TempSortFileWriter writer = TempSortFileWriterFactory.getInstance()
        .getTempSortFileWriter(parameters.isSortFileCompressionEnabled(),
            parameters.getDimColCount(), parameters.getComplexDimColCount(),
            parameters.getMeasureColCount(), parameters.getNoDictionaryCount(),
            parameters.getFileWriteBufferSize());

    if (parameters.isPrefetch() && !parameters.isSortFileCompressionEnabled()) {
      chunkWriter = new SortTempFileChunkWriter(writer, parameters.getBufferSize());
    } else {
      chunkWriter =
          new SortTempFileChunkWriter(writer, parameters.getSortTempFileNoOFRecordsInCompression());
    }

    return chunkWriter;
  }

  /**
   * This method will be used to delete sort temp location is it is exites
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public void deleteSortLocationIfExists() throws CarbonSortKeyAndGroupByException {
    CarbonDataProcessorUtil.deleteSortLocationIfExists(parameters.getTempFileLocation());
  }

  /**
   * Below method will be used to start file based merge
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startFileBasedMerge() throws CarbonSortKeyAndGroupByException {
    try {
      dataSorterAndWriterExecutorService.shutdown();
      dataSorterAndWriterExecutorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while shutdown the server ", e);
    }
  }

  /**
   * Observer class for thread execution
   * In case of any failure we need stop all the running thread
   */
  private class ThreadStatusObserver {
    /**
     * Below method will be called if any thread fails during execution
     *
     * @param exception
     * @throws CarbonSortKeyAndGroupByException
     */
    public void notifyFailed(Throwable exception) throws CarbonSortKeyAndGroupByException {
      dataSorterAndWriterExecutorService.shutdownNow();
      intermediateFileMerger.close();
      parameters.getObserver().setFailed(true);
      LOGGER.error(exception);
      throw new CarbonSortKeyAndGroupByException(exception);
    }
  }

  /**
   * This class is responsible for sorting and writing the object
   * array which holds the records equal to given array size
   */
  private class DataSorterAndWriter implements Callable<Void> {
    private Object[][] recordHolderArray;

    public DataSorterAndWriter(Object[][] recordHolderArray) {
      this.recordHolderArray = recordHolderArray;
    }

    @Override public Void call() throws Exception {
      try {
        long startTime = System.currentTimeMillis();
        if (parameters.isUseKettle()) {
          if (parameters.getNoDictionaryCount() > 0) {
            Arrays.sort(recordHolderArray,
                new RowComparator(parameters.getNoDictionaryDimnesionColumn(),
                    parameters.getNoDictionaryCount()));
          } else {
            Arrays.sort(recordHolderArray,
                new RowComparatorForNormalDims(parameters.getDimColCount()));
          }
        } else {
          if (parameters.getNoDictionaryCount() > 0) {
            Arrays.sort(recordHolderArray,
                new NewRowComparator(parameters.getNoDictionaryDimnesionColumn()));
          } else {
            Arrays.sort(recordHolderArray,
                new NewRowComparatorForNormalDims(parameters.getDimColCount()));
          }
        }

        // create a new file every time
        File sortTempFile = new File(
            parameters.getTempFileLocation() + File.separator + parameters.getTableName() + System
                .nanoTime() + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
        writeDataTofile(recordHolderArray, recordHolderArray.length, sortTempFile);
        // add sort temp filename to and arrayList. When the list size reaches 20 then
        // intermediate merging of sort temp files will be triggered
        intermediateFileMerger.addFileToMerge(sortTempFile);
        LOGGER.info("Time taken to sort and write sort temp file " + sortTempFile + " is: " + (
            System.currentTimeMillis() - startTime));
      } catch (Throwable e) {
        threadStatusObserver.notifyFailed(e);
      } finally {
        semaphore.release();
      }
      return null;
    }
  }
}

