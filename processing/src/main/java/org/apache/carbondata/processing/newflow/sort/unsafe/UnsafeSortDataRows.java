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

package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.newflow.sort.unsafe.comparator.UnsafeRowComparator;
import org.apache.carbondata.processing.newflow.sort.unsafe.comparator.UnsafeRowComparatorForNormalDIms;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeCarbonRow;
import org.apache.carbondata.processing.newflow.sort.unsafe.merger.UnsafeIntermediateMerger;
import org.apache.carbondata.processing.newflow.sort.unsafe.sort.TimSort;
import org.apache.carbondata.processing.newflow.sort.unsafe.sort.UnsafeIntSortDataFormat;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortTempFileChunkWriter;
import org.apache.carbondata.processing.sortandgroupby.sortdata.TempSortFileWriter;
import org.apache.carbondata.processing.sortandgroupby.sortdata.TempSortFileWriterFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class UnsafeSortDataRows {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeSortDataRows.class.getName());
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

  private SortParameters parameters;

  private UnsafeIntermediateMerger unsafeInMemoryIntermediateFileMerger;

  private UnsafeCarbonRowPage rowPage;

  private final Object addRowsLock = new Object();

  private int inMemoryChunkSizeInMB;

  private boolean enableInMemoryIntermediateMerge;

  public UnsafeSortDataRows(SortParameters parameters,
      UnsafeIntermediateMerger unsafeInMemoryIntermediateFileMerger) {
    this.parameters = parameters;

    this.unsafeInMemoryIntermediateFileMerger = unsafeInMemoryIntermediateFileMerger;

    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();

    this.inMemoryChunkSizeInMB = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB,
            CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB_DEFAULT));
    enableInMemoryIntermediateMerge = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT,
            CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT_DEFAULT));
  }

  /**
   * This method will be used to initialize
   */
  public void initialize() throws CarbonSortKeyAndGroupByException {
    MemoryBlock baseBlock = getMemoryBlock(inMemoryChunkSizeInMB * 1024 * 1024);
    this.rowPage = new UnsafeCarbonRowPage(parameters.getNoDictionaryDimnesionColumn(),
        parameters.getDimColCount(), parameters.getMeasureColCount(), parameters.getAggType(),
        baseBlock, !UnsafeMemoryManager.INSTANCE.isMemoryAvailable());
    // Delete if any older file exists in sort temp folder
    deleteSortLocationIfExists();

    // create new sort temp directory
    if (!new File(parameters.getTempFileLocation()).mkdirs()) {
      LOGGER.info("Sort Temp Location Already Exists");
    }
    this.dataSorterAndWriterExecutorService =
        Executors.newFixedThreadPool(parameters.getNumberOfCores());
  }

  public static MemoryBlock getMemoryBlock(long size) throws CarbonSortKeyAndGroupByException {
    MemoryBlock baseBlock = null;
    int tries = 0;
    while (true && tries < 100) {
      baseBlock = UnsafeMemoryManager.INSTANCE.allocateMemory(size);
      if (baseBlock == null) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          throw new CarbonSortKeyAndGroupByException(e);
        }
      } else {
        break;
      }
      tries++;
    }
    if (baseBlock == null) {
      throw new CarbonSortKeyAndGroupByException("Not enough memory to create page");
    }
    return baseBlock;
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
      for (int i = 0; i < size; i++) {
        if (rowPage.canAdd()) {
          rowPage.addRow(rowBatch[i]);
        } else {
          try {
            if (enableInMemoryIntermediateMerge) {
              unsafeInMemoryIntermediateFileMerger.startInmemoryMergingIfPossible();
            }
            unsafeInMemoryIntermediateFileMerger.startFileMergingIfPossible();
            dataSorterAndWriterExecutorService.submit(new DataSorterAndWriter(rowPage));
            MemoryBlock memoryBlock = getMemoryBlock(inMemoryChunkSizeInMB * 1024 * 1024);
            boolean saveToDisk = !UnsafeMemoryManager.INSTANCE.isMemoryAvailable();
            rowPage = new UnsafeCarbonRowPage(parameters.getNoDictionaryDimnesionColumn(),
                parameters.getDimColCount(), parameters.getMeasureColCount(),
                parameters.getAggType(), memoryBlock,
                saveToDisk);
            rowPage.addRow(rowBatch[i]);
          } catch (Exception e) {
            LOGGER.error(
                "exception occurred while trying to acquire a semaphore lock: " + e.getMessage());
            throw new CarbonSortKeyAndGroupByException(e);
          }

        }
      }
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
    LOGGER.info("Unsafe based sorting will be used");
    if (this.rowPage.getUsedSize() > 0) {
      TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort = new TimSort<>(
          new UnsafeIntSortDataFormat(rowPage));
      if (parameters.getNoDictionaryCount() > 0) {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeRowComparator(rowPage));
      } else {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeRowComparatorForNormalDIms(parameters.getDimColCount(), rowPage));
      }
      unsafeInMemoryIntermediateFileMerger.addDataChunkToMerge(rowPage);
    } else {
      rowPage.freeMemory();
    }
    startFileBasedMerge();
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

  private void writeData(UnsafeCarbonRowPage rowPage, File file)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {
      // open stream
      stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file),
          parameters.getFileWriteBufferSize()));
      int actualSize = rowPage.getBuffer().getActualSize();
      // write number of entries to the file
      stream.writeInt(actualSize);
      for (int i = 0; i < actualSize; i++) {
        rowPage.fillRow(rowPage.getBuffer().get(i) + rowPage.getDataBlock().getBaseOffset(),
            stream);
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
      unsafeInMemoryIntermediateFileMerger.close();
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
    private UnsafeCarbonRowPage page;

    public DataSorterAndWriter(UnsafeCarbonRowPage rowPage) {
      this.page = rowPage;
    }

    @Override public Void call() throws Exception {
      try {
        long startTime = System.currentTimeMillis();
        TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort = new TimSort<>(
            new UnsafeIntSortDataFormat(page));
        if (parameters.getNoDictionaryCount() > 0) {
          timSort.sort(page.getBuffer(), 0, page.getBuffer().getActualSize(),
              new UnsafeRowComparator(page));
        } else {
          timSort.sort(page.getBuffer(), 0, page.getBuffer().getActualSize(),
              new UnsafeRowComparatorForNormalDIms(parameters.getDimColCount(), page));
        }
        if (rowPage.isSaveToDisk()) {
          // create a new file every time
          File sortTempFile = new File(
              parameters.getTempFileLocation() + File.separator + parameters.getTableName() + System
                  .nanoTime() + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
          writeData(page, sortTempFile);
          LOGGER.info("Time taken to sort row page with size" + page.getBuffer().getActualSize()
              + " and write is: " + (System.currentTimeMillis() - startTime));
          page.freeMemory();
          // add sort temp filename to and arrayList. When the list size reaches 20 then
          // intermediate merging of sort temp files will be triggered
          unsafeInMemoryIntermediateFileMerger.addFileToMerge(sortTempFile);
        } else {
          // add sort temp filename to and arrayList. When the list size reaches 20 then
          // intermediate merging of sort temp files will be triggered
          unsafeInMemoryIntermediateFileMerger.addDataChunkToMerge(page);
          LOGGER.info(
              "Time taken to sort row page with size" + page.getBuffer().getActualSize() + "is: "
                  + (System.currentTimeMillis() - startTime));
        }
      } catch (Throwable e) {
        threadStatusObserver.notifyFailed(e);
      }
      return null;
    }
  }
}

