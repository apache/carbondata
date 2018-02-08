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

package org.apache.carbondata.processing.loading.sort.unsafe;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.IntPointerBuffer;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.memory.UnsafeSortMemoryManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.processing.loading.sort.unsafe.comparator.UnsafeRowComparator;
import org.apache.carbondata.processing.loading.sort.unsafe.comparator.UnsafeRowComparatorForNormalDims;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeCarbonRow;
import org.apache.carbondata.processing.loading.sort.unsafe.merger.UnsafeIntermediateMerger;
import org.apache.carbondata.processing.loading.sort.unsafe.sort.TimSort;
import org.apache.carbondata.processing.loading.sort.unsafe.sort.UnsafeIntSortDataFormat;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;
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
  private TableFieldStat tableFieldStat;
  private ThreadLocal<ByteBuffer> rowBuffer;
  private UnsafeIntermediateMerger unsafeInMemoryIntermediateFileMerger;

  private UnsafeCarbonRowPage rowPage;

  private final Object addRowsLock = new Object();

  private long inMemoryChunkSize;

  private boolean enableInMemoryIntermediateMerge;

  private int bytesAdded;

  private long maxSizeAllowed;

  /**
   * semaphore which will used for managing sorted data object arrays
   */
  private Semaphore semaphore;

  private final long taskId;

  public UnsafeSortDataRows(SortParameters parameters,
      UnsafeIntermediateMerger unsafeInMemoryIntermediateFileMerger, int inMemoryChunkSize) {
    this.parameters = parameters;
    this.tableFieldStat = new TableFieldStat(parameters);
    this.rowBuffer = new ThreadLocal<ByteBuffer>() {
      @Override protected ByteBuffer initialValue() {
        byte[] backedArray = new byte[2 * 1024 * 1024];
        return ByteBuffer.wrap(backedArray);
      }
    };
    this.unsafeInMemoryIntermediateFileMerger = unsafeInMemoryIntermediateFileMerger;

    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();
    this.taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();
    this.inMemoryChunkSize = inMemoryChunkSize;
    this.inMemoryChunkSize = inMemoryChunkSize * 1024L * 1024L;
    enableInMemoryIntermediateMerge = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT,
            CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT_DEFAULT));

    this.maxSizeAllowed = parameters.getBatchSortSizeinMb();
    if (maxSizeAllowed <= 0) {
      // If user does not input any memory size, then take half the size of usable memory configured
      // in sort memory size.
      this.maxSizeAllowed = UnsafeMemoryManager.INSTANCE.getUsableMemory() / 2;
    } else {
      this.maxSizeAllowed = this.maxSizeAllowed * 1024 * 1024;
    }
  }

  /**
   * This method will be used to initialize
   */
  public void initialize() throws MemoryException {
    MemoryBlock baseBlock =
        UnsafeMemoryManager.allocateMemoryWithRetry(this.taskId, inMemoryChunkSize);
    boolean isMemoryAvailable =
        UnsafeSortMemoryManager.INSTANCE.isMemoryAvailable(baseBlock.size());
    if (isMemoryAvailable) {
      UnsafeSortMemoryManager.INSTANCE.allocateDummyMemory(baseBlock.size());
    }
    this.rowPage = new UnsafeCarbonRowPage(tableFieldStat, baseBlock, !isMemoryAvailable, taskId);
    // Delete if any older file exists in sort temp folder
    deleteSortLocationIfExists();

    // create new sort temp directory
    CarbonDataProcessorUtil.createLocations(parameters.getTempFileLocation());
    this.dataSorterAndWriterExecutorService = Executors
        .newFixedThreadPool(parameters.getNumberOfCores(),
            new CarbonThreadFactory("UnsafeSortDataRowPool:" + parameters.getTableName()));
    semaphore = new Semaphore(parameters.getNumberOfCores());
  }

  public boolean canAdd() {
    return bytesAdded < maxSizeAllowed;
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
      addBatch(rowBatch, size);
    }
  }

  /**
   * This method will be used to add new row
   *
   * @param rowBatch new rowBatch
   * @param size
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  public void addRowBatchWithOutSync(Object[][] rowBatch, int size)
      throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    addBatch(rowBatch, size);
  }

  private void addBatch(Object[][] rowBatch, int size) throws CarbonSortKeyAndGroupByException {
    for (int i = 0; i < size; i++) {
      if (rowPage.canAdd()) {
        bytesAdded += rowPage.addRow(rowBatch[i], rowBuffer.get());
      } else {
        try {
          if (enableInMemoryIntermediateMerge) {
            unsafeInMemoryIntermediateFileMerger.startInmemoryMergingIfPossible();
          }
          unsafeInMemoryIntermediateFileMerger.startFileMergingIfPossible();
          semaphore.acquire();
          dataSorterAndWriterExecutorService.execute(new DataSorterAndWriter(rowPage));
          MemoryBlock memoryBlock =
              UnsafeMemoryManager.allocateMemoryWithRetry(this.taskId, inMemoryChunkSize);
          boolean saveToDisk =
              UnsafeSortMemoryManager.INSTANCE.isMemoryAvailable(memoryBlock.size());
          if (!saveToDisk) {
            UnsafeSortMemoryManager.INSTANCE.allocateDummyMemory(memoryBlock.size());
          }
          rowPage = new UnsafeCarbonRowPage(tableFieldStat, memoryBlock, saveToDisk, taskId);
          bytesAdded += rowPage.addRow(rowBatch[i], rowBuffer.get());
        } catch (Exception e) {
          LOGGER.error(
                  "exception occurred while trying to acquire a semaphore lock: " + e.getMessage());
          throw new CarbonSortKeyAndGroupByException(e);
        }

      }
    }
  }

  /**
   * This method will be used to add new row
   */
  public void addRow(Object[] row) throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    if (rowPage.canAdd()) {
      rowPage.addRow(row, rowBuffer.get());
    } else {
      try {
        if (enableInMemoryIntermediateMerge) {
          unsafeInMemoryIntermediateFileMerger.startInmemoryMergingIfPossible();
        }
        unsafeInMemoryIntermediateFileMerger.startFileMergingIfPossible();
        semaphore.acquire();
        dataSorterAndWriterExecutorService.submit(new DataSorterAndWriter(rowPage));
        MemoryBlock memoryBlock =
            UnsafeMemoryManager.allocateMemoryWithRetry(this.taskId, inMemoryChunkSize);
        boolean saveToDisk = UnsafeSortMemoryManager.INSTANCE.isMemoryAvailable(memoryBlock.size());
        if (!saveToDisk) {
          UnsafeSortMemoryManager.INSTANCE.allocateDummyMemory(memoryBlock.size());
        }
        rowPage = new UnsafeCarbonRowPage(tableFieldStat, memoryBlock, saveToDisk, taskId);
        rowPage.addRow(row, rowBuffer.get());
      } catch (Exception e) {
        LOGGER.error(
            "exception occurred while trying to acquire a semaphore lock: " + e.getMessage());
        throw new CarbonSortKeyAndGroupByException(e);
      }

    }
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws InterruptedException
   */
  public void startSorting() throws InterruptedException {
    LOGGER.info("Unsafe based sorting will be used");
    if (this.rowPage.getUsedSize() > 0) {
      TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort = new TimSort<>(
          new UnsafeIntSortDataFormat(rowPage));
      if (parameters.getNumberOfNoDictSortColumns() > 0) {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeRowComparator(rowPage));
      } else {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeRowComparatorForNormalDims(rowPage));
      }
      unsafeInMemoryIntermediateFileMerger.addDataChunkToMerge(rowPage);
    } else {
      rowPage.freeMemory();
    }
    startFileBasedMerge();
  }

  /**
   * write a page to sort temp file
   * @param rowPage page
   * @param file file
   * @throws CarbonSortKeyAndGroupByException
   */
  private void writeDataToFile(UnsafeCarbonRowPage rowPage, File file)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {
      // open stream
      stream = FileFactory.getDataOutputStream(file.getPath(), FileFactory.FileType.LOCAL,
          parameters.getFileWriteBufferSize(), parameters.getSortTempCompressorName());
      int actualSize = rowPage.getBuffer().getActualSize();
      // write number of entries to the file
      stream.writeInt(actualSize);
      for (int i = 0; i < actualSize; i++) {
        rowPage.writeRow(
            rowPage.getBuffer().get(i) + rowPage.getDataBlock().getBaseOffset(), stream);
      }
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
    }
  }

  /**
   * This method will be used to delete sort temp location is it is exites
   */
  public void deleteSortLocationIfExists() {
    CarbonDataProcessorUtil.deleteSortLocationIfExists(parameters.getTempFileLocation());
  }

  /**
   * Below method will be used to start file based merge
   *
   * @throws InterruptedException
   */
  private void startFileBasedMerge() throws InterruptedException {
    dataSorterAndWriterExecutorService.shutdown();
    dataSorterAndWriterExecutorService.awaitTermination(2, TimeUnit.DAYS);
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
  private class DataSorterAndWriter implements Runnable {
    private UnsafeCarbonRowPage page;

    public DataSorterAndWriter(UnsafeCarbonRowPage rowPage) {
      this.page = rowPage;
    }

    @Override
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort = new TimSort<>(
            new UnsafeIntSortDataFormat(page));
        // if sort_columns is not none, sort by sort_columns
        if (parameters.getNumberOfNoDictSortColumns() > 0) {
          timSort.sort(page.getBuffer(), 0, page.getBuffer().getActualSize(),
              new UnsafeRowComparator(page));
        } else {
          timSort.sort(page.getBuffer(), 0, page.getBuffer().getActualSize(),
              new UnsafeRowComparatorForNormalDims(page));
        }
        if (page.isSaveToDisk()) {
          // create a new file every time
          // create a new file and pick a temp directory randomly every time
          String tmpDir = parameters.getTempFileLocation()[
              new Random().nextInt(parameters.getTempFileLocation().length)];
          File sortTempFile = new File(
              tmpDir + File.separator + parameters.getTableName()
                  + System.nanoTime() + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
          writeDataToFile(page, sortTempFile);
          LOGGER.info("Time taken to sort row page with size" + page.getBuffer().getActualSize()
              + " and write is: " + (System.currentTimeMillis() - startTime) + ": location:"
              + sortTempFile + ", sort temp file size in MB is "
              + sortTempFile.length() * 0.1 * 10 / 1024 / 1024);
          page.freeMemory();
          // add sort temp filename to and arrayList. When the list size reaches 20 then
          // intermediate merging of sort temp files will be triggered
          unsafeInMemoryIntermediateFileMerger.addFileToMerge(sortTempFile);
        } else {
          // creating a new memory block as size is already allocated
          // so calling lazy memory allocator
          MemoryBlock newMemoryBlock = UnsafeSortMemoryManager.INSTANCE
              .allocateMemoryLazy(taskId, page.getDataBlock().size());
          // copying data from working memory manager to sortmemory manager
          CarbonUnsafe.getUnsafe()
              .copyMemory(page.getDataBlock().getBaseObject(), page.getDataBlock().getBaseOffset(),
                  newMemoryBlock.getBaseObject(), newMemoryBlock.getBaseOffset(),
                  page.getDataBlock().size());
          // free unsafememory manager
          page.freeMemory();
          page.setNewDataBlock(newMemoryBlock);
          // add sort temp filename to and arrayList. When the list size reaches 20 then
          // intermediate merging of sort temp files will be triggered
          page.getBuffer().loadToUnsafe();
          unsafeInMemoryIntermediateFileMerger.addDataChunkToMerge(page);
          LOGGER.info(
              "Time taken to sort row page with size: " + page.getBuffer().getActualSize() + "is: "
                  + (System.currentTimeMillis() - startTime));
        }
      } catch (Throwable e) {
        try {
          threadStatusObserver.notifyFailed(e);
        } catch (CarbonSortKeyAndGroupByException ex) {
          LOGGER.error(e);
        }
      } finally {
        semaphore.release();
      }
    }
  }
}

