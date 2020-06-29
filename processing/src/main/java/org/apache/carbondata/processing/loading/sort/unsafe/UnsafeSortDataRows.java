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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.IntPointerBuffer;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.memory.UnsafeSortMemoryManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ReUsableByteArrayDataOutputStream;
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

import org.apache.log4j.Logger;

public class UnsafeSortDataRows {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(UnsafeSortDataRows.class.getName());
  private ThreadStatusObserver threadStatusObserver;
  private SortParameters parameters;
  private TableFieldStat tableFieldStat;
  private ThreadLocal<ReUsableByteArrayDataOutputStream> reUsableByteArrayDataOutputStream;
  private UnsafeIntermediateMerger unsafeInMemoryIntermediateFileMerger;

  private UnsafeCarbonRowPage rowPage;

  private long inMemoryChunkSize;

  private final String taskId;

  private int instanceId;

  public UnsafeSortDataRows(SortParameters parameters,
      UnsafeIntermediateMerger unsafeInMemoryIntermediateFileMerger, int inMemoryChunkSize) {
    this.parameters = parameters;
    this.tableFieldStat = new TableFieldStat(parameters);
    this.reUsableByteArrayDataOutputStream = new ThreadLocal<ReUsableByteArrayDataOutputStream>() {
      @Override
      protected ReUsableByteArrayDataOutputStream initialValue() {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        return new ReUsableByteArrayDataOutputStream(byteStream);
      }
    };
    this.unsafeInMemoryIntermediateFileMerger = unsafeInMemoryIntermediateFileMerger;

    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();
    this.taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();
    this.inMemoryChunkSize = inMemoryChunkSize * 1024L * 1024L;
  }

  public void setInstanceId(int instanceId) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3679
    this.instanceId = instanceId;
  }

  public void initialize() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
    MemoryBlock baseBlock =
        UnsafeMemoryManager.allocateMemoryWithRetry(this.taskId, inMemoryChunkSize);
    boolean isMemoryAvailable =
        UnsafeSortMemoryManager.INSTANCE.isMemoryAvailable(baseBlock.size());
    this.rowPage = new UnsafeCarbonRowPage(tableFieldStat, baseBlock, taskId, isMemoryAvailable);
  }

  private UnsafeCarbonRowPage createUnsafeRowPage() {
    MemoryBlock baseBlock =
        UnsafeMemoryManager.allocateMemoryWithRetry(this.taskId, inMemoryChunkSize);
    boolean isSaveToDisk =
        UnsafeSortMemoryManager.INSTANCE.isMemoryAvailable(baseBlock.size());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
    if (!isSaveToDisk) {
      // merge and spill in-memory pages to disk if memory is not enough
      unsafeInMemoryIntermediateFileMerger.tryTriggerInMemoryMerging(true);
    }
    return new UnsafeCarbonRowPage(tableFieldStat, baseBlock, taskId, true);
  }

  public void addRowBatch(Object[][] rowBatch, int size) throws CarbonSortKeyAndGroupByException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2901
    if (rowPage == null) {
      return;
    }
    for (int i = 0; i < size; i++) {
      try {
        if (!rowPage.canAdd()) {
          handlePreviousPage();
          try {
            rowPage = createUnsafeRowPage();
          } catch (Exception ex) {
            // row page has freed in handlePreviousPage(), so other iterator may try to access it.
            rowPage = null;
            LOGGER.error("exception occurred while trying to acquire a semaphore lock: "
                + ex.getMessage(), ex);
            throw new CarbonSortKeyAndGroupByException(ex);
          }
        }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3679
        rowPage.addRow(rowBatch[i], reUsableByteArrayDataOutputStream.get());
      } catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3304
        if (e.getMessage().contains("cannot handle this row. create new page")) {
          rowPage.makeCanAddFail();
          // so that same rowBatch will be handled again in new page
          i--;
        } else {
          LOGGER.error(
              "exception occurred while trying to acquire a semaphore lock: " + e.getMessage(), e);
          throw new CarbonSortKeyAndGroupByException(e);
        }
      }
    }
  }

  public void addRow(Object[] row) throws CarbonSortKeyAndGroupByException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2901
    if (rowPage == null) {
      return;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2927
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2927
    try {
      // if record holder list size is equal to sort buffer size then it will
      // sort the list and then write current list data to file
      if (!rowPage.canAdd()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2238
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2238
        handlePreviousPage();
        try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2232
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2232
          rowPage = createUnsafeRowPage();
        } catch (Exception ex) {
          rowPage = null;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
          LOGGER.error("exception occurred while trying to acquire a semaphore lock: "
              + ex.getMessage(), ex);
          throw new CarbonSortKeyAndGroupByException(ex);
        }
      }
      rowPage.addRow(row, reUsableByteArrayDataOutputStream.get());
    } catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3304
      if (e.getMessage().contains("cannot handle this row. create new page")) {
        rowPage.makeCanAddFail();
        addRow(row);
      } else {
        LOGGER.error(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
            "exception occurred while trying to acquire a semaphore lock: " + e.getMessage(), e);
        throw new CarbonSortKeyAndGroupByException(e);
      }
    }
  }

  /**
   * Below method will be used to start sorting process. This method will get
   * all the temp unsafe pages in memory and all the temp files and try to merge them if possible.
   * Also, it will spill the pages to disk or add it to unsafe sort memory.
   */
  public void startSorting() {
    LOGGER.info("Unsafe based sorting will be used");
    if (this.rowPage.getUsedSize() > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
      TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort = new TimSort<>(
          new UnsafeIntSortDataFormat(rowPage));
      if (parameters.getNumberOfNoDictSortColumns() > 0) {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeRowComparator(rowPage));
      } else {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
            new UnsafeRowComparatorForNormalDims(rowPage));
      }
      unsafeInMemoryIntermediateFileMerger.addDataChunkToMerge(rowPage);
    } else {
      rowPage.freeMemory();
    }
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
      stream = FileFactory.getDataOutputStream(file.getPath(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1839
          parameters.getFileWriteBufferSize(), parameters.getSortTempCompressorName());
      int actualSize = rowPage.getBuffer().getActualSize();
      // write number of entries to the file
      stream.writeInt(actualSize);
      for (int i = 0; i < actualSize; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
        rowPage.writeRow(
            rowPage.getBuffer().get(i) + rowPage.getDataBlock().getBaseOffset(), stream);
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2927
    } catch (IOException | MemoryException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
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
      unsafeInMemoryIntermediateFileMerger.close();
      parameters.getObserver().setFailed(true);
      LOGGER.error(exception);
      throw new CarbonSortKeyAndGroupByException(exception);
    }
  }

  /**
   * Deal with the previous pages added to sort-memory. Carbondata will merge the in-memory pages
   * or merge the sort temp files if possible. After that, carbondata will add current page to
   * sort memory or just spill them.
   */
  private void handlePreviousPage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3679
    try {
      long startTime = System.currentTimeMillis();
      TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort = new TimSort<>(
              new UnsafeIntSortDataFormat(rowPage));
      // if sort_columns is not none, sort by sort_columns
      if (parameters.getNumberOfNoDictSortColumns() > 0) {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
                new UnsafeRowComparator(rowPage));
      } else {
        timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
                new UnsafeRowComparatorForNormalDims(rowPage));
      }
      // get sort storage memory block if memory is available in sort storage manager
      // if space is available then store it in memory, if memory is not available
      // then spill to disk
      MemoryBlock sortStorageMemoryBlock = null;
      if (!rowPage.isSaveToDisk()) {
        sortStorageMemoryBlock =
            UnsafeSortMemoryManager.INSTANCE.allocateMemory(taskId, rowPage.getDataBlock().size());
      }
      if (null == sortStorageMemoryBlock || rowPage.isSaveToDisk()) {
        // create a new file every time
        // create a new file and pick a temp directory randomly every time
        String tmpDir = parameters.getTempFileLocation()[
                new Random().nextInt(parameters.getTempFileLocation().length)];
        File sortTempFile = new File(tmpDir + File.separator + parameters.getTableName()
                + '_' + parameters.getRangeId() + '_' + instanceId + '_' + System.nanoTime()
                + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
        writeDataToFile(rowPage, sortTempFile);
        LOGGER.info("Time taken to sort row page with size" + rowPage.getBuffer().getActualSize()
                + " and write is: " + (System.currentTimeMillis() - startTime) + ": location:"
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2018
                + sortTempFile + ", sort temp file size in MB is "
                + sortTempFile.length() * 0.1 * 10 / 1024 / 1024);
        rowPage.freeMemory();
        // add sort temp filename to and arrayList. When the list size reaches 20 then
        // intermediate merging of sort temp files will be triggered
        unsafeInMemoryIntermediateFileMerger.addFileToMerge(sortTempFile);
      } else {
        // copying data from working memory manager block to storage memory manager block
        CarbonUnsafe.getUnsafe().copyMemory(
                rowPage.getDataBlock().getBaseObject(), rowPage.getDataBlock().getBaseOffset(),
                sortStorageMemoryBlock.getBaseObject(), sortStorageMemoryBlock.getBaseOffset(),
                rowPage.getDataBlock().size());
        // free unsafememory manager
        rowPage.freeMemory();
        rowPage.setNewDataBlock(sortStorageMemoryBlock);
        // add sort temp filename to and arrayList. When the list size reaches 20 then
        // intermediate merging of sort temp files will be triggered
        rowPage.getBuffer().loadToUnsafe();
        unsafeInMemoryIntermediateFileMerger.addDataChunkToMerge(rowPage);
        LOGGER.info("Time taken to sort row page with size: " + rowPage.getBuffer().getActualSize()
                + " is: " + (System.currentTimeMillis() - startTime));
      }
    } catch (Throwable e) {
      try {
        threadStatusObserver.notifyFailed(e);
      } catch (CarbonSortKeyAndGroupByException ex) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
        LOGGER.error(e.getMessage(), e);
      }
    }
  }
}

