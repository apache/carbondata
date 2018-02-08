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

import java.io.File;
import java.io.FileFilter;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class SingleThreadFinalSortFilesMerger extends CarbonIterator<Object[]> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SingleThreadFinalSortFilesMerger.class.getName());

  /**
   * lockObject
   */
  private static final Object LOCKOBJECT = new Object();

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * fileBufferSize
   */
  private int fileBufferSize;

  /**
   * recordHolderHeap
   */
  private AbstractQueue<SortTempFileChunkHolder> recordHolderHeapLocal;

  /**
   * tableName
   */
  private String tableName;
  private SortParameters sortParameters;
  private SortStepRowHandler sortStepRowHandler;
  /**
   * tempFileLocation
   */
  private String[] tempFileLocation;

  private int maxThreadForSorting;

  private ExecutorService executorService;

  private List<Future<Void>> mergerTask;

  public SingleThreadFinalSortFilesMerger(String[] tempFileLocation, String tableName,
      SortParameters sortParameters) {
    this.tempFileLocation = tempFileLocation;
    this.tableName = tableName;
    this.sortParameters = sortParameters;
    this.sortStepRowHandler = new SortStepRowHandler(sortParameters);
    try {
      maxThreadForSorting = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_READER_THREAD,
              CarbonCommonConstants.CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE));
    } catch (NumberFormatException e) {
      maxThreadForSorting =
          Integer.parseInt(CarbonCommonConstants.CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE);
    }
    this.mergerTask = new ArrayList<>();
  }

  /**
   * This method will be used to merger the merged files
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public void startFinalMerge() throws CarbonDataWriterException {
    List<File> filesToMerge = getFilesToMergeSort();
    if (filesToMerge.size() == 0) {
      LOGGER.info("No file to merge in final merge stage");
      return;
    }

    startSorting(filesToMerge);
  }

  private List<File> getFilesToMergeSort() {
    FileFilter fileFilter = new FileFilter() {
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(tableName);
      }
    };

    // get all the merged files
    List<File> files = new ArrayList<File>(tempFileLocation.length);
    for (String tempLoc : tempFileLocation) {
      File[] subFiles = new File(tempLoc).listFiles(fileFilter);
      if (null != subFiles && subFiles.length > 0) {
        files.addAll(Arrays.asList(subFiles));
      }
    }

    return files;
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startSorting(List<File> files) throws CarbonDataWriterException {
    this.fileCounter = files.size();
    if (fileCounter == 0) {
      LOGGER.info("No files to merge sort");
      return;
    }
    this.fileBufferSize = CarbonDataProcessorUtil
        .getFileBufferSize(this.fileCounter, CarbonProperties.getInstance(),
            CarbonCommonConstants.CONSTANT_SIZE_TEN);

    LOGGER.info("Started Final Merge");

    LOGGER.info("Number of temp file: " + this.fileCounter);

    LOGGER.info("File Buffer Size: " + this.fileBufferSize);

    // create record holder heap
    createRecordHolderQueue();

    // iterate over file list and create chunk holder and add to heap
    LOGGER.info("Started adding first record from each file");
    this.executorService = Executors.newFixedThreadPool(maxThreadForSorting);

    for (final File tempFile : files) {

      Callable<Void> callable = new Callable<Void>() {
        @Override public Void call() throws CarbonSortKeyAndGroupByException {
            // create chunk holder
            SortTempFileChunkHolder sortTempFileChunkHolder =
                new SortTempFileChunkHolder(tempFile, sortParameters, tableName);
          try {
            // initialize
            sortTempFileChunkHolder.initialize();
            sortTempFileChunkHolder.readRow();
          } catch (CarbonSortKeyAndGroupByException ex) {
            sortTempFileChunkHolder.closeStream();
            notifyFailure(ex);
          }
          synchronized (LOCKOBJECT) {
            recordHolderHeapLocal.add(sortTempFileChunkHolder);
          }
          return null;
        }
      };
      mergerTask.add(executorService.submit(callable));
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(2, TimeUnit.HOURS);
    } catch (Exception e) {
      throw new CarbonDataWriterException(e.getMessage(), e);
    }
    checkFailure();
    LOGGER.info("final merger Heap Size" + this.recordHolderHeapLocal.size());
  }

  private void checkFailure() {
    for (int i = 0; i < mergerTask.size(); i++) {
      try {
        mergerTask.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new CarbonDataWriterException(e);
      }
    }
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   */
  private void createRecordHolderQueue() {
    // creating record holder heap
    this.recordHolderHeapLocal = new PriorityQueue<SortTempFileChunkHolder>(fileCounter);
  }

  private synchronized void notifyFailure(Throwable throwable) {
    close();
    LOGGER.error(throwable);
  }

  /**
   * This method will be used to get the sorted sort temp row from the sort temp files
   *
   * @return sorted row
   * @throws CarbonSortKeyAndGroupByException
   */
  public Object[] next() {
    IntermediateSortTempRow sortTempRow = getSortedRecordFromFile();
    return sortStepRowHandler.convertIntermediateSortTempRowTo3Parted(sortTempRow);
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   * @throws CarbonSortKeyAndGroupByException
   */
  private IntermediateSortTempRow getSortedRecordFromFile() throws CarbonDataWriterException {
    IntermediateSortTempRow row = null;

    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    SortTempFileChunkHolder poll = this.recordHolderHeapLocal.poll();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // if chunk is empty then close the stream
      poll.closeStream();

      // change the file counter
      --this.fileCounter;

      // reaturn row
      return row;
    }

    // read new row
    try {
      poll.readRow();
    } catch (CarbonSortKeyAndGroupByException e) {
      close();
      throw new CarbonDataWriterException(e.getMessage(), e);
    }

    // add to heap
    this.recordHolderHeapLocal.add(poll);

    // return row
    return row;
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  public boolean hasNext() {
    return this.fileCounter > 0;
  }

  public void close() {
    if (null != executorService && !executorService.isShutdown()) {
      executorService.shutdownNow();
    }
    if (null != recordHolderHeapLocal) {
      SortTempFileChunkHolder sortTempFileChunkHolder;
      while (!recordHolderHeapLocal.isEmpty()) {
        sortTempFileChunkHolder = recordHolderHeapLocal.poll();
        if (null != sortTempFileChunkHolder) {
          sortTempFileChunkHolder.closeStream();
        }
      }
    }
  }
}
