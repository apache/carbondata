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

package org.apache.carbondata.processing.loading.sort.unsafe.merger;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;
import org.apache.carbondata.processing.loading.sort.CarbonPriorityQueue;
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.SortTempChunkHolder;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeFinalMergePageHolder;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeInmemoryHolder;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeSortTempFileChunkHolder;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

import org.apache.log4j.Logger;

public class UnsafeSingleThreadFinalSortFilesMerger extends CarbonIterator<Object[]> {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(UnsafeSingleThreadFinalSortFilesMerger.class.getName());

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * recordHolderHeap
   */
  private CarbonPriorityQueue<SortTempChunkHolder> recordHolderHeapLocal;

  private SortParameters parameters;
  private SortStepRowHandler sortStepRowHandler;
  /**
   * tempFileLocation
   */
  private String[] tempFileLocation;

  private String tableName;

  private boolean isStopProcess;

  public UnsafeSingleThreadFinalSortFilesMerger(SortParameters parameters,
      String[] tempFileLocation) {
    this.parameters = parameters;
    this.sortStepRowHandler = new SortStepRowHandler(parameters);
    this.tempFileLocation = tempFileLocation;
    this.tableName = parameters.getTableName();
  }

  /**
   * This method will be used to merger the merged files
   *
   */
  public void startFinalMerge(UnsafeCarbonRowPage[] rowPages,
      List<UnsafeInMemoryIntermediateDataMerger> merges) throws CarbonDataWriterException {
    // remove the spilled pages
    for (Iterator<UnsafeInMemoryIntermediateDataMerger> iter = merges.iterator();
         iter.hasNext(); ) {
      UnsafeInMemoryIntermediateDataMerger merger = iter.next();
      if (merger.isSpillDisk()) {
        // it has already been closed once the spill is finished, so no need to close it here
        iter.remove();
      }
    }
    startSorting(rowPages, merges);
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   */
  private void startSorting(UnsafeCarbonRowPage[] rowPages,
      List<UnsafeInMemoryIntermediateDataMerger> merges) throws CarbonDataWriterException {
    try {
      List<File> filesToMergeSort = getFilesToMergeSort();
      this.fileCounter = rowPages.length + filesToMergeSort.size() + merges.size();
      if (fileCounter == 0) {
        LOGGER.info("No files to merge sort");
        return;
      }
      LOGGER.info(String.format("Starting final merge of %d pages, including row pages: %d"
          + ", sort temp files: %d, intermediate merges: %d",
          this.fileCounter, rowPages.length, filesToMergeSort.size(), merges.size()));

      // create record holder heap
      createRecordHolderQueue();
      TableFieldStat tableFieldStat = new TableFieldStat(parameters);
      // iterate over file list and create chunk holder and add to heap
      LOGGER.info("Started adding first record from each page");
      for (final UnsafeCarbonRowPage rowPage : rowPages) {

        SortTempChunkHolder sortTempFileChunkHolder = new UnsafeInmemoryHolder(rowPage);

        // initialize
        sortTempFileChunkHolder.readRow();

        recordHolderHeapLocal.add(sortTempFileChunkHolder);
      }

      for (final UnsafeInMemoryIntermediateDataMerger merger : merges) {

        SortTempChunkHolder sortTempFileChunkHolder =
            new UnsafeFinalMergePageHolder(merger, tableFieldStat);

        // initialize
        sortTempFileChunkHolder.readRow();

        recordHolderHeapLocal.add(sortTempFileChunkHolder);
      }

      for (final File file : filesToMergeSort) {

        SortTempChunkHolder sortTempFileChunkHolder =
            new UnsafeSortTempFileChunkHolder(file, parameters, true, tableFieldStat);

        // initialize
        sortTempFileChunkHolder.readRow();

        recordHolderHeapLocal.add(sortTempFileChunkHolder);
      }

      LOGGER.info("Heap Size: " + this.recordHolderHeapLocal.size());
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new CarbonDataWriterException(e);
    }
  }

  private List<File> getFilesToMergeSort() {
    // this can be partitionId, bucketId or rangeId, let's call it rangeId
    final int rangeId = parameters.getRangeId();

    FileFilter fileFilter = new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(tableName + '_' + rangeId);
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
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   */
  private void createRecordHolderQueue() {
    // creating record holder heap
    this.recordHolderHeapLocal = new CarbonPriorityQueue<>(fileCounter);
  }

  /**
   * This method will be used to get the sorted row in 3-parted format.
   * The row will feed the following writer process step.
   *
   * @return sorted row
   */
  @Override
  public Object[] next() {
    if (hasNext()) {
      return sortStepRowHandler.convertIntermediateSortTempRowTo3Parted(getSortedRecordFromFile());
    } else {
      throw new NoSuchElementException("No more elements to return");
    }
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   */
  private IntermediateSortTempRow getSortedRecordFromFile() throws CarbonDataWriterException {
    IntermediateSortTempRow row = null;

    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    SortTempChunkHolder poll = this.recordHolderHeapLocal.peek();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // if chunk is empty then close the stream
      poll.close();
      recordHolderHeapLocal.poll();

      // change the file counter
      --this.fileCounter;

      // return row
      return row;
    }

    // read new row
    try {
      poll.readRow();
    } catch (Exception e) {
      throw new CarbonDataWriterException(e);
    }

    // maintain heap
    this.recordHolderHeapLocal.siftTopDown();

    // return row
    return row;
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  @Override
  public boolean hasNext() {
    return this.fileCounter > 0;
  }

  public void clear() {
    if (null != recordHolderHeapLocal) {
      for (SortTempChunkHolder pageHolder : recordHolderHeapLocal) {
        pageHolder.close();
      }
      recordHolderHeapLocal = null;
    }
  }

  public boolean isStopProcess() {
    return isStopProcess;
  }

  public void setStopProcess(boolean stopProcess) {
    isStopProcess = stopProcess;
  }
}
