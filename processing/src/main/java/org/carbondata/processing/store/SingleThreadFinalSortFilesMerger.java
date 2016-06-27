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

package org.carbondata.processing.store;

import java.io.File;
import java.io.FileFilter;
import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.processing.sortandgroupby.sortdata.SortTempFileChunkHolder;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

public class SingleThreadFinalSortFilesMerger {
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

  /**
   * measureCount
   */
  private int measureCount;

  /**
   * dimensionCount
   */
  private int dimensionCount;

  /**
   * measure count
   */
  private int noDictionaryCount;

  /**
   * complexDimensionCount
   */
  private int complexDimensionCount;

  /**
   * tempFileLocation
   */
  private String tempFileLocation;

  private char[] aggType;

  /**
   * below code is to check whether dimension
   * is of no dictionary type or not
   */
  private boolean[] isNoDictionaryColumn;

  public SingleThreadFinalSortFilesMerger(String tempFileLocation, String tableName,
      int dimensionCount, int complexDimensionCount, int measureCount, int noDictionaryCount,
      char[] aggType, boolean[] isNoDictionaryColumn) {
    this.tempFileLocation = tempFileLocation;
    this.tableName = tableName;
    this.dimensionCount = dimensionCount;
    this.complexDimensionCount = complexDimensionCount;
    this.measureCount = measureCount;
    this.aggType = aggType;
    this.noDictionaryCount = noDictionaryCount;
    this.isNoDictionaryColumn = isNoDictionaryColumn;
  }

  /**
   * This method will be used to merger the merged files
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public void startFinalMerge() throws CarbonDataWriterException {
    // get all the merged files
    File file = new File(tempFileLocation);

    File[] fileList = file.listFiles(new FileFilter() {
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(tableName);
      }
    });

    if (null == fileList || fileList.length < 0) {
      return;
    }
    startSorting(fileList);
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startSorting(File[] files) throws CarbonDataWriterException {
    this.fileCounter = files.length;
    this.fileBufferSize = CarbonDataProcessorUtil
        .getFileBufferSize(this.fileCounter, CarbonProperties.getInstance(),
            CarbonCommonConstants.CONSTANT_SIZE_TEN);

    LOGGER.info("Number of temp file: " + this.fileCounter);

    LOGGER.info("File Buffer Size: " + this.fileBufferSize);

    // create record holder heap
    createRecordHolderQueue(files);

    // iterate over file list and create chunk holder and add to heap
    LOGGER.info("Started adding first record from each file");
    int maxThreadForSorting = 0;
    try {
      maxThreadForSorting = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_READER_THREAD,
              CarbonCommonConstants.CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE));
    } catch (NumberFormatException e) {
      maxThreadForSorting =
          Integer.parseInt(CarbonCommonConstants.CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE);
    }
    ExecutorService service = Executors.newFixedThreadPool(maxThreadForSorting);

    for (final File tempFile : files) {

      Callable<Void> runnable = new Callable<Void>() {
        @Override public Void call() throws CarbonSortKeyAndGroupByException {
          // create chunk holder
          SortTempFileChunkHolder sortTempFileChunkHolder =
              new SortTempFileChunkHolder(tempFile, dimensionCount, complexDimensionCount,
                  measureCount, fileBufferSize, noDictionaryCount, aggType, isNoDictionaryColumn);

          // initialize
          sortTempFileChunkHolder.initialize();
          sortTempFileChunkHolder.readRow();

          synchronized (LOCKOBJECT) {
            recordHolderHeapLocal.add(sortTempFileChunkHolder);
          }

          // add to heap
          return null;
        }
      };
      service.submit(runnable);
    }
    service.shutdown();

    try {
      service.awaitTermination(2, TimeUnit.HOURS);
    } catch (Exception e) {
      throw new CarbonDataWriterException(e.getMessage(), e);
    }

    LOGGER.info("Heap Size" + this.recordHolderHeapLocal.size());
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   *
   * @param listFiles list of temp files
   */
  private void createRecordHolderQueue(File[] listFiles) {
    // creating record holder heap
    this.recordHolderHeapLocal = new PriorityQueue<SortTempFileChunkHolder>(listFiles.length);
  }

  /**
   * This method will be used to get the sorted row
   *
   * @return sorted row
   * @throws CarbonSortKeyAndGroupByException
   */
  public Object[] next() throws CarbonDataWriterException {
    return getSortedRecordFromFile();
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getSortedRecordFromFile() throws CarbonDataWriterException {
    Object[] row = null;

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

  public void clear() {
    if (null != recordHolderHeapLocal) {
      recordHolderHeapLocal = null;
    }
  }
}
