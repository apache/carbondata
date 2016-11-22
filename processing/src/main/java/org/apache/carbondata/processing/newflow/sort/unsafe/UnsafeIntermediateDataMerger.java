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

import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

public class UnsafeIntermediateDataMerger implements Callable<Void> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeIntermediateDataMerger.class.getName());

  /**
   * recordHolderHeap
   */
  private AbstractQueue<UnsafePageHolder> recordHolderHeap;

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * totalNumberOfRecords
   */
  private int totalNumberOfRecords;


  /**
   * entryCount
   */
  private int entryCount;


  /**
   * totalSize
   */
  private int totalSize;

  private SortParameters mergerParameters;

  private UnsafeCarbonRowPage[] unsafeCarbonRowPages;

  private UnsafeCarbonRowPage outPutPage;

  private boolean useKettle;

  private boolean[] noDictionarycolumnMapping;

  /**
   * IntermediateFileMerger Constructor
   */
  public UnsafeIntermediateDataMerger(SortParameters mergerParameters,
      UnsafeCarbonRowPage[] unsafeCarbonRowPages, UnsafeCarbonRowPage outPutPage) {
    this.mergerParameters = mergerParameters;
    this.fileCounter = unsafeCarbonRowPages.length;
    this.unsafeCarbonRowPages = unsafeCarbonRowPages;
    this.outPutPage = outPutPage;
    this.useKettle = mergerParameters.isUseKettle();
    noDictionarycolumnMapping = mergerParameters.getNoDictionaryDimnesionColumn();
  }

  @Override public Void call() throws Exception {
    long intermediateMergeStartTime = System.currentTimeMillis();
    int fileConterConst = fileCounter;
    boolean isFailed = false;
    try {
      startSorting();
      while (hasNext()) {
        writeDataTofileWithOutKettle(next());
      }
      double intermediateMergeCostTime =
          (System.currentTimeMillis() - intermediateMergeStartTime) / 1000.0;
      LOGGER.info("============================== Intermediate Merge of " + fileConterConst
          + " Sort Temp Files Cost Time: " + intermediateMergeCostTime + "(s)");
    } catch (Exception e) {
      LOGGER.error(e, "Problem while intermediate merging");
      isFailed = true;
    } finally {
      if (!isFailed) {
        try {
          finish();
        } catch (CarbonSortKeyAndGroupByException e) {
          LOGGER.error(e, "Problem while deleting the merge file");
        }
      } else {
        outPutPage.freeMemory();
      }
    }

    return null;
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getSortedRecordFromMemory() throws CarbonSortKeyAndGroupByException {
    Object[] row = null;

    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    UnsafePageHolder poll = this.recordHolderHeap.poll();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // if chunk is empty then close the stream
      poll.freeMemory();

      // change the file counter
      --this.fileCounter;

      // reaturn row
      return row;
    }

    // read new row
    poll.readRow();

    // add to heap
    this.recordHolderHeap.add(poll);

    // return row
    return row;
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startSorting() throws CarbonSortKeyAndGroupByException {
    LOGGER.info("Number of temp file: " + this.fileCounter);

    // create record holder heap
    createRecordHolderQueue(unsafeCarbonRowPages);

    // iterate over file list and create chunk holder and add to heap
    LOGGER.info("Started adding first record from each file");

    UnsafePageHolder unsafePageHolder = null;

    for (UnsafeCarbonRowPage unsafeCarbonRowPage : unsafeCarbonRowPages) {
      // create chunk holder
      unsafePageHolder = new UnsafePageHolder(unsafeCarbonRowPage,
          mergerParameters.getDimColCount() + mergerParameters.getMeasureColCount());

      // initialize
      unsafePageHolder.readRow();
      this.totalNumberOfRecords += unsafePageHolder.numberOfRows();

      // add to heap
      this.recordHolderHeap.add(unsafePageHolder);
    }

    LOGGER.info("Heap Size" + this.recordHolderHeap.size());
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   */
  private void createRecordHolderQueue(UnsafeCarbonRowPage[] pages) {
    // creating record holder heap
    this.recordHolderHeap = new PriorityQueue<UnsafePageHolder>(pages.length);
  }

  /**
   * This method will be used to get the sorted row
   *
   * @return sorted row
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] next() throws CarbonSortKeyAndGroupByException {
    return getSortedRecordFromMemory();
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  private boolean hasNext() {
    return this.fileCounter > 0;
  }

  /**
   * Below method will be used to write data to file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataTofileWithOutKettle(Object[] row) throws CarbonSortKeyAndGroupByException {
    try {
      outPutPage.addRow(row);

    } catch (Exception e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    }
  }

  private void finish() throws CarbonSortKeyAndGroupByException {
    if (recordHolderHeap != null) {
      int size = recordHolderHeap.size();
      for (int i = 0; i < size; i++) {
        recordHolderHeap.poll().freeMemory();
      }
    }
  }

}
