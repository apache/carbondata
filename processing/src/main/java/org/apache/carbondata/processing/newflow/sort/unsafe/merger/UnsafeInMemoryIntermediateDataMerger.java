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

package org.apache.carbondata.processing.newflow.sort.unsafe.merger;

import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeCarbonRowForMerge;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeInmemoryMergeHolder;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

public class UnsafeInMemoryIntermediateDataMerger implements Callable<Void> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeInMemoryIntermediateDataMerger.class.getName());

  /**
   * recordHolderHeap
   */
  private AbstractQueue<UnsafeInmemoryMergeHolder> recordHolderHeap;

  /**
   * fileCounter
   */
  private int holderCounter;

  /**
   * entryCount
   */
  private int entryCount;

  private UnsafeCarbonRowPage[] unsafeCarbonRowPages;

  private long[] mergedAddresses;

  private byte[] rowPageIndexes;

  /**
   * IntermediateFileMerger Constructor
   */
  public UnsafeInMemoryIntermediateDataMerger(UnsafeCarbonRowPage[] unsafeCarbonRowPages,
      int totalSize) {
    this.holderCounter = unsafeCarbonRowPages.length;
    this.unsafeCarbonRowPages = unsafeCarbonRowPages;
    this.mergedAddresses = new long[totalSize];
    this.rowPageIndexes = new byte[totalSize];
    this.entryCount = 0;
  }

  @Override public Void call() throws Exception {
    long intermediateMergeStartTime = System.currentTimeMillis();
    int holderCounterConst = holderCounter;
    boolean isFailed = false;
    try {
      startSorting();
      while (hasNext()) {
        writeDataToMemory(next());
      }
      double intermediateMergeCostTime =
          (System.currentTimeMillis() - intermediateMergeStartTime) / 1000.0;
      LOGGER.info("============================== Intermediate Merge of " + holderCounterConst
          + " in-memory sort Cost Time: " + intermediateMergeCostTime + "(s)");
    } catch (Exception e) {
      LOGGER.error(e, "Problem while intermediate merging");
    }
    return null;
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   * @throws CarbonSortKeyAndGroupByException
   */
  private UnsafeCarbonRowForMerge getSortedRecordFromMemory()
      throws CarbonSortKeyAndGroupByException {
    UnsafeCarbonRowForMerge row = null;

    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    UnsafeInmemoryMergeHolder poll = this.recordHolderHeap.poll();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // change the file counter
      --this.holderCounter;

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
    LOGGER.info("Number of row pages in intermediate merger: " + this.holderCounter);

    // create record holder heap
    createRecordHolderQueue(unsafeCarbonRowPages);

    // iterate over file list and create chunk holder and add to heap
    LOGGER.info("Started adding first record from row page");

    UnsafeInmemoryMergeHolder unsafePageHolder = null;
    byte index = 0;
    for (UnsafeCarbonRowPage unsafeCarbonRowPage : unsafeCarbonRowPages) {
      // create chunk holder
      unsafePageHolder = new UnsafeInmemoryMergeHolder(unsafeCarbonRowPage, index++);

      // initialize
      unsafePageHolder.readRow();

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
    this.recordHolderHeap = new PriorityQueue<UnsafeInmemoryMergeHolder>(pages.length);
  }

  /**
   * This method will be used to get the sorted row
   *
   * @return sorted row
   * @throws CarbonSortKeyAndGroupByException
   */
  private UnsafeCarbonRowForMerge next() throws CarbonSortKeyAndGroupByException {
    return getSortedRecordFromMemory();
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  private boolean hasNext() {
    return this.holderCounter > 0;
  }

  /**
   * Below method will be used to write data to file
   */
  private void writeDataToMemory(UnsafeCarbonRowForMerge row) {
    mergedAddresses[entryCount] = row.address;
    rowPageIndexes[entryCount] = row.index;
    entryCount++;
  }

  public int getEntryCount() {
    return entryCount;
  }

  public UnsafeCarbonRowPage[] getUnsafeCarbonRowPages() {
    return unsafeCarbonRowPages;
  }

  public long[] getMergedAddresses() {
    return mergedAddresses;
  }

  public byte[] getRowPageIndexes() {
    return rowPageIndexes;
  }
}
