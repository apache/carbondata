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

package org.carbondata.query.carbon.result.impl;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Below class is a holder over list based result wrapper
 */
public class ListBasedResult implements Result<List<ListBasedResultWrapper>> {

  /**
   * current result list
   */
  private List<ListBasedResultWrapper> currentRowPointer;

  /**
   * all result list , this is required because if we merger all the scanned
   * result from all the blocks in one list, that list creation will take more
   * time as every time list will create a big array and then it will do copy
   * the older element to new array, and creation of big array will also be a
   * problem if memory is fragmented then jvm in to do defragmentation to
   * create a big space, but if divide the data in multiple list than it avoid
   * copy and defragmentation
   */
  private List<List<ListBasedResultWrapper>> allRowsResult;

  /**
   * counter to check how many result processed
   */
  private int totalRecordCounter = -1;

  /**
   * number of records
   */
  private int totalNumberOfRecords;

  /**
   * current counter of the record in list
   */
  private int listRecordCounter = -1;

  /**
   * current list counter
   */
  private int currentListCounter;

  public ListBasedResult() {
    currentRowPointer =
        new ArrayList<ListBasedResultWrapper>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    allRowsResult =
        new ArrayList<List<ListBasedResultWrapper>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * below method will be used to add the scan result
   */
  @Override public void addScannedResult(List<ListBasedResultWrapper> listBasedResult) {
    this.currentRowPointer = listBasedResult;
    totalNumberOfRecords = listBasedResult.size();
  }

  /**
   * Method to check more result is present
   * or not
   */
  @Override public boolean hasNext() {
    if (allRowsResult.size() == 0) {
      return false;
    }
    // As we are storing data in list of list, below code is to check whether
    // any more result is present
    // in the result.
    // first it will check list counter is zero if it is zero
    // than it will check list counter to check how many list has been processed
    // if more list are present and all the list of current list is processed
    // than it will take a new list from all row result list
    totalRecordCounter++;
    listRecordCounter++;
    if (listRecordCounter == 0 || (listRecordCounter >= currentRowPointer.size()
        && currentListCounter < allRowsResult.size())) {
      listRecordCounter = 0;
      currentRowPointer = allRowsResult.get(currentListCounter);
      currentListCounter++;
    }
    return totalRecordCounter < totalNumberOfRecords;
  }

  /**
   * @return key
   */
  @Override public ByteArrayWrapper getKey() {
    return currentRowPointer.get(listRecordCounter).getKey();
  }

  /**
   * @return will return the value
   */
  @Override public MeasureAggregator[] getValue() {
    return currentRowPointer.get(listRecordCounter).getValue();
  }

  /***
   * below method will be used to merge the
   * scanned result
   *
   * @param otherResult return to be merged
   */
  @Override public void merge(Result<List<ListBasedResultWrapper>> otherResult) {
    if (otherResult.size() > 0) {
      totalNumberOfRecords += otherResult.size();
      this.allRowsResult.add(otherResult.getResult());
    }
  }

  /**
   * Return the size of the result
   */
  @Override public int size() {
    return totalNumberOfRecords;
  }

  /**
   * @return the complete result
   */
  @Override public List<ListBasedResultWrapper> getResult() {
    return currentRowPointer;
  }
}
