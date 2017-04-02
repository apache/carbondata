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

package org.apache.carbondata.core.scan.processor;

import java.util.List;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.scan.model.SortOrderType;

/**
 * Below class will be used to iterate over data block
 */
public class SortMdkBlockletIterator extends BlockletIterator {

  private SortOrderType sortType;
  private List<DataRefNode> datablocks;

  /**
   * Constructor
   *
   * @param datablocks
   *          datablock list
   * @param sortType
   *          sort order type
   * @param totalNumberOfBlocksToScan
   *          total number of blocks to be scanned
   */
  public SortMdkBlockletIterator(List<DataRefNode> datablocks, SortOrderType sortType) {
    super(SortOrderType.ASC.equals(sortType) ? datablocks.get(0)
        : datablocks.get(datablocks.size() - 1), datablocks.size());
    this.sortType = sortType;

    if (SortOrderType.DSC.equals(sortType)) {
      this.datablocks = datablocks;
      this.blockCounter = datablocks.size() - 1;
    }

  }

  @Override
  /**
   * To get the next block
   *
   * @return next data block
   *
   */
  public DataRefNode next() {
    if (SortOrderType.ASC.equals(sortType)) {
      return super.next();
    }
    // get the current blocks
    DataRefNode datablockTemp = datablock;

    // increment the counter
    blockCounter--;
    if (blockCounter < 0) {
      hasNext = false;
    } else {
      // store the next data block
      datablock = datablocks.get(blockCounter);
      // if all the data block is processed then
      // set the has next flag to false
      // or if number of blocks assigned to this iterator is processed
      // then also set the hasnext flag to false
      if (null == datablock) {
        hasNext = false;
      }
    }

    return datablockTemp;
  }
}
