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

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.DataRefNode;

/**
 * Below class will be used to iterate over data block
 */
public class BlockletIterator extends CarbonIterator<DataRefNode> {
  /**
   * data store block
   */
  protected DataRefNode datablock;
  /**
   * block counter to keep a track how many block has been processed
   */
  protected int blockCounter;

  /**
   * flag to be used to check any more data block is present or not
   */
  protected boolean hasNext = true;

  /**
   * total number blocks assgned to this iterator
   */
  protected long totalNumberOfBlocksToScan;

  /**
   * Constructor
   *
   * @param datablock                 first data block
   * @param totalNumberOfBlocksToScan total number of blocks to be scanned
   */
  public BlockletIterator(DataRefNode datablock, long totalNumberOfBlocksToScan) {
    this.datablock = datablock;
    this.totalNumberOfBlocksToScan = totalNumberOfBlocksToScan;
  }

  /**
   * is all the blocks assigned to this iterator has been processed
   */
  @Override public boolean hasNext() {
    return hasNext;
  }


  /**
   * To get the next block
   * @return next data block
   *
   */
  @Override public DataRefNode next() {
    // get the current blocks
    DataRefNode datablockTemp = datablock;
    // store the next data block
    datablock = datablock.getNextDataRefNode();
    // increment the counter
    blockCounter++;
    // if all the data block is processed then
    // set the has next flag to false
    // or if number of blocks assigned to this iterator is processed
    // then also set the hasnext flag to false
    if (null == datablock || blockCounter >= this.totalNumberOfBlocksToScan) {
      hasNext = false;
    }
    return datablockTemp;
  }
}
