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
package org.apache.carbondata.core.datastore.block;

import java.io.Serializable;

/**
 * The class holds the blocks blocklets info
 */
public class BlockletInfos implements Serializable {
  /**
   * no of blockLets
   */
  private int noOfBlockLets = 0;

  /**
   * start blocklet number
   */
  private int startBlockletNumber;
  /**
   * end blocklet number
   */
  private int numberOfBlockletToScan;
  /**
   * default constructor
   */
  BlockletInfos() {
  }
  /**
   * constructor to initialize the blockletinfo
   * @param noOfBlockLets
   * @param startBlockletNumber
   * @param numberOfBlockletToScan
   */
  public BlockletInfos(int noOfBlockLets, int startBlockletNumber, int numberOfBlockletToScan) {
    this.noOfBlockLets = noOfBlockLets;
    this.startBlockletNumber = startBlockletNumber;
    this.numberOfBlockletToScan = numberOfBlockletToScan;
  }

  /**
   * returns the number of blockLets
   *
   * @return
   */
  public int getNoOfBlockLets() {
    return noOfBlockLets;
  }

  /**
   * sets the number of blockLets
   *
   * @param noOfBlockLets
   */
  public void setNoOfBlockLets(int noOfBlockLets) {
    this.noOfBlockLets = noOfBlockLets;
  }

  /**
   * returns start blocklet number
   *
   * @return
   */
  public int getStartBlockletNumber() {
    return startBlockletNumber;
  }

  /**
   * returns end blocklet number
   *
   * @return
   */
  public int getNumberOfBlockletToScan() {
    return numberOfBlockletToScan;
  }

}

