/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.metadata.index;

import java.io.Serializable;
import java.util.List;

/**
 * Persist Index of all blocklet in one file
 */
public class BlockletIndex implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 1L;

  /**
   * list of btree index for all blocklets
   */
  private List<BlockletBTreeIndex> btreeIndexList;

  /**
   * list of max and min key of blocklets
   */
  private List<BlockletMinMaxIndex> minMaxIndexList;

  /**
   * @return the btreeIndexList
   */
  public List<BlockletBTreeIndex> getBtreeIndexList() {
    return btreeIndexList;
  }

  /**
   * @param btreeIndexList the btreeIndexList to set
   */
  public void setBtreeIndexList(List<BlockletBTreeIndex> btreeIndexList) {
    this.btreeIndexList = btreeIndexList;
  }

  /**
   * @return the minMaxIndexList
   */
  public List<BlockletMinMaxIndex> getMinMaxIndexList() {
    return minMaxIndexList;
  }

  /**
   * @param minMaxIndexList the minMaxIndexList to set
   */
  public void setMinMaxIndexList(List<BlockletMinMaxIndex> minMaxIndexList) {
    this.minMaxIndexList = minMaxIndexList;
  }
}
