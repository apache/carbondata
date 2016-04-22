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
package org.carbondata.core.carbon.metadata.leafnode.indexes;

import java.io.Serializable;

/**
 * Persist Index of all leaf nodes in one file
 */
public class LeafNodeIndex implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 1L;

  /**
   * list of btree index for all the leaf
   */
  private LeafNodeBtreeIndex btreeIndex;

  /**
   * list of max and min key of all leaf
   */
  private LeafNodeMinMaxIndex minMaxIndex;

  /**
   * @return the btreeIndex
   */
  public LeafNodeBtreeIndex getBtreeIndex() {
    return btreeIndex;
  }

  /**
   * @param btreeIndex the btreeIndex to set
   */
  public void setBtreeIndex(LeafNodeBtreeIndex btreeIndex) {
    this.btreeIndex = btreeIndex;
  }

  /**
   * @return the minMaxIndex
   */
  public LeafNodeMinMaxIndex getMinMaxIndex() {
    return minMaxIndex;
  }

  /**
   * @param minMaxIndex the minMaxIndex to set
   */
  public void setMinMaxIndex(LeafNodeMinMaxIndex minMaxIndex) {
    this.minMaxIndex = minMaxIndex;
  }

}
