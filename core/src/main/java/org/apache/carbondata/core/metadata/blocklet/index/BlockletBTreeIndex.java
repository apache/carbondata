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

package org.apache.carbondata.core.metadata.blocklet.index;

import java.io.Serializable;

/**
 * Class hold the information about start and end key of one blocklet
 */
public class BlockletBTreeIndex implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 6116185464700853045L;

  /**
   * Bit-packed start key of one blocklet
   */
  private byte[] startKey;

  /**
   * Bit-packed start key of one blocklet
   */
  private byte[] endKey;

  public BlockletBTreeIndex() {
  }

  public BlockletBTreeIndex(byte[] startKey, byte[] endKey) {
    this.startKey = startKey;
    this.endKey = endKey;
  }

  /**
   * @return the startKey
   */
  public byte[] getStartKey() {
    return startKey;
  }

  /**
   * @param startKey the startKey to set
   */
  public void setStartKey(byte[] startKey) {
    this.startKey = startKey;
  }

  /**
   * @return the endKey
   */
  public byte[] getEndKey() {
    return endKey;
  }

  /**
   * @param endKey the endKey to set
   */
  public void setEndKey(byte[] endKey) {
    this.endKey = endKey;
  }
}
