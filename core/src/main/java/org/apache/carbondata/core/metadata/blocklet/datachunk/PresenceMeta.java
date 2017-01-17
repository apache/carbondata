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

package org.apache.carbondata.core.metadata.blocklet.datachunk;

import java.util.BitSet;

/**
 * information about presence of values in each row of the column chunk
 */
public class PresenceMeta {

  /**
   * if true, ones in the bit stream reprents presence. otherwise represents absence
   */
  private boolean representNullValues;

  /**
   * Compressed bit stream representing the presence of null values
   */
  private BitSet bitSet;

  /**
   * @return the representNullValues
   */
  public boolean isRepresentNullValues() {
    return representNullValues;
  }

  /**
   * @param representNullValues the representNullValues to set
   */
  public void setRepresentNullValues(boolean representNullValues) {
    this.representNullValues = representNullValues;
  }

  /**
   * @return the bitSet
   */
  public BitSet getBitSet() {
    return bitSet;
  }

  /**
   * @param bitSet the bitSet to set
   */
  public void setBitSet(BitSet bitSet) {
    this.bitSet = bitSet;
  }
}
