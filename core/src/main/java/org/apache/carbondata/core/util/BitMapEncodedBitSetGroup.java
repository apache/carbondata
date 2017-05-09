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
package org.apache.carbondata.core.util;

/**
 * Maintains the group of bitsets. Each filter executor returns BitSetGroup
 * after filtering the data.
 */
public class BitMapEncodedBitSetGroup extends BitSetGroup {

  public BitMapEncodedBitSetGroup(int groupSize) {
    super(groupSize);
  }

  /**
   * @return return the valid pages
   */
  public int getBitSetIndex(int rowId) {
    for (int bitSetIndex = 0; bitSetIndex < bitSets.length; bitSetIndex++) {
      if (bitSets[bitSetIndex] != null && bitSets[bitSetIndex].get(rowId)) {
        return bitSetIndex;
      }
    }
    throw new RuntimeException("wrong data");
  }
}
