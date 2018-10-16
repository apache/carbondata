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
package org.apache.carbondata.core.mutate;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Class which keep the information about the rows
 * while got deleted
 */
public class DeleteDeltaVo {

  /**
   * deleted rows bitset
   */
  private BitSet bitSet;

  public DeleteDeltaVo() {
    bitSet = new BitSet();
  }

  /**
   * Below method will be used to insert the rows
   * which are deleted
   *
   * @param data
   */
  public void insertData(Set<Integer> data) {
    Iterator<Integer> iterator = data.iterator();
    while (iterator.hasNext()) {
      bitSet.set(iterator.next());
    }
  }

  /**
   * below method will be used to check the row is deleted or not
   *
   * @param counter
   * @return
   */
  public boolean containsRow(int counter) {
    return bitSet.get(counter);
  }

  public BitSet getBitSet() {
    return bitSet;
  }
}
