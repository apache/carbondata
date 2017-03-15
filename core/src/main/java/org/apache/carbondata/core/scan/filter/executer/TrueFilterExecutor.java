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

package org.apache.carbondata.core.scan.filter.executer;

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;

public class TrueFilterExecutor implements FilterExecuter {

  /**
   * API will apply filter based on resolver instance
   *
   * @return
   * @throws FilterUnsupportedException
   */
  public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    int numberOfPages = blockChunkHolder.getDataBlock().numberOfPages();
    BitSetGroup group = new BitSetGroup(numberOfPages);
    for (int i = 0; i < numberOfPages; i++) {
      BitSet set = new BitSet();
      set.flip(0, blockChunkHolder.getDataBlock().nodeSize());
      group.setBitSet(set, i);
    }
    return group;
  }

  /**
   * API will verify whether the block can be shortlisted based on block
   * max and min key.
   *
   * @param blockMaxValue, maximum value of the
   * @param blockMinValue
   * @return BitSet
   */
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.flip(0, 1);
    return bitSet;
  }

  /**
   * It just reads necessary block for filter executor, it does not uncompress the data.
   *
   * @param blockChunkHolder
   */
  public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    // do nothing
  }
}
