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
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * API will apply filter based on resolver instance
 *
 * @return
 * @throws FilterUnsupportedException
 */
public class FalseFilterExecutor implements FilterExecuter {

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawChunks, boolean useBitsetPipeline)
      throws FilterUnsupportedException, IOException {
    int numberOfPages = rawChunks.getDataBlock().numberOfPages();
    BitSetGroup group = new BitSetGroup(numberOfPages);
    for (int i = 0; i < numberOfPages; i++) {
      BitSet set = new BitSet();
      group.setBitSet(set, i);
    }
    return group;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawChunks)
      throws FilterUnsupportedException, IOException {
    int numberOfPages = rawChunks.getDataBlock().numberOfPages();
    BitSet set = new BitSet(numberOfPages);
    return set;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    return false;
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    return new BitSet();
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks blockChunkHolder) {
    // Do Nothing
  }
}
