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

package org.apache.carbondata.core.datastore.columnar;

import java.util.concurrent.Callable;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.processing.store.colgroup.ColGroupDataHolder;
import org.apache.carbondata.processing.store.colgroup.ColGroupMinMax;

/**
 * it is holder of column group data and also min max for colgroup block data
 */
public class ColGroupBlockStorage implements IndexStorage, Callable<IndexStorage> {

  private byte[][] data;

  private ColGroupMinMax colGrpMinMax;

  public ColGroupBlockStorage(SegmentProperties segmentProperties, int colGrpIndex, byte[][] data) {
    colGrpMinMax = new ColGroupMinMax(segmentProperties, colGrpIndex);
    this.data = data;
    for (int i = 0; i < data.length; i++) {
      colGrpMinMax.add(data[i]);
    }
  }

  /**
   * sorting is not required for colgroup storage and hence return true
   */
  @Override public boolean isAlreadySorted() {
    return true;
  }

  /**
   * for column group storage its not required
   */
  @Override public ColGroupDataHolder getDataAfterComp() {
    //not required for column group storage
    return null;
  }

  /**
   * for column group storage its not required
   */
  @Override public ColGroupDataHolder getIndexMap() {
    // not required for column group storage
    return null;
  }

  /**
   * for column group storage its not required
   */
  @Override public byte[][] getKeyBlock() {
    return data;
  }

  /**
   * for column group storage its not required
   */
  @Override public ColGroupDataHolder getDataIndexMap() {
    //not required for column group
    return null;
  }

  /**
   * for column group storage its not required
   */
  @Override public int getTotalSize() {
    return data.length;
  }

  @Override public byte[] getMin() {
    return colGrpMinMax.getMin();
  }

  @Override public byte[] getMax() {
    return colGrpMinMax.getMax();
  }

  /**
   * return self
   */
  @Override public IndexStorage call() throws Exception {
    return this;
  }
}