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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * VO object which contains the info used in CDC case during cache loading in Index server
 */
public class CdcVO implements Serializable, Writable {

  /**
   * This collection contains column to index mapping which give info about the index for a column
   * in IndexRow object to fetch min max
   */
  private Map<String, Integer> columnToIndexMap;

  /**
   * This list will contain the column indexes to fetch from the min max row in blocklet
   */
  private List<Integer> indexesToFetch;

  public CdcVO(Map<String, Integer> columnToIndexMap) {
    this.columnToIndexMap = columnToIndexMap;
  }

  public CdcVO() {
  }

  public Map<String, Integer> getColumnToIndexMap() {
    return columnToIndexMap;
  }

  public List<Integer> getIndexesToFetch() {
    return indexesToFetch;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Collection<Integer> indexesToFetch = columnToIndexMap.values();
    dataOutput.writeInt(indexesToFetch.size());
    for (Integer index : indexesToFetch) {
      dataOutput.writeInt(index);
      dataOutput.writeInt(index);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.indexesToFetch = new ArrayList<>();
    int lengthOfIndexes = dataInput.readInt();
    for (int i = 0; i < lengthOfIndexes; i++) {
      indexesToFetch.add(dataInput.readInt());
    }
  }
}
