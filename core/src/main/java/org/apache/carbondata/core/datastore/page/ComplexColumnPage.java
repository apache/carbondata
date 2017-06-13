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

package org.apache.carbondata.core.datastore.page;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;


// Represent a complex column page, e.g. Array, Struct type column
public class ComplexColumnPage {

  // Holds data for all rows in this page in columnar layout.
  // After the complex data expand, it is of type byte[][], the first level array in the byte[][]
  // representing a sub-column in the complex type, which can be retrieved by giving the depth
  // of the complex type.
  // TODO: further optimize it to make it more memory efficient
  private List<ArrayList<byte[]>> complexColumnData;

  // depth is the number of column after complex type is expanded. It is from 1 to N
  private final int depth;

  private final int pageSize;

  public ComplexColumnPage(int pageSize, int depth) {
    this.pageSize = pageSize;
    this.depth = depth;
    complexColumnData = new ArrayList<>(depth);
    for (int i = 0; i < depth; i++) {
      complexColumnData.add(new ArrayList<byte[]>(pageSize));
    }
  }

  public void putComplexData(int rowId, int depth, List<byte[]> value) {
    assert (depth <= this.depth);
    ArrayList<byte[]> subColumnPage = complexColumnData.get(depth);
    subColumnPage.addAll(value);
  }

  // iterate on the sub-column after complex type is expanded, return columnar page of
  // each sub-column
  public Iterator<byte[][]> iterator() {

    return new CarbonIterator<byte[][]>() {
      private int index = 0;
      @Override public boolean hasNext() {
        return index < depth;
      }

      @Override public byte[][] next() {
        // convert the subColumnPage from ArrayList<byte[]> to byte[][]
        ArrayList<byte[]> subColumnPage = complexColumnData.get(index);
        index++;
        return subColumnPage.toArray(new byte[subColumnPage.size()][]);
      }
    };
  }

  public int getDepth() {
    return depth;
  }

}
