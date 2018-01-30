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

import java.util.BitSet;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.util.CarbonUtil;

public class ExcludeFilterExecuterImplTest extends IncludeFilterExecuterImplTest {

 @Override public BitSet setFilterdIndexToBitSetNew(DimensionColumnPage dimColumnDataChunk,
     int numerOfRows, byte[][] filterValues) {
   BitSet bitSet = new BitSet(numerOfRows);
   bitSet.flip(0, numerOfRows);
   // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
   if (filterValues.length > 1) {
     for (int j = 0; j < numerOfRows; j++) {
       int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
           dimColumnDataChunk.getChunkData(j));
       if (index >= 0) {
         bitSet.flip(j);
       }
     }
   } else if (filterValues.length == 1) {
     for (int j = 0; j < numerOfRows; j++) {
       if (dimColumnDataChunk.compareTo(j, filterValues[0]) == 0) {
         bitSet.flip(j);
       }
     }
   }
   return bitSet;
 }

 @Override public BitSet setFilterdIndexToBitSet(DimensionColumnPage dimColumnDataChunk,
      int numerOfRows, byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int k = 0; k < filterValues.length; k++) {
      for (int j = 0; j < numerOfRows; j++) {
        if (dimColumnDataChunk.compareTo(j, filterValues[k]) == 0) {
          bitSet.flip(j);
        }
      }
    }
    return bitSet;
  }
}
