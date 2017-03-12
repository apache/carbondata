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

import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.util.CarbonUtil;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class IncludeFilterExecuterImplTest extends TestCase {

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {

  }

  private BitSet setFilterdIndexToBitSetNew(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows, byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      if (filterValues.length > 1) {
        for (int i = 0; i < numerOfRows; i++) {
          int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
              dimensionColumnDataChunk.getChunkData(i));
          if (index >= 0) {
            bitSet.set(i);
          }
        }
      } else if (filterValues.length == 1) {
        for (int i = 0; i < numerOfRows; i++) {
          if (dimensionColumnDataChunk.compareTo(i, filterValues[0]) == 0) {
            bitSet.set(i);
          }
        }
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk, int numerOfRows,
      byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      for (int k = 0; k < filterValues.length; k++) {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnDataChunk.compareTo(j, filterValues[k]) == 0) {
            bitSet.set(j);
          }
        }
      }
    }
    return bitSet;
  }

  /**
   * short int to byte
   * 
   * @param s
   *          short int
   * @return byte[]
   */
  private byte[] unsignedShortToByte2(int s) {
    byte[] targets = new byte[2];
    targets[0] = (byte) (s >> 8 & 0xFF);
    targets[1] = (byte) (s & 0xFF);
    return targets;
  }

  @Test
  public void testPerformance() {

    int dataCnt = 120000;
    int queryCnt = 5;
    int repeatCnt = 200;
    
    int filteredValueCnt = 800;
    comparePerformance(dataCnt, filteredValueCnt, queryCnt, repeatCnt);
    
    filteredValueCnt = 100;
    comparePerformance(dataCnt, filteredValueCnt, queryCnt, repeatCnt);

  }
  
  /**
   * Tests the filterKeys.length = 0  and filterKeys.length = 1
   */
  @Test
  public void testBoundary() {

	// dimension's data number in a blocklet, usually default is 120000
    int dataChunkSize = 120000; 
    //  repeat query count in the test
    int queryCnt = 5;    
    // use to generate repeated dictionary value
    int repeatCnt = 20;
    //filtered value count in a blocklet
    int filteredValueCnt = 1;
    comparePerformance(dataChunkSize, filteredValueCnt, queryCnt, repeatCnt);
    
    filteredValueCnt = 0;
    comparePerformance(dataChunkSize, filteredValueCnt, queryCnt, repeatCnt);

  }

  /**
   * comapre result and performance
   * 
   * @param dataChunkSize dataChunk's stored data size
   * @param filteredValueCnt filtered dictionary value count
   * @param queryCnt repeat query count in the test
   * @param repeatCnt use to generate repeated test dictionary value
   * @return 
   */
  private void comparePerformance(int dataChunkSize, int filteredValueCnt,
      int queryCnt, int repeatCnt) {
    long start;
    long oldTime = 0;
    long newTime = 0;
    
    // column size
    int dimColumnSize = 2;
    FixedLengthDimensionDataChunk dimensionColumnDataChunk;
    DimColumnExecuterFilterInfo dim = new DimColumnExecuterFilterInfo();

    byte[] dataChunk = new byte[dataChunkSize * dimColumnSize];
    for (int i = 0; i < dataChunkSize; i++) {

      if (i % repeatCnt == 0) {
        repeatCnt++;
      }

      byte[] data = unsignedShortToByte2(repeatCnt);
      dataChunk[2 * i] = data[0];
      dataChunk[2 * i + 1] = data[1];

    }

    byte[][] filterKeys = new byte[filteredValueCnt][2];
    for (int ii = 0; ii < filteredValueCnt; ii++) {
      filterKeys[ii] = unsignedShortToByte2(100 + ii);
    }
    dim.setFilterKeys(filterKeys);

    dimensionColumnDataChunk = new FixedLengthDimensionDataChunk(dataChunk, null, null,
        dataChunkSize, dimColumnSize);

    // repeat query and compare 2 result between old code and new optimized code
    for (int j = 0; j < queryCnt; j++) {

      start = System.currentTimeMillis();
      BitSet bitOld = this.setFilterdIndexToBitSet(dimensionColumnDataChunk, dataChunkSize, filterKeys);
      oldTime = oldTime + System.currentTimeMillis() - start;

      start = System.currentTimeMillis();
      BitSet bitNew = this.setFilterdIndexToBitSetNew((FixedLengthDimensionDataChunk) dimensionColumnDataChunk, dataChunkSize,
          filterKeys);
      newTime = newTime + System.currentTimeMillis() - start;

      assertTrue(bitOld.equals(bitNew));

    }

    if (filteredValueCnt >= 100) {
      assertTrue(newTime < oldTime);
    }

    System.out.println("old code performance time: " + oldTime + " ms");
    System.out.println("new code performance time: " + newTime + " ms");

  }


  private BitSet setFilterdIndexToBitSetWithColumnIndexOld(FixedLengthDimensionDataChunk dimensionColumnDataChunk,
      int numerOfRows, byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    int start = 0;
    int last = 0;
    int startIndex = 0;
    // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      start = CarbonUtil.getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
          filterValues[i], false);
      if (start < 0) {
        continue;
      }
      bitSet.set(start);
      last = start;
      for (int j = start + 1; j < numerOfRows; j++) {
        if (dimensionColumnDataChunk.compareTo(j, filterValues[i]) == 0) {
          bitSet.set(j);
          last++;
        } else {
          break;
        }
      }
      startIndex = last;
      if (startIndex >= numerOfRows) {
        break;
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSetWithColumnIndexNew(FixedLengthDimensionDataChunk dimensionColumnDataChunk,
      int numerOfRows, byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    int startIndex = 0;
    // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      int[] rangeIndex = CarbonUtil.getRangeIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex,
          numerOfRows - 1, filterValues[i]);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {

        bitSet.set(j);
      }

      if (rangeIndex[1] > -1) {
        startIndex = rangeIndex[1];
      }
    }
    return bitSet;
  }

  @Test
  public void testRangBinarySearch() {

    long oldTime = 0;
    long newTime = 0;
    long start;
    long end;
    int dataCnt = 120000;
    int filterCnt = 800;
    int queryCnt = 10000;
    int repeatCnt = 200;
    byte[] keyWord = new byte[2];
    FixedLengthDimensionDataChunk dimensionColumnDataChunk;
    DimColumnExecuterFilterInfo dim = new DimColumnExecuterFilterInfo();

    byte[] dataChunk = new byte[dataCnt * keyWord.length];
    for (int i = 0; i < dataCnt; i++) {

      if (i % repeatCnt == 0) {
        repeatCnt++;
      }

      byte[] data = unsignedShortToByte2(repeatCnt);
      dataChunk[2 * i] = data[0];
      dataChunk[2 * i + 1] = data[1];

    }

    byte[][] filterKeys = new byte[filterCnt][2];
    for (int ii = 0; ii < filterCnt; ii++) {
      filterKeys[ii] = unsignedShortToByte2(100 + ii);
    }
    dim.setFilterKeys(filterKeys);

    dimensionColumnDataChunk = new FixedLengthDimensionDataChunk(dataChunk, null, null,
        dataChunk.length / keyWord.length, keyWord.length);

    // initial to run
    BitSet bitOld = this.setFilterdIndexToBitSetWithColumnIndexOld(dimensionColumnDataChunk, dataCnt, filterKeys);
    BitSet bitNew = this.setFilterdIndexToBitSetWithColumnIndexNew(dimensionColumnDataChunk, dataCnt, filterKeys);

    // performance run
    for (int j = 0; j < queryCnt; j++) {

      start = System.currentTimeMillis();
      bitOld = this.setFilterdIndexToBitSetWithColumnIndexOld(dimensionColumnDataChunk, dataCnt, filterKeys);
      end = System.currentTimeMillis();
      oldTime = oldTime + end - start;

      start = System.currentTimeMillis();
      bitNew = this.setFilterdIndexToBitSetWithColumnIndexNew(dimensionColumnDataChunk, dataCnt, filterKeys);
      end = System.currentTimeMillis();
      newTime = newTime + end - start;

      assertTrue(bitOld.equals(bitNew));

    }

    System.out.println("old code performance time: " + oldTime + " ms");
    System.out.println("new code performance time: " + newTime + " ms");

  }

}
