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
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionColumnPage;
import org.apache.carbondata.core.util.CarbonUtil;

import org.junit.Assert;
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

  public BitSet setFilterdIndexToBitSetNew(DimensionColumnPage dimensionColumnPage,
      int numerOfRows, byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnPage instanceof FixedLengthDimensionColumnPage) {
      // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      if (filterValues.length > 1) {
        for (int i = 0; i < numerOfRows; i++) {
          int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
              dimensionColumnPage, i);
          if (index >= 0) {
            bitSet.set(i);
          }
        }
      } else if (filterValues.length == 1) {
        for (int i = 0; i < numerOfRows; i++) {
          if (dimensionColumnPage.compareTo(i, filterValues[0]) == 0) {
            bitSet.set(i);
          }
        }
      }
    }
    return bitSet;
  }

  public BitSet setFilterdIndexToBitSet(DimensionColumnPage dimensionColumnPage, int numerOfRows,
      byte[][] filterValues) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnPage instanceof FixedLengthDimensionColumnPage) {
      // byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      for (int k = 0; k < filterValues.length; k++) {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnPage.compareTo(j, filterValues[k]) == 0) {
            bitSet.set(j);
          }
        }
      }
    }
    return bitSet;
  }

  /**
   * change int to byte[]
   * 
   * @param value
   * @param size
   * @return byte[]
   */
  private byte[] transferIntToByteArr(int value, int size) {
    byte[] targets = new byte[size];
    for (int i = 0; i < size; i++) {
      int data = value;
      for (int j = i; j < size - 1; j++) {
        data = data >> 8;
      }
      data = data & 0xFF;
      targets[i] = (byte) (data & 0xFF);
    }
    return targets;
  }

  @Test
  public void testPerformance() {

    // dimension's data number in a blocklet, usually default is 32000
    int dataChunkSize = 32000; 
    //  repeat query times in the test
    int queryTimes = 5;    
    // repeated times for a dictionary value
    int repeatTimes = 200;
    //filtered value count in a blocklet
    int filteredValueCnt = 800;
    comparePerformance(dataChunkSize, filteredValueCnt, queryTimes, repeatTimes);
    
    filteredValueCnt = 100;
    // set big repeat value for test dimension dictionary size is 1
    repeatTimes = 2000;
    comparePerformance(dataChunkSize, filteredValueCnt, queryTimes, repeatTimes);

  }
  
  /**
   * Tests the filterKeys.length = 0  and filterKeys.length = 1
   */
  @Test
  public void testBoundary() {

	// dimension's data number in a blocklet, usually default is 32000
    int dataChunkSize = 32000; 
    //  repeat query times in the test
    int queryTimes = 5;    
    // repeated times for a dictionary value
    int repeatTimes = 20;
    //filtered value count in a blocklet
    int filteredValueCnt = 1;
    comparePerformance(dataChunkSize, filteredValueCnt, queryTimes, repeatTimes);
    
    filteredValueCnt = 0;
    comparePerformance(dataChunkSize, filteredValueCnt, queryTimes, repeatTimes);

  }

  /**
   * comapre result and performance
   * 
   * @param dataChunkSize dataChunk's stored data size
   * @param filteredValueCnt filtered dictionary value count
   * @param queryTimes repeat query times in the test
   * @param repeatTimes repeated times for a dictionary value
   * @return 
   */
  private void comparePerformance(int dataChunkSize, int filteredValueCnt,
      int queryTimes, int repeatTimes) {
    long start;
    long oldTime = 0;
    long newTime = 0;
    
    // used to generate filter value
    int baseFilteredValue = 100;
    // column dictionary size
    int dimColumnSize = 2;
    if ((dataChunkSize / repeatTimes) <= 255 && (baseFilteredValue+filteredValueCnt) <= 255) {
      dimColumnSize = 1;
    }
    System.out.println("dimColumnSize: " + dimColumnSize);
    
    FixedLengthDimensionColumnPage dimensionColumnDataChunk;
    DimColumnExecuterFilterInfo dim = new DimColumnExecuterFilterInfo();

    byte[] dataChunk = new byte[dataChunkSize * dimColumnSize];
    for (int i = 0; i < dataChunkSize; i++) {
      if (i % repeatTimes == 0) {
        repeatTimes++;
      }
      byte[] data = transferIntToByteArr(repeatTimes, dimColumnSize);      
      for(int j =0 ; j< dimColumnSize;j++){      
        dataChunk[dimColumnSize * i + j] = data[j];
      }
    }

    byte[][] filterKeys = new byte[filteredValueCnt][2];
    for (int k = 0; k < filteredValueCnt; k++) {
      filterKeys[k] = transferIntToByteArr(baseFilteredValue + k, dimColumnSize);
    }
    dim.setFilterKeys(filterKeys);

    dimensionColumnDataChunk = new FixedLengthDimensionColumnPage(dataChunk, null, null,
        dataChunkSize, dimColumnSize);

    // repeat query and compare 2 result between old code and new optimized code
    for (int j = 0; j < queryTimes; j++) {

      start = System.currentTimeMillis();
      BitSet bitOld = this.setFilterdIndexToBitSet(dimensionColumnDataChunk, dataChunkSize, filterKeys);
      oldTime = oldTime + System.currentTimeMillis() - start;

      start = System.currentTimeMillis();
      BitSet bitNew = this.setFilterdIndexToBitSetNew(dimensionColumnDataChunk, dataChunkSize,
          filterKeys);
      newTime = newTime + System.currentTimeMillis() - start;

      assertTrue(bitOld.equals(bitNew));
    }

    if (filteredValueCnt >= 100) {
      Assert.assertTrue(newTime <= oldTime);
    }

    System.out.println("old code performance time: " + oldTime + " ms");
    System.out.println("new code performance time: " + newTime + " ms");
    System.out.println("filteredValueCnt: " + filteredValueCnt);

  }


  private BitSet setFilterdIndexToBitSetWithColumnIndexOld(FixedLengthDimensionColumnPage dimensionColumnDataChunk,
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

  private BitSet setFilterdIndexToBitSetWithColumnIndexNew(FixedLengthDimensionColumnPage dimensionColumnDataChunk,
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

    // dimension's data number in a blocklet, usually default is 32000
    int dataChunkSize = 32000;
    //  repeat query times in the test
    int queryTimes = 10000;
    // repeated times for a dictionary value
    int repeatTimes = 200;
    //filtered value count in a blocklet
    int filteredValueCnt = 800;
    // column dictionary size
    int dimColumnSize = 2;
    FixedLengthDimensionColumnPage dimensionColumnDataChunk;
    DimColumnExecuterFilterInfo dim = new DimColumnExecuterFilterInfo();

    byte[] dataChunk = new byte[dataChunkSize * dimColumnSize];
    for (int i = 0; i < dataChunkSize; i++) {

      if (i % repeatTimes == 0) {
        repeatTimes++;
      }

      byte[] data = transferIntToByteArr(repeatTimes, dimColumnSize);
      dataChunk[2 * i] = data[0];
      dataChunk[2 * i + 1] = data[1];

    }

    byte[][] filterKeys = new byte[filteredValueCnt][2];
    for (int k = 0; k < filteredValueCnt; k++) {
      filterKeys[k] = transferIntToByteArr(100 + k, dimColumnSize);
    }
    dim.setFilterKeys(filterKeys);

    dimensionColumnDataChunk = new FixedLengthDimensionColumnPage(dataChunk, null, null,
        dataChunk.length / dimColumnSize, dimColumnSize);

    // initial to run
    BitSet bitOld = this.setFilterdIndexToBitSetWithColumnIndexOld(dimensionColumnDataChunk, dataChunkSize, filterKeys);
    BitSet bitNew = this.setFilterdIndexToBitSetWithColumnIndexNew(dimensionColumnDataChunk, dataChunkSize, filterKeys);

    // performance run
    for (int j = 0; j < queryTimes; j++) {

      start = System.currentTimeMillis();
      bitOld = this.setFilterdIndexToBitSetWithColumnIndexOld(dimensionColumnDataChunk, dataChunkSize, filterKeys);
      end = System.currentTimeMillis();
      oldTime = oldTime + end - start;

      start = System.currentTimeMillis();
      bitNew = this.setFilterdIndexToBitSetWithColumnIndexNew(dimensionColumnDataChunk, dataChunkSize, filterKeys);
      end = System.currentTimeMillis();
      newTime = newTime + end - start;

      assertTrue(bitOld.equals(bitNew));

    }

    System.out.println("old code performance time: " + oldTime + " ms");
    System.out.println("new code performance time: " + newTime + " ms");

  }

}
