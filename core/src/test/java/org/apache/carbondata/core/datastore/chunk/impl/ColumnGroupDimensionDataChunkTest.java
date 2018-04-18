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

package org.apache.carbondata.core.datastore.chunk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ColumnGroupDimensionDataChunkTest {

  static ColumnGroupDimensionColumnPage columnGroupDimensionDataChunk;
  static KeyGenerator keyGenerator;

  @BeforeClass public static void setup() {
    int[] bitLength = CarbonUtil.getDimensionBitLength(new int[] { 10, 10, 10 }, new int[] { 3 });
    // create a key generator
    keyGenerator = new MultiDimKeyVarLengthGenerator(bitLength);
    byte[] data = new byte[keyGenerator.getKeySizeInBytes() * 3];
    int position = 0;
    for (int i = 1; i <= 3; i++) {
      try {
        System.arraycopy(keyGenerator.generateKey(new int[] { i, i, i }), 0, data, position,
            keyGenerator.getKeySizeInBytes());
      } catch (KeyGenException e) {
        assertTrue(false);
      }
      position += keyGenerator.getKeySizeInBytes();
    }
    columnGroupDimensionDataChunk =
        new ColumnGroupDimensionColumnPage(data, keyGenerator.getKeySizeInBytes(), 3);
  }

  @Test public void fillChunkDataTest() {
    List<Integer> ordinals = new ArrayList<Integer>();
    ordinals.add(1);
    KeyStructureInfo keyStructureInfo = getKeyStructureInfo(ordinals, keyGenerator);
    byte[] buffer = new byte[1];
    columnGroupDimensionDataChunk.fillRawData(1, 0, buffer, keyStructureInfo);
    assertEquals(buffer[0], 2);
  }

  @Test public void getChunkDataTest() {
    byte[] b = { 34, 2 };
    byte res[] = columnGroupDimensionDataChunk.getChunkData(1);
    Assert.assertTrue(Arrays.equals(res, b));
  }

  @Test public void fillConvertedChunkDataTest() {
    int[] row = new int[3];
    int[] expected = { 0, 0, 3 };
    List<Integer> ordinals = new ArrayList<Integer>();
    ordinals.add(2);
    KeyStructureInfo keyStructureInfo = getKeyStructureInfo(ordinals, keyGenerator);
    keyStructureInfo.setMdkeyQueryDimensionOrdinal(new int[] { 2 });
    int res = columnGroupDimensionDataChunk.fillSurrogateKey(2, 2, row, keyStructureInfo);
    Assert.assertTrue(Arrays.equals(row, expected));
  }

  /**
   * Below method will be used to get the key structure info for the query
   *
   * @param ordinals   query model
   * @param keyGenerator
   * @return key structure info
   */
  private KeyStructureInfo getKeyStructureInfo(List<Integer> ordinals, KeyGenerator keyGenerator) {
    // getting the masked byte range for dictionary column
    int[] maskByteRanges = QueryUtil.getMaskedByteRangeBasedOrdinal(ordinals, keyGenerator);

    // getting the masked bytes for query dimension dictionary column
    int[] maskedBytes = QueryUtil.getMaskedByte(keyGenerator.getKeySizeInBytes(), maskByteRanges);

    // max key for the dictionary dimension present in the query
    byte[] maxKey = null;
    try {
      // getting the max key which will be used to masked and get the
      // masked key
      maxKey = QueryUtil.getMaxKeyBasedOnOrinal(ordinals, keyGenerator);
    } catch (KeyGenException e) {
    }

    KeyStructureInfo restructureInfos = new KeyStructureInfo();
    restructureInfos.setKeyGenerator(keyGenerator);
    restructureInfos.setMaskByteRanges(maskByteRanges);
    restructureInfos.setMaxKey(maxKey);
    return restructureInfos;
  }
}

