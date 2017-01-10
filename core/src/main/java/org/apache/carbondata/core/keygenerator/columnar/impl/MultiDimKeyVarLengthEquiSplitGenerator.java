/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.keygenerator.columnar.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;

/**
 * It is Equi Split implementation class of Columnar splitter. And uses var key length
 * generator to generate keys.
 * It splits depends on the @dimensionsToSplit parameter. This parameter decides how many
 * dimensions should be present in each column.
 */
public class MultiDimKeyVarLengthEquiSplitGenerator extends MultiDimKeyVarLengthGenerator
    implements ColumnarSplitter {

  private static final long serialVersionUID = -7767757692821917570L;

  private byte dimensionsToSplit;

  private int[] blockKeySize;

  public MultiDimKeyVarLengthEquiSplitGenerator(int[] lens, byte dimensionsToSplit) {
    super(lens);
    this.dimensionsToSplit = dimensionsToSplit;
    intialize();
  }

  private void intialize() {
    byte s = 0;
    List<Set<Integer>> splitList =
        new ArrayList<Set<Integer>>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    Set<Integer> split = new TreeSet<Integer>();
    splitList.add(split);
    for (int i = 0; i < byteRangesForKeys.length; i++) {
      if (s == dimensionsToSplit) {
        s = 0;
        split = new TreeSet<Integer>();
        splitList.add(split);
      }
      for (int j = 0; j < byteRangesForKeys[i].length; j++) {
        for (int j2 = byteRangesForKeys[i][0]; j2 <= byteRangesForKeys[i][1]; j2++) {
          split.add(j2);
        }
      }
      s++;
    }
    List<Integer>[] splits = new List[splitList.size()];
    int i = 0;
    for (Set<Integer> splitLocal : splitList) {
      List<Integer> range = new ArrayList<Integer>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      for (Integer index : splitLocal) {
        range.add(index);
      }
      splits[i++] = range;
    }
    for (int j = 1; j < splits.length; j++) {
      if (Objects.equals(splits[j - 1].get(splits[j - 1].size() - 1), splits[j].get(0))) {
        splits[j].remove(0);
      }
    }
    int[][] splitDimArray = new int[splits.length][];
    for (int j = 0; j < splits.length; j++) {
      int[] a = convertToArray(splits[j]);
      splitDimArray[j] = a.length > 0 ? new int[] { a[0], a[a.length - 1] } : a;
    }

    int[][] dimBlockArray = new int[byteRangesForKeys.length][];
    Set<Integer>[] dimBlockSet = new Set[dimBlockArray.length];
    for (int k = 0; k < byteRangesForKeys.length; k++) {
      int[] dimRange = byteRangesForKeys[k];
      Set<Integer> dimBlockPosSet = new TreeSet<Integer>();
      dimBlockSet[k] = dimBlockPosSet;
      for (int j = 0; j < splitDimArray.length; j++) {
        if (dimRange[0] >= splitDimArray[j][0] && dimRange[0] <= splitDimArray[j][1]) {
          dimBlockPosSet.add(j);
        }
        if (dimRange[1] >= splitDimArray[j][0] && dimRange[1] <= splitDimArray[j][1]) {
          dimBlockPosSet.add(j);
        }
      }

    }

    for (int j = 0; j < dimBlockSet.length; j++) {
      dimBlockArray[j] = convertToArray(dimBlockSet[j]);
    }

    int[][] splitDimArrayLocalIndexes = new int[splitDimArray.length][];
    for (int j = 0; j < splitDimArrayLocalIndexes.length; j++) {
      splitDimArrayLocalIndexes[j] = splitDimArray[j].length > 0 ?
          new int[] { 0, splitDimArray[j][1] - splitDimArray[j][0] } :
          new int[0];
    }

    int[][][] byteRangesForDims = new int[byteRangesForKeys.length][][];
    for (int j = 0; j < byteRangesForKeys.length; j++) {
      if (dimBlockArray[j].length > 1) {
        int[] bArray1 = splitDimArrayLocalIndexes[dimBlockArray[j][0]];
        byteRangesForDims[j] = new int[2][2];
        byteRangesForDims[j][0] =
            new int[] { bArray1[bArray1.length - 1], bArray1[bArray1.length - 1] };
        byteRangesForDims[j][1] = new int[] { 0,
            (byteRangesForKeys[j][byteRangesForKeys[j].length - 1] - byteRangesForKeys[j][0]) - 1 };
      } else {
        byteRangesForDims[j] = new int[1][1];
        int[] bArray1 = splitDimArray[dimBlockArray[j][0]];
        byteRangesForDims[j][0] = new int[] { byteRangesForKeys[j][0] - bArray1[0],
            byteRangesForKeys[j][1] - bArray1[0] };
      }
    }
    blockKeySize = new int[splitDimArray.length];

    for (int j = 0; j < blockKeySize.length; j++) {
      blockKeySize[j] =
          splitDimArray[j].length > 0 ? splitDimArray[j][1] - splitDimArray[j][0] + 1 : 0;
    }
  }

  private int[] convertToArray(List<Integer> list) {
    int[] ints = new int[list.size()];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = list.get(i);
    }
    return ints;
  }

  private int[] convertToArray(Set<Integer> set) {
    int[] ints = new int[set.size()];
    int i = 0;
    for (Iterator iterator = set.iterator(); iterator.hasNext(); ) {
      ints[i++] = (Integer) iterator.next();
    }
    return ints;
  }

  @Override public byte[][] splitKey(byte[] key) {
    byte[][] split = new byte[blockKeySize.length][];
    int copyIndex = 0;
    for (int i = 0; i < split.length; i++) {
      split[i] = new byte[blockKeySize[i]];
      System.arraycopy(key, copyIndex, split[i], 0, split[i].length);
      copyIndex += blockKeySize[i];
    }
    return split;
  }

  @Override public byte[][] generateAndSplitKey(long[] keys) throws KeyGenException {
    return splitKey(generateKey(keys));
  }

  @Override public byte[][] generateAndSplitKey(int[] keys) throws KeyGenException {
    return splitKey(generateKey(keys));
  }

  @Override public long[] getKeyArray(byte[][] key) {
    byte[] fullKey = new byte[getKeySizeInBytes()];
    int copyIndex = 0;
    for (int i = 0; i < key.length; i++) {
      System.arraycopy(key[i], 0, fullKey, copyIndex, key[i].length);
      copyIndex += key[i].length;
    }
    return getKeyArray(fullKey);
  }

  @Override public byte[] getKeyByteArray(byte[][] key) {
    byte[] fullKey = new byte[getKeySizeInBytes()];
    int copyIndex = 0;
    for (int i = 0; i < key.length; i++) {
      System.arraycopy(key[i], 0, fullKey, copyIndex, key[i].length);
      copyIndex += key[i].length;
    }
    return fullKey;
  }

  public int[] getBlockKeySize() {
    return blockKeySize;
  }

  @Override public int getKeySizeByBlock(int[] blockIndexes) {
    int size = 0;

    for (int i = 0; i < blockIndexes.length; i++) {
      if (blockIndexes[i] < blockKeySize.length) {
        size += blockKeySize[blockIndexes[i]];
      }
    }
    return size;
  }

  @Override public boolean equals(Object obj) {
    if(!(obj instanceof MultiDimKeyVarLengthEquiSplitGenerator)) {
      return false;
    }
    MultiDimKeyVarLengthEquiSplitGenerator o = (MultiDimKeyVarLengthEquiSplitGenerator)obj;
    return o.dimensionsToSplit == dimensionsToSplit && super.equals(obj);
  }

  @Override public int hashCode() {
    return super.hashCode() + dimensionsToSplit;
  }
}
