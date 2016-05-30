package org.carbondata.core.keygenerator.columnar.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;

public class MultiDimKeyVarLengthVariableSplitGenerator extends MultiDimKeyVarLengthGenerator
    implements ColumnarSplitter {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private int[] dimensionsToSplit;

  private int[] blockKeySize;

  public MultiDimKeyVarLengthVariableSplitGenerator(int[] lens, int[] dimSplit) {
    super(lens);
    this.dimensionsToSplit = dimSplit;
    initialise();

  }

  private void initialise() {
    int s = 0;
    List<Set<Integer>> splitList =
        new ArrayList<Set<Integer>>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    Set<Integer> split = new TreeSet<Integer>();
    splitList.add(split);
    int dimSplitIndx = 0;

    for (int i = 0; i < byteRangesForKeys.length; i++) {
      if (s == dimensionsToSplit[dimSplitIndx]) {
        s = 0;
        split = new TreeSet<Integer>();
        splitList.add(split);
        dimSplitIndx++;
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
      if (splits[j - 1].get(splits[j - 1].size() - 1) == splits[j].get(0)) {
        splits[j].remove(0);
      }
    }
    int[][] splitDimArray = new int[splits.length][];
    for (int j = 0; j < splits.length; j++) {
      int[] a = convertToArray(splits[j]);
      splitDimArray[j] = new int[] { a[0], a[a.length - 1] };
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
      splitDimArrayLocalIndexes[j] = new int[] { 0, splitDimArray[j][1] - splitDimArray[j][0] };
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
      blockKeySize[j] = splitDimArray[j][1] - splitDimArray[j][0] + 1;
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

  @Override public byte[] getKeyByteArray(byte[][] key, int[] columnIndexes) {
    return null;
  }

  @Override public long[] getKeyArray(byte[][] key, int[] columnIndexes) {
    return null;
  }

  public int[] getBlockKeySize() {
    return blockKeySize;
  }

  @Override public int getKeySizeByBlock(int[] blockIndexes) {
    Set<Integer> selectedRanges = new HashSet<>();
    for (int i = 0; i < blockIndexes.length; i++) {
      int[] byteRange = byteRangesForKeys[blockIndexes[i]];
      for (int j = byteRange[0]; j <= byteRange[1]; j++) {
        selectedRanges.add(j);
      }
    }
    return selectedRanges.size();
  }

}
