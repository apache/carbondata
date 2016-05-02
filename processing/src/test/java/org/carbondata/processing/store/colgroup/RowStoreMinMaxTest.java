package org.carbondata.processing.store.colgroup;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.vo.ColumnGroupModel;
import org.carbondata.processing.store.colgroup.ColumnDataHolder;
import org.carbondata.processing.store.colgroup.DataHolder;
import org.carbondata.processing.store.colgroup.RowBlockStorage;
import org.carbondata.processing.store.colgroup.RowStoreDataHolder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * RowStore store min max test
 */
public class RowStoreMinMaxTest {
  /**
   * colgrpmodel
   */
  ColumnGroupModel colGrpModel;
  /**
   * column splitter
   */
  ColumnarSplitter columnSplitter;
  /**
   * column groups
   */
  int[][] columnGroups;

  /**
   * surrogate key
   */
  int[][] data;
  /**
   * mdkey data
   */
  byte[][] mdkeyData;

  /**
   * min value of surrogates
   */
  int[] min;
  /**
   * max value of surrogates
   */
  int[] max;

  @Before public void setupBeforeClass() throws KeyGenException {
    int[] dimLens = new int[] { 100, 10, 100, 1 };
    columnGroups = new int[][] { { 0, 1 }, { 2, 3 } };
    colGrpModel = CarbonUtil.getColGroupModel(dimLens, columnGroups);
    columnSplitter = new MultiDimKeyVarLengthVariableSplitGenerator(CarbonUtil
        .getDimensionBitLength(colGrpModel.getColumnGroupCardinality(),
            colGrpModel.getColumnSplit()), colGrpModel.getColumnSplit());
    KeyGenerator keyGenerator = (KeyGenerator) columnSplitter;
    Random random = new Random();
    data = new int[1000][];
    min = new int[dimLens.length];
    max = new int[dimLens.length];
    for (int i = 0; i < 1000; i++) {
      data[i] = new int[] { random.nextInt(100), random.nextInt(10), random.nextInt(100),
          random.nextInt(1) };
      setMinData(data[i]);
      setMaxData(data[i]);
    }
    mdkeyData = new byte[1000][];
    for (int i = 0; i < 1000; i++) {
      mdkeyData[i] = keyGenerator.generateKey(data[i]);
    }
  }

  private void setMaxData(int[] data) {
    for (int i = 0; i < max.length; i++) {
      if (max[i] < data[i]) {
        max[i] = data[i];
      }
    }

  }

  private void setMinData(int[] data) {
    for (int i = 0; i < min.length; i++) {
      if (min[i] > data[i]) {
        min[i] = data[i];
      }
    }
  }

  @Test public void testRowStoreMinMax() throws KeyGenException {

    DataHolder[] dataHolders =
        getDataHolders(colGrpModel.getNoOfColumnStore(), mdkeyData.length);
    for (int i = 0; i < mdkeyData.length; i++) {
      byte[][] split = columnSplitter.splitKey(mdkeyData[i]);
      for (int j = 0; j < split.length; j++) {
        dataHolders[j].addData(split[j], i);
      }

    }
    RowBlockStorage[] rowBlockStorage = new RowBlockStorage[dataHolders.length];
    for (int i = 0; i < dataHolders.length; i++) {
      rowBlockStorage[i] = new RowBlockStorage(dataHolders[i]);
    }
    int[][] columnGroup = colGrpModel.getColumnGroup();
    for (int i = 0; i < dataHolders.length; i++) {
      assertMinMax(rowBlockStorage[i].getMin(),rowBlockStorage[i].getMax(), columnGroup[i]);
    }

  }

  private void assertMinMax(byte[] min,byte[] max, int[] columnGroup) throws KeyGenException {
    KeyGenerator keyGenerator = (KeyGenerator) columnSplitter;

    int columnStartIndex = 0;
    for (int i = 0; i < columnGroup.length; i++) {
      int col = columnGroup[i];
      int[] maskByteRange = getMaskByteRange(col);
      int[] maskBytePosition = new int[keyGenerator.getKeySizeInBytes()];
      updateMaskedKeyRanges(maskBytePosition, maskByteRange);

      byte[] columnMin = new byte[maskByteRange.length];
      System.arraycopy(min, columnStartIndex, columnMin, 0, maskByteRange.length);
      byte[] columnMax = new byte[maskByteRange.length];
      System.arraycopy(max, columnStartIndex, columnMax, 0, maskByteRange.length);

      long[] minKeyArray = keyGenerator.getKeyArray(columnMin, maskBytePosition);
      long[] maxKeyArray = keyGenerator.getKeyArray(columnMax, maskBytePosition);
      Assert.assertEquals(minKeyArray[col], this.min[col]);
      Assert.assertEquals(maxKeyArray[col], this.max[col]);
      System.out.println(
          "(min,max) for column " + col + ":(" + minKeyArray[col] + "," + maxKeyArray[col] + ")");
      columnStartIndex += maskByteRange.length;
    }

  }

  private DataHolder[] getDataHolders(int noOfColumn, int noOfRow) {
    DataHolder[] dataHolders = new DataHolder[noOfColumn];
    for (int i = 0; i < noOfColumn; i++) {
      if (colGrpModel.isColumnar(i)) {
        dataHolders[i] = new ColumnDataHolder(noOfRow);
      } else {
        dataHolders[i] = new RowStoreDataHolder(this.colGrpModel, columnSplitter, i, noOfRow);
      }
    }
    return dataHolders;
  }

  private int[] getMaskByteRange(int col) {
    KeyGenerator keyGenerator = (KeyGenerator) columnSplitter;
    Set<Integer> integers = new HashSet<>();
    int[] range = keyGenerator.getKeyByteOffsets(col);
    for (int j = range[0]; j <= range[1]; j++) {
      integers.add(j);
    }
    int[] byteIndexs = new int[integers.size()];
    int j = 0;
    for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
      Integer integer = (Integer) iterator.next();
      byteIndexs[j++] = integer.intValue();
    }
    return byteIndexs;
  }

  private void updateMaskedKeyRanges(int[] maskedKey, int[] maskedKeyRanges) {
    Arrays.fill(maskedKey, -1);
    for (int i = 0; i < maskedKeyRanges.length; i++) {
      maskedKey[maskedKeyRanges[i]] = i;
    }
  }
}
