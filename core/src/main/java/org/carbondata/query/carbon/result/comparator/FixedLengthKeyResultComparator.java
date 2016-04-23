package org.carbondata.query.carbon.result.comparator;

import java.util.Comparator;

import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

public class FixedLengthKeyResultComparator implements Comparator<ByteArrayWrapper> {

  /**
   * compareRange
   */
  private int[] compareRange;

  /**
   * sortOrder
   */
  private byte sortOrder;

  /**
   * maskedKey
   */
  private byte[] maskedKey;

  public FixedLengthKeyResultComparator(int[] compareRange, byte sortOrder, byte[] maskedKey) {
    this.compareRange = compareRange;
    this.sortOrder = sortOrder;
    this.maskedKey = maskedKey;
  }

  @Override
  public int compare(ByteArrayWrapper byteArrayWrapper1, ByteArrayWrapper byteArrayWrapper2) {
    int cmp = 0;
    byte[] o1 = byteArrayWrapper1.getDictionaryKey();
    byte[] o2 = byteArrayWrapper2.getDictionaryKey();
    for (int i = 0; i < compareRange.length; i++) {
      int a = (o1[compareRange[i]] & this.maskedKey[i]) & 0xff;
      int b = (o2[compareRange[i]] & this.maskedKey[i]) & 0xff;
      cmp = a - b;
      if (cmp != 0) {

        if (sortOrder == 1) {
          return cmp = cmp * -1;
        }
      }
    }
    return 0;
  }

}
