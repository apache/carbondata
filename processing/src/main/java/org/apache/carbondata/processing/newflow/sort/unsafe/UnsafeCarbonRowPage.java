package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Created by root1 on 21/11/16.
 */
public class UnsafeCarbonRowPage {

  public final int BYTE_ARRAY_OFFSET;

  public final int LONG_ARRAY_OFFSET;

  private boolean[] noDictionaryDimensionMapping;

  private int dimensionSize;

  private int measureSize;

  private char[] aggType;

  private long[] nullSetWords;

  private PointerBuffer buffer;

  private int lastSize;

  private int sizeToBeUsed;

  public UnsafeCarbonRowPage(boolean[] noDictionaryDimensionMapping, int dimensionSize,
      int measureSize, char[] aggType, int sizeInMB) {
    this.noDictionaryDimensionMapping = noDictionaryDimensionMapping;
    this.dimensionSize = dimensionSize;
    this.measureSize = measureSize;
    this.aggType = aggType;
    this.nullSetWords = new long[((measureSize - 1) >> 6) + 1];
    BYTE_ARRAY_OFFSET = CarbonUnsafe.unsafe.arrayBaseOffset(byte[].class);
    LONG_ARRAY_OFFSET = CarbonUnsafe.unsafe.arrayBaseOffset(long[].class);
    buffer = new PointerBuffer(sizeInMB);
    sizeToBeUsed = ((sizeInMB * 1024 * 1024) * 5)/100;
  }

  public void addRow(Object[] row) {
    int size = addRow(row, buffer.getBaseAddress() + lastSize);
    buffer.set(lastSize);
    lastSize = lastSize + size;
  }

  public Iterator<Object[]> getIterator() {
    return new UnsafeIterator();
  }

  private int addRow(Object[] row, long address) {
    int dimCount = 0;
    int size = 0;
    Arrays.fill(nullSetWords, 0);
    int nullSetSize = nullSetWords.length * 8;
    size += nullSetSize;
    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        byte[] col = (byte[]) row[dimCount];
        CarbonUnsafe.unsafe.putShort(address + size, (short) col.length);
        size += 2;
        CarbonUnsafe.unsafe.copyMemory(col, BYTE_ARRAY_OFFSET, null, address + size, col.length);
        size += col.length;
      } else {
        CarbonUnsafe.unsafe.putInt(address + size, (int) row[dimCount]);
        size += 4;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      byte[] col = (byte[]) row[dimCount];
      CarbonUnsafe.unsafe.putShort(address + size, (short) col.length);
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(col, BYTE_ARRAY_OFFSET, null, address + size, col.length);
      size += col.length;
    }
    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      Object value = row[mesCount + dimensionSize];
      if (null != value) {
        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          Double val = (Double) value;
          CarbonUnsafe.unsafe.putDouble(address + size, val);
          size += 8;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          Long val = (Long) value;
          CarbonUnsafe.unsafe.putLong(address + size, val);
          size += 8;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          BigDecimal val = (BigDecimal) value;
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
          CarbonUnsafe.unsafe.putShort(address + size, (short) bigDecimalInBytes.length);
          size += 2;
          CarbonUnsafe.unsafe.copyMemory(bigDecimalInBytes, BYTE_ARRAY_OFFSET, null, address + size,
              bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
        }
        set(nullSetWords, mesCount);
      } else {
        unset(nullSetWords, mesCount);
      }
      CarbonUnsafe.unsafe
          .copyMemory(nullSetWords, LONG_ARRAY_OFFSET, null, address, nullSetWords.length);
    }
    return size;
  }

  public Object[] getRow(long address, Object[] rowToFill) {
    int dimCount = 0;
    int size = 0;
    int nullSetSize = nullSetWords.length * 8;
    long[] words = new long[nullSetWords.length];
    CarbonUnsafe.unsafe.copyMemory(null, address + size, words, LONG_ARRAY_OFFSET, words.length);
    size += nullSetSize;

    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.unsafe.getShort(address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.unsafe.copyMemory(null, address + size, col, BYTE_ARRAY_OFFSET, col.length);
        size += col.length;
        rowToFill[dimCount] = col;
      } else {
        int anInt = CarbonUnsafe.unsafe.getInt(address + size);
        size += 4;
        rowToFill[dimCount] = anInt;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.unsafe.getShort(address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(null, address + size, col, BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      rowToFill[dimCount] = col;
    }

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(words, mesCount)) {
        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          Double val = CarbonUnsafe.unsafe.getDouble(address + size);
          size += 8;
          rowToFill[dimensionSize + mesCount] = val;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          Long val = CarbonUnsafe.unsafe.getLong(address + size);
          size += 8;
          rowToFill[dimensionSize + mesCount] = val;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          short aShort = CarbonUnsafe.unsafe.getShort(address + size);
          byte[] bigDecimalInBytes = new byte[aShort];
          size += 2;
          CarbonUnsafe.unsafe.copyMemory(null, address + size, bigDecimalInBytes, BYTE_ARRAY_OFFSET,
              bigDecimalInBytes.length);
          BigDecimal val = DataTypeUtil.byteToBigDecimal(bigDecimalInBytes);
          size += bigDecimalInBytes.length;
          rowToFill[dimensionSize + mesCount] = val;
        }
      } else {
        rowToFill[dimensionSize + mesCount] = null;
      }
    }
    return rowToFill;
  }

  private Object[] getRow(long address) {
    Object[] row = new Object[dimensionSize + measureSize];
    return getRow(address, row);
  }

  public void freeMemory() {
    buffer.freeMemory();
  }

  public PointerBuffer getBuffer() {
    return buffer;
  }

  public int getUsedSize() {
    return lastSize;
  }

  public boolean canAdd() {
    return lastSize < sizeToBeUsed;
  }

  class UnsafeIterator extends CarbonIterator<Object[]> {

    private int counter;

    private int actualSize;

    public UnsafeIterator() {
      this.actualSize = buffer.getActualSize();
    }

    @Override public boolean hasNext() {
      if (counter < actualSize) {
        return true;
      }
      return false;
    }

    @Override public Object[] next() {
      long address = buffer.get(counter);
      counter++;
      return getRow(address + buffer.getBaseAddress());
    }
  }

  public static void set(long[] words, int index) {
    int wordOffset = (index >> 6);
    words[wordOffset] |= (1L << index);
  }

  public static void unset(long[] words, int index) {
    int wordOffset = (index >> 6);
    words[wordOffset] &= ~(1L << index);
  }

  public static boolean isSet(long[] words, int index) {
    int wordOffset = (index >> 6);
    return ((words[wordOffset] & (1L << index)) != 0);
  }

  public boolean[] getNoDictionaryDimensionMapping() {
    return noDictionaryDimensionMapping;
  }

}
