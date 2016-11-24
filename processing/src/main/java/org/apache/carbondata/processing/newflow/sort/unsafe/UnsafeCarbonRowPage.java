package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryBlock;

/**
 * It can keep the data of prescribed size data in offheap/onheap memory and returns it when needed
 */
public class UnsafeCarbonRowPage {

  private boolean[] noDictionaryDimensionMapping;

  private int dimensionSize;

  private int measureSize;

  private char[] aggType;

  private long[] nullSetWords;

  private PointerBuffer buffer;

  private int lastSize;

  private int sizeToBeUsed;

  private MemoryBlock dataBlock;

  private boolean keepInMemory;

  public UnsafeCarbonRowPage(boolean[] noDictionaryDimensionMapping, int dimensionSize,
      int measureSize, char[] aggType, int sizeInMB, boolean unsafe) {
    this.noDictionaryDimensionMapping = noDictionaryDimensionMapping;
    this.dimensionSize = dimensionSize;
    this.measureSize = measureSize;
    this.aggType = aggType;
    this.nullSetWords = new long[((measureSize - 1) >> 6) + 1];
    buffer = new PointerBuffer(sizeInMB, unsafe);
    this.dataBlock = buffer.getBaseBlock();
    int totalSize = sizeInMB * 1024 * 1024;
    // TODO Only using 98% of space for safe side.May be we can have different logic.
    sizeToBeUsed = totalSize - (totalSize * 2) / 100;
    // Check memory is available, if memory is not available flush to disk.
    this.keepInMemory = UnsafeMemoryManager.INSTANCE.allocateMemory(sizeInMB);

  }

  public void addRow(Object[] row) {
    int size = addRow(row, dataBlock.getBaseOffset() + lastSize);
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
        CarbonUnsafe.unsafe.putShort(dataBlock.getBaseObject(), address + size, (short) col.length);
        size += 2;
        CarbonUnsafe.unsafe
            .copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataBlock.getBaseObject(),
                address + size, col.length);
        size += col.length;
      } else {
        CarbonUnsafe.unsafe.putInt(dataBlock.getBaseObject(), address + size, (int) row[dimCount]);
        size += 4;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      byte[] col = (byte[]) row[dimCount];
      CarbonUnsafe.unsafe.putShort(dataBlock.getBaseObject(), address + size, (short) col.length);
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataBlock.getBaseObject(),
          address + size, col.length);
      size += col.length;
    }
    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      Object value = row[mesCount + dimensionSize];
      if (null != value) {
        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          Double val = (Double) value;
          CarbonUnsafe.unsafe.putDouble(dataBlock.getBaseObject(), address + size, val);
          size += 8;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          Long val = (Long) value;
          CarbonUnsafe.unsafe.putLong(dataBlock.getBaseObject(), address + size, val);
          size += 8;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          BigDecimal val = (BigDecimal) value;
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
          CarbonUnsafe.unsafe.putShort(dataBlock.getBaseObject(), address + size,
              (short) bigDecimalInBytes.length);
          size += 2;
          CarbonUnsafe.unsafe.copyMemory(bigDecimalInBytes, CarbonUnsafe.BYTE_ARRAY_OFFSET,
              dataBlock.getBaseObject(), address + size, bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
        }
        set(nullSetWords, mesCount);
      } else {
        unset(nullSetWords, mesCount);
      }
      CarbonUnsafe.unsafe
          .copyMemory(nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET, dataBlock.getBaseObject(),
              address, nullSetWords.length);
    }
    return size;
  }

  public Object[] getRow(long address, Object[] rowToFill) {
    int dimCount = 0;
    int size = 0;
    int nullSetSize = nullSetWords.length * 8;
    long[] words = new long[nullSetWords.length];
    CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, words,
        CarbonUnsafe.LONG_ARRAY_OFFSET, words.length);
    size += nullSetSize;

    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, col,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
        size += col.length;
        rowToFill[dimCount] = col;
      } else {
        int anInt = CarbonUnsafe.unsafe.getInt(dataBlock.getBaseObject(), address + size);
        size += 4;
        rowToFill[dimCount] = anInt;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, col,
          CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      rowToFill[dimCount] = col;
    }

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(words, mesCount)) {
        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          Double val = CarbonUnsafe.unsafe.getDouble(dataBlock.getBaseObject(), address + size);
          size += 8;
          rowToFill[dimensionSize + mesCount] = val;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          Long val = CarbonUnsafe.unsafe.getLong(dataBlock.getBaseObject(), address + size);
          size += 8;
          rowToFill[dimensionSize + mesCount] = val;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
          byte[] bigDecimalInBytes = new byte[aShort];
          size += 2;
          CarbonUnsafe.unsafe
              .copyMemory(dataBlock.getBaseObject(), address + size, bigDecimalInBytes,
                  CarbonUnsafe.BYTE_ARRAY_OFFSET, bigDecimalInBytes.length);
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

  public Object[] getRowForSort(long address, Object[] rowToFill) {
    int dimCount = 0;
    int size = 0;
    size += nullSetWords.length * 8;
    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, col,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
        size += col.length;
        rowToFill[dimCount] = col;
      } else {
        int anInt = CarbonUnsafe.unsafe.getInt(dataBlock.getBaseObject(), address + size);
        size += 4;
        rowToFill[dimCount] = anInt;
      }
    }
    return rowToFill;
  }


  public void fillRow(long address, DataOutputStream stream) throws IOException {
    int dimCount = 0;
    int size = 0;
    int nullSetSize = nullSetWords.length * 8;
    long[] words = new long[nullSetWords.length];
    CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, words,
        CarbonUnsafe.LONG_ARRAY_OFFSET, words.length);
    size += nullSetSize;
    for (int i = 0; i < words.length; i++) {
      stream.writeLong(words[i]);
    }

    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, col,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
        size += col.length;
        stream.writeShort(aShort);
        stream.write(col);
      } else {
        int anInt = CarbonUnsafe.unsafe.getInt(dataBlock.getBaseObject(), address + size);
        size += 4;
        stream.writeInt(anInt);
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(dataBlock.getBaseObject(), address + size, col,
          CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      stream.writeShort(aShort);
      stream.write(col);
    }

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(words, mesCount)) {
        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          Double val = CarbonUnsafe.unsafe.getDouble(dataBlock.getBaseObject(), address + size);
          size += 8;
          stream.writeDouble(val);
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          Long val = CarbonUnsafe.unsafe.getLong(dataBlock.getBaseObject(), address + size);
          size += 8;
          stream.writeLong(val);
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          short aShort = CarbonUnsafe.unsafe.getShort(dataBlock.getBaseObject(), address + size);
          byte[] bigDecimalInBytes = new byte[aShort];
          size += 2;
          CarbonUnsafe.unsafe
              .copyMemory(dataBlock.getBaseObject(), address + size, bigDecimalInBytes,
                  CarbonUnsafe.BYTE_ARRAY_OFFSET, bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
          stream.writeShort(aShort);
          stream.write(bigDecimalInBytes);
        }
      }
    }
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

  public MemoryBlock getDataBlock() {
    return dataBlock;
  }

  public boolean isKeepInMemory() {
    return keepInMemory;
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
      return getRow(address + dataBlock.getBaseOffset());
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
