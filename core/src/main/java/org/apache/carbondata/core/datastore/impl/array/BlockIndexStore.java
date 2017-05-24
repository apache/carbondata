package org.apache.carbondata.core.datastore.impl.array;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;
import org.apache.carbondata.core.memory.MemoryBlock;

/**
 * Order of storing and retrieving data.
 * 1. Number of rows (int)
 * 2. Key length(int) + key
 * 3. Fixed max keys.
 * 4. Variable max keys in LV format (length in short)
 * 5. Fixed min keys.
 * 6. Variable min keys in LV format (length in short)
 */
public class BlockIndexStore implements IndexStore {

  private MemoryBlock block;

  private int[] rowPointers;

  private int[] dimensionColumnValueSize;

  private int[] tableBlockPointer;

  public BlockIndexStore(BTreeBuilderInfo btreeBuilderInfo, MemoryBlock block, int[] rowPointers,
      int[] tableBlockPointer) {
    this.rowPointers = rowPointers;
    this.block = block;
    this.dimensionColumnValueSize = btreeBuilderInfo.getDimensionColumnValueSize();
    this.tableBlockPointer = tableBlockPointer;
  }

  @Override public IndexKey getIndexKey(int index) {
    // Starting we are adding row number so we add 4
    int size = getKeySize(rowPointers[index]);
    byte[] startKey = new byte[size];
    CarbonUnsafe.unsafe
        .copyMemory(block.getBaseObject(), block.getBaseOffset() + rowPointers[index] + 8, startKey,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, startKey.length);
    ByteBuffer buffer = ByteBuffer.wrap(startKey);
    buffer.rewind();
    int dictonaryKeySize = buffer.getInt();
    int nonDictonaryKeySize = buffer.getInt();
    byte[] dictionaryKey = new byte[dictonaryKeySize];
    buffer.get(dictionaryKey);
    byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
    buffer.get(nonDictionaryKey);
    return new IndexKey(dictionaryKey, nonDictionaryKey);
  }

  @Override public byte[] getMin(int index, int colIndex) {
    int startOffset = 0;
    int length = 0;
    if (dimensionColumnValueSize[colIndex] > 0) {
      startOffset = getFixedMinOffset(index, colIndex);
      length = dimensionColumnValueSize[colIndex];
    } else {
      startOffset = getVariableMinOffset(index, colIndex);
      length =
          CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + startOffset);
      startOffset += 2;
    }
    byte[] min = new byte[length];
    CarbonUnsafe.unsafe.copyMemory(block.getBaseObject(), block.getBaseOffset() + startOffset, min,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return min;
  }

  @Override public byte[] getMax(int index, int colIndex) {
    int startOffset = 0;
    int length = 0;
    if (dimensionColumnValueSize[colIndex] > 0) {
      startOffset = getFixedMaxOffset(index, colIndex);
      length = dimensionColumnValueSize[colIndex];
    } else {
      startOffset = getVariableMaxOffset(index, colIndex);
      length =
          CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + startOffset);
      startOffset += 2;
    }
    byte[] max = new byte[length];
    CarbonUnsafe.unsafe.copyMemory(block.getBaseObject(), block.getBaseOffset() + startOffset, max,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return max;
  }

  private int getFixedMaxOffset(int rowIndex, int colIndex) {
    int start = getKeySize(rowPointers[rowIndex]);
    start += rowPointers[rowIndex] + 8;
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] > 0) {
        start += dimensionColumnValueSize[i];
      }
    }
    return start;
  }

  private int getVariableMaxOffset(int rowIndex, int colIndex) {
    int start = getFixedMaxOffset(rowIndex, dimensionColumnValueSize.length);
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] < 0) {
        short aShort =
            CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + start);
        start += 2;
        start += aShort;
      }
    }
    return start;
  }

  private int getFixedMinOffset(int rowIndex, int colIndex) {
    int start = getVariableMaxOffset(rowIndex, dimensionColumnValueSize.length);
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] > 0) {
        start += dimensionColumnValueSize[i];
      }
    }
    return start;
  }

  private int getVariableMinOffset(int rowIndex, int colIndex) {
    int start = getFixedMinOffset(rowIndex, dimensionColumnValueSize.length);
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] < 0) {
        short aShort =
            CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + start);
        start += 2;
        start += aShort;
      }
    }
    return start;
  }

  private int getKeySize(int rowPointer) {
    return CarbonUnsafe.unsafe
        .getInt(block.getBaseObject(), block.getBaseOffset() + rowPointer + 4);
  }

  @Override public boolean isKeyAvailableAtIndex(int index) {
    return index < rowPointers.length;
  }

  @Override public int getRowCount(int index) {
    return CarbonUnsafe.unsafe
        .getInt(block.getBaseObject(), block.getBaseOffset() + rowPointers[index]);
  }

  @Override public byte[][] getMins(int index) {
    byte[][] mins = new byte[dimensionColumnValueSize.length][];
    for (int i = 0; i < mins.length; i++) {
      mins[i] = getMin(index, i);
    }
    return mins;
  }

  @Override public byte[][] getMaxs(int index) {
    byte[][] maxs = new byte[dimensionColumnValueSize.length][];
    for (int i = 0; i < maxs.length; i++) {
      maxs[i] = getMax(index, i);
    }
    return maxs;
  }

  public TableBlockInfo getTableBlockInfo(int index) throws IOException {
    int size = CarbonUnsafe.unsafe
        .getInt(block.getBaseObject(), block.getBaseOffset() + tableBlockPointer[index]);
    byte[] data = new byte[size];
    CarbonUnsafe.unsafe
        .copyMemory(block.getBaseObject(), block.getBaseOffset() + tableBlockPointer[index] + 4,
            data, CarbonUnsafe.BYTE_ARRAY_OFFSET, size);
    TableBlockInfo tableBlockInfo = new TableBlockInfo();
    tableBlockInfo.writeSerializedData(data);
    return tableBlockInfo;
  }

  @Override public int getIndexKeyCount() {
    return rowPointers.length;
  }

  @Override public void clear() {
    MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().free(block);
  }
}
