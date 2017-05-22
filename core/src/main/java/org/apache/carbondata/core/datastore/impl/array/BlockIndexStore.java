package org.apache.carbondata.core.datastore.impl.array;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.memory.CarbonUnsafe;
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

  public BlockIndexStore(BTreeBuilderInfo btreeBuilderInfo, MemoryBlock block, int[] rowPointers) {
    this.rowPointers = rowPointers;
    this.block = block;
    this.dimensionColumnValueSize = btreeBuilderInfo.getDimensionColumnValueSize();
  }

  @Override public IndexKey getIndexKey(int index) {
    // Starting we are adding row number so we add 4
    int size = getKeySize(rowPointers[index]);
    byte[] startKey = new byte[size];
    CarbonUnsafe.unsafe.copyMemory(block.getBaseObject(), block.getBaseOffset() + 8, startKey,
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
    if (dimensionColumnValueSize[index] > 0) {
      startOffset = getFixedMinOffset(index, colIndex);
      length = dimensionColumnValueSize[index];
    } else {
      startOffset = getVariableMinOffset(index, colIndex);
      length =
          CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + startOffset);
    }
    byte[] min = new byte[length];
    CarbonUnsafe.unsafe.copyMemory(block.getBaseObject(), block.getBaseOffset() + startOffset, min,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return min;
  }

  @Override public byte[] getMax(int index, int colIndex) {
    int startOffset = 0;
    int length = 0;
    if (dimensionColumnValueSize[index] > 0) {
      startOffset = getFixedMaxOffset(index, colIndex);
      length = dimensionColumnValueSize[index];
    } else {
      startOffset = getVariableMaxOffset(index, colIndex);
      length =
          CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + startOffset);
    }
    byte[] max = new byte[length];
    CarbonUnsafe.unsafe.copyMemory(block.getBaseObject(), block.getBaseOffset() + startOffset, max,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return max;
  }

  private int getFixedMaxOffset(int rowIndex, int colIndex) {
    int start = getKeySize(rowPointers[rowIndex]);

    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] > 0) {
        start += dimensionColumnValueSize[i];
      }
    }
    return start;
  }

  private int getVariableMaxOffset(int rowIndex, int colIndex) {
    int start = getFixedMaxOffset(rowIndex, dimensionColumnValueSize.length - 1);
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] < 0) {
        short aShort =
            CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + start);
        start += aShort;
      }
    }
    return start;
  }

  private int getFixedMinOffset(int rowIndex, int colIndex) {
    int start = getVariableMaxOffset(rowIndex, dimensionColumnValueSize.length - 1);
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] > 0) {
        start += dimensionColumnValueSize[i];
      }
    }
    return start;
  }

  private int getVariableMinOffset(int rowIndex, int colIndex) {
    int start = getFixedMinOffset(rowIndex, dimensionColumnValueSize.length - 1);
    for (int i = 0; i < colIndex; i++) {
      if (dimensionColumnValueSize[i] < 0) {
        short aShort =
            CarbonUnsafe.unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + start);
        start += aShort;
      }
    }
    return start;
  }

  private int getKeySize(int rowPointer) {
    return CarbonUnsafe.unsafe
        .getInt(block.getBaseObject(), block.getBaseOffset() + rowPointer + 4);
  }

  @Override public DataRefNode getNextDataRefNode() {
    return null;
  }

  @Override public int nodeSize() {
    return CarbonUnsafe.unsafe
        .getInt(block.getBaseObject(), block.getBaseOffset() + rowPointers[index]);
  }

  @Override public long nodeNumber() {
    return 0;
  }

  @Override public byte[][] getColumnsMaxValue() {
    return new byte[0][];
  }

  @Override public byte[][] getColumnsMinValue() {
    return new byte[0][];
  }

  @Override
  public DimensionRawColumnChunk[] getDimensionChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    return new DimensionRawColumnChunk[0];
  }

  @Override
  public DimensionRawColumnChunk getDimensionChunk(FileHolder fileReader, int blockIndexes)
      throws IOException {
    return null;
  }

  @Override
  public MeasureRawColumnChunk[] getMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    return new MeasureRawColumnChunk[0];
  }

  @Override public MeasureRawColumnChunk getMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    return null;
  }

  @Override
  public void setDeleteDeltaDataCache(BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache) {

  }

  @Override public BlockletLevelDeleteDeltaDataCache getDeleteDeltaDataCache() {
    return null;
  }

  @Override public int numberOfPages() {
    return 0;
  }
}
