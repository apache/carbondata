package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryAllocator;
import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryBlock;

import org.apache.spark.unsafe.Platform;

/**
 * Holds the pointers for rows.
 */
public class PointerBuffer {

  private int length;

  private int actualSize;

  private int totalSize;

  private MemoryAllocator allocator;

  private MemoryBlock pointerBlock;

  private MemoryBlock baseBlock;

  public PointerBuffer(int sizeInMB, boolean unsafe) {
    // TODO can be configurable, it is initial size and it can grow automatically.
    this.length = 100000;
    this.totalSize = sizeInMB;
    if (unsafe) {
      allocator = MemoryAllocator.UNSAFE;
    } else {
      allocator = MemoryAllocator.HEAP;
    }
    pointerBlock = allocator.allocate(length * 4);
    baseBlock = allocator.allocate(sizeInMB * 1024 * 1024);
    zeroOut(pointerBlock);
    zeroOut(baseBlock);
  }

  public PointerBuffer(int length, MemoryBlock baseBlock, MemoryAllocator allocator) {
    this.length = length;
    this.allocator = allocator;
    pointerBlock = allocator.allocate(length * 4);
    this.baseBlock = baseBlock;
    zeroOut(pointerBlock);
  }

  /**
   * Fill this all with 0.
   */
  private void zeroOut(MemoryBlock memoryBlock) {
    long length = memoryBlock.size() / 8;
    long maSize = memoryBlock.getBaseOffset() + length * 8;
    for (long off = memoryBlock.getBaseOffset(); off < maSize; off += 8) {
      Platform.putLong(memoryBlock.getBaseObject(), off, 0);
    }
  }

  public void set(int index, int value) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    CarbonUnsafe.unsafe
        .putInt(pointerBlock.getBaseObject(), pointerBlock.getBaseOffset() + index * 4, value);
  }

  public void set(int value) {
    ensureMemory();
    CarbonUnsafe.unsafe
        .putInt(pointerBlock.getBaseObject(), pointerBlock.getBaseOffset() + actualSize * 4, value);
    actualSize++;
  }

  /**
   * Returns the value at position {@code index}.
   */
  public int get(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    return CarbonUnsafe.unsafe
        .getInt(pointerBlock.getBaseObject(), pointerBlock.getBaseOffset() + index * 4);
  }

  public int getActualSize() {
    return actualSize;
  }

  public MemoryBlock getBaseBlock() {
    return baseBlock;
  }

  public MemoryBlock getPointerBlock() {
    return pointerBlock;
  }

  public MemoryAllocator getAllocator() {
    return allocator;
  }

  private void ensureMemory() {
    if (actualSize >= length) {
      // Expand by quarter, may be we can correct the logic later
      int localLength = length + (int) (length * (0.25));
      MemoryBlock memoryAddress = allocator.allocate(localLength * 4);
      CarbonUnsafe.unsafe.copyMemory(pointerBlock.getBaseObject(), pointerBlock.getBaseOffset(),
          memoryAddress.getBaseObject(), memoryAddress.getBaseOffset(), length);
      allocator.free(pointerBlock);
      pointerBlock = memoryAddress;
      length = localLength;
    }
  }

  public int getTotalSize() {
    return totalSize;
  }

  public void freeMemory() {
    allocator.free(pointerBlock);
    allocator.free(baseBlock);
    UnsafeMemoryManager.INSTANCE.freeMemory(totalSize);
  }
}
