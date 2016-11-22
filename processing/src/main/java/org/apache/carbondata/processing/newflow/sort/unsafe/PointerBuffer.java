package org.apache.carbondata.processing.newflow.sort.unsafe;

/**
 * Created by root1 on 21/11/16.
 */
public class PointerBuffer {

  private long pointerBaseAddress;

  private int length;

  private int actualSize;

  private long baseAddress;

  private int totalSize;

  public PointerBuffer(int sizeInMB) {
    this.length = 100000;
    this.totalSize = sizeInMB;
    pointerBaseAddress = CarbonUnsafe.unsafe.allocateMemory(length * 4);
    baseAddress = CarbonUnsafe.unsafe.allocateMemory(sizeInMB * 1024 * 1024);
    zeroOut();
  }

  public PointerBuffer(int length, long baseAddress) {
    this.length = length;
    pointerBaseAddress = CarbonUnsafe.unsafe.allocateMemory(length * 4);
    this.baseAddress = baseAddress;
    zeroOut();
  }

  /**
   * Fill this all with 0.
   */
  public void zeroOut() {
    CarbonUnsafe.unsafe.setMemory(pointerBaseAddress, length * 4, (byte) 0);
  }

  public void set(int index, int value) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    CarbonUnsafe.unsafe.putInt(pointerBaseAddress + index * 4, value);
  }

  public void set(int value) {
    ensureMemory();
    CarbonUnsafe.unsafe.putInt(pointerBaseAddress + actualSize * 4, value);
    actualSize++;
  }

  /**
   * Returns the value at position {@code index}.
   */
  public int get(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    return CarbonUnsafe.unsafe.getInt(pointerBaseAddress + index * 4);
  }

  public int getActualSize() {
    return actualSize;
  }

  public long getBaseAddress() {
    return baseAddress;
  }

  public long getPointerBaseAddress() {
    return pointerBaseAddress;
  }

  private void ensureMemory() {
    if (actualSize >= length) {
      // Expand by quarter, may be we can correct the logic later
      int localLength = length + (int) (length * (0.25));
      long memoryAddress = CarbonUnsafe.unsafe.allocateMemory(localLength * 4);
      CarbonUnsafe.unsafe.copyMemory(pointerBaseAddress, memoryAddress, length);
      CarbonUnsafe.unsafe.freeMemory(pointerBaseAddress);
      pointerBaseAddress = memoryAddress;
      length = localLength;
    }
  }

  public int getTotalSize() {
    return totalSize;
  }

  public void freeMemory() {
    CarbonUnsafe.unsafe.freeMemory(pointerBaseAddress);
    CarbonUnsafe.unsafe.freeMemory(baseAddress);
  }
}
