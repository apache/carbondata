package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.spark.util.collection.SortDataFormat;

/**
 * Interface implementation for utilities to sort the data.
 */
public class UnsafeSortDataFormat extends SortDataFormat<Object[], PointerBuffer> {
  private int keyLength;

  private UnsafeCarbonRowPage page;

  public UnsafeSortDataFormat(int keyLength, UnsafeCarbonRowPage page) {
    this.keyLength = keyLength;
    this.page = page;
  }

  @Override public Object[] getKey(PointerBuffer data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override public Object[] newKey() {
    return new Object[keyLength];
  }

  @Override public Object[] getKey(PointerBuffer data, int pos, Object[] reuse) {
    long address = data.get(pos) + data.getBaseBlock().getBaseOffset();
    Object[] row = page.getRowForSort(address, reuse);
    return row;
  }

  @Override public void swap(PointerBuffer data, int pos0, int pos1) {
    int tempPointer = data.get(pos0);
    data.set(pos0, data.get(pos1));
    data.set(pos1, tempPointer);
  }

  @Override public void copyElement(PointerBuffer src, int srcPos, PointerBuffer dst, int dstPos) {
    dst.set(dstPos, src.get(srcPos));
  }

  @Override
  public void copyRange(PointerBuffer src, int srcPos, PointerBuffer dst, int dstPos, int length) {
    CarbonUnsafe.unsafe.copyMemory(src.getPointerBlock().getBaseObject(),
        src.getPointerBlock().getBaseOffset() + srcPos * 4, dst.getPointerBlock().getBaseObject(),
        dst.getPointerBlock().getBaseOffset() + dstPos * 4, length * 4);
  }

  @Override public PointerBuffer allocate(int length) {
    return new PointerBuffer(length, page.getBuffer().getBaseBlock(),
        page.getBuffer().getAllocator());
  }
}
