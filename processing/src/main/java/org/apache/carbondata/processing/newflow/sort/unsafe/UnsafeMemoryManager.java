package org.apache.carbondata.processing.newflow.sort.unsafe;

/**
 * Created by root1 on 21/11/16.
 */
public class UnsafeMemoryManager {

  static {
    INSTANCE = new UnsafeMemoryManager(100);
  }

  public static final UnsafeMemoryManager INSTANCE;

  private int memoryInMB;

  private int memoryUsed;

  private UnsafeMemoryManager(int memoryInMB) {
    this.memoryInMB = memoryInMB;
  }

  public synchronized boolean allocateMemory(int memoryInMBRequested) {
    if (memoryUsed + memoryInMBRequested < memoryInMBRequested) {
      memoryUsed += memoryInMBRequested;
      return true;
    }
    return false;
  }

  public synchronized void freeMemory(int memoryInMBtoFree) {
    memoryUsed -= memoryInMBtoFree;
    memoryUsed = memoryUsed - memoryInMBtoFree < 0 ? 0 : memoryUsed - memoryInMBtoFree;
  }

  public synchronized int getAvailableMemory() {
    return memoryInMB - memoryUsed;
  }
}
