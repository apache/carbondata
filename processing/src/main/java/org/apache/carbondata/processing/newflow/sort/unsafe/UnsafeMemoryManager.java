package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Created by root1 on 21/11/16.
 */
public class UnsafeMemoryManager {

  static {
    int size = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB,
            CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT));
    INSTANCE = new UnsafeMemoryManager(size);
  }

  public static final UnsafeMemoryManager INSTANCE;

  private int memoryInMB;

  private int memoryUsed;

  private UnsafeMemoryManager(int memoryInMB) {
    this.memoryInMB = memoryInMB;
  }

  public synchronized boolean allocateMemory(int memoryInMBRequested) {
    if (memoryUsed + memoryInMBRequested < memoryInMB) {
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
