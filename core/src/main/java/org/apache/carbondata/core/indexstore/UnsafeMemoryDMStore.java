/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.indexstore;

import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.UnsafeDataMapRow;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

import static org.apache.carbondata.core.memory.CarbonUnsafe.BYTE_ARRAY_OFFSET;
import static org.apache.carbondata.core.memory.CarbonUnsafe.getUnsafe;

/**
 * Store the data map row @{@link DataMapRow} data to unsafe.
 */
public class UnsafeMemoryDMStore {

  private MemoryBlock memoryBlock;

  private static int capacity = 8 * 1024 * 1024;

  private int allocatedSize;

  private int runningLength;

  private boolean isMemoryFreed;

  private CarbonRowSchema[] schema;

  private int[] pointers;

  private int rowCount;

  private final long taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  public UnsafeMemoryDMStore(CarbonRowSchema[] schema) throws MemoryException {
    this.schema = schema;
    this.allocatedSize = capacity;
    this.memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, allocatedSize);
    this.pointers = new int[1000];
  }

  /**
   * Check memory is sufficient or not, if not sufficient allocate more memory and copy old data to
   * new one.
   *
   * @param rowSize
   */
  private void ensureSize(int rowSize) throws MemoryException {
    if (runningLength + rowSize >= allocatedSize) {
      MemoryBlock allocate =
          UnsafeMemoryManager.allocateMemoryWithRetry(taskId, allocatedSize + capacity);
      getUnsafe().copyMemory(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset(),
          allocate.getBaseObject(), allocate.getBaseOffset(), runningLength);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      allocatedSize = allocatedSize + capacity;
      memoryBlock = allocate;
    }
    if (this.pointers.length <= rowCount + 1) {
      int[] newPointer = new int[pointers.length + 1000];
      System.arraycopy(pointers, 0, newPointer, 0, pointers.length);
      this.pointers = newPointer;
    }
  }

  /**
   * Add the index row to unsafe.
   *
   * @param indexRow
   * @return
   */
  public void addIndexRowToUnsafe(DataMapRow indexRow) throws MemoryException {
    // First calculate the required memory to keep the row in unsafe
    int rowSize = indexRow.getTotalSizeInBytes();
    // Check whether allocated memory is sufficient or not.
    ensureSize(rowSize);
    int pointer = runningLength;

    for (int i = 0; i < schema.length; i++) {
      addToUnsafe(schema[i], indexRow, i);
    }
    pointers[rowCount++] = pointer;
  }

  private void addToUnsafe(CarbonRowSchema schema, DataMapRow row, int index) {
    switch (schema.getSchemaType()) {
      case FIXED:
        DataType dataType = schema.getDataType();
        if (dataType == DataTypes.BYTE) {
          getUnsafe()
              .putByte(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + runningLength,
                  row.getByte(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.SHORT) {
          getUnsafe()
              .putShort(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + runningLength,
                  row.getShort(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.INT) {
          getUnsafe()
              .putInt(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + runningLength,
                  row.getInt(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.LONG) {
          getUnsafe()
              .putLong(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + runningLength,
                  row.getLong(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.FLOAT) {
          getUnsafe()
              .putFloat(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + runningLength,
                  row.getFloat(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.DOUBLE) {
          getUnsafe()
              .putDouble(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + runningLength,
                  row.getDouble(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.BYTE_ARRAY) {
          byte[] data = row.getByteArray(index);
          getUnsafe().copyMemory(data, BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + runningLength, data.length);
          runningLength += row.getSizeInBytes(index);
        } else {
          throw new UnsupportedOperationException(
              "unsupported data type for unsafe storage: " + schema.getDataType());
        }
        break;
      case VARIABLE:
        byte[] data = row.getByteArray(index);
        getUnsafe().putShort(memoryBlock.getBaseObject(),
            memoryBlock.getBaseOffset() + runningLength, (short) data.length);
        runningLength += 2;
        getUnsafe().copyMemory(data, BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
            memoryBlock.getBaseOffset() + runningLength, data.length);
        runningLength += data.length;
        break;
      case STRUCT:
        CarbonRowSchema[] childSchemas =
            ((CarbonRowSchema.StructCarbonRowSchema) schema).getChildSchemas();
        DataMapRow struct = row.getRow(index);
        for (int i = 0; i < childSchemas.length; i++) {
          addToUnsafe(childSchemas[i], struct, i);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type for unsafe storage: " + schema.getDataType());
    }
  }

  public UnsafeDataMapRow getUnsafeRow(int index) {
    assert (index < rowCount);
    return new UnsafeDataMapRow(schema, memoryBlock, pointers[index]);
  }

  public void finishWriting() throws MemoryException {
    if (runningLength < allocatedSize) {
      MemoryBlock allocate =
          UnsafeMemoryManager.allocateMemoryWithRetry(taskId, runningLength);
      getUnsafe().copyMemory(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset(),
          allocate.getBaseObject(), allocate.getBaseOffset(), runningLength);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = allocate;
    }
    // Compact pointers.
    if (rowCount < pointers.length) {
      int[] newPointer = new int[rowCount];
      System.arraycopy(pointers, 0, newPointer, 0, rowCount);
      this.pointers = newPointer;
    }
  }

  public void freeMemory() {
    if (!isMemoryFreed) {
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      isMemoryFreed = true;
    }
  }

  public int getMemoryUsed() {
    return runningLength;
  }

  public CarbonRowSchema[] getSchema() {
    return schema;
  }

  public int getRowCount() {
    return rowCount;
  }

}
