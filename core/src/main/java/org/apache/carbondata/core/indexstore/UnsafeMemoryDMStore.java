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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.UnsafeDataMapRow;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryType;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import static org.apache.carbondata.core.memory.CarbonUnsafe.BYTE_ARRAY_OFFSET;
import static org.apache.carbondata.core.memory.CarbonUnsafe.getUnsafe;

/**
 * Store the data map row @{@link DataMapRow} data to unsafe.
 */
public class UnsafeMemoryDMStore extends AbstractMemoryDMStore {

  private static final long serialVersionUID = -5344592407101055335L;

  private transient MemoryBlock memoryBlock;

  private static int capacity = 8 * 1024;

  private int allocatedSize;

  private int runningLength;

  private int[] pointers;

  private int rowCount;

  private byte[] data;

  public UnsafeMemoryDMStore() {
    this.allocatedSize = capacity;
    this.memoryBlock =
        UnsafeMemoryManager.allocateMemoryWithRetry(MemoryType.ONHEAP, taskId, allocatedSize);
    this.pointers = new int[100];
  }

  /**
   * Check memory is sufficient or not, if not sufficient allocate more memory and copy old data to
   * new one.
   *
   * @param rowSize
   */
  private void ensureSize(int rowSize) {
    if (runningLength + rowSize >= allocatedSize) {
      increaseMemory(runningLength + rowSize);
    }
    if (this.pointers.length <= rowCount + 1) {
      int[] newPointer = new int[pointers.length + 100];
      System.arraycopy(pointers, 0, newPointer, 0, pointers.length);
      this.pointers = newPointer;
    }
  }

  private void increaseMemory(int requiredMemory) {
    MemoryBlock newMemoryBlock = UnsafeMemoryManager
        .allocateMemoryWithRetry(MemoryType.ONHEAP, taskId, allocatedSize + requiredMemory);
    getUnsafe().copyMemory(this.memoryBlock.getBaseObject(), this.memoryBlock.getBaseOffset(),
        newMemoryBlock.getBaseObject(), newMemoryBlock.getBaseOffset(), runningLength);
    UnsafeMemoryManager.INSTANCE.freeMemory(taskId, this.memoryBlock);
    allocatedSize = allocatedSize + requiredMemory;
    this.memoryBlock = newMemoryBlock;
  }

  /**
   * Add the index row to unsafe.
   * Below format is used to store data in memory block
   * WRITE:
   * <FD><FD><FD><VO><VO><VO><LO><VD><VD><VD>
   * FD: Fixed Column data
   * VO: Variable column data offset
   * VD: Variable column data
   * LO: Last Offset
   *
   * Read:
   * FD: Read directly based of byte postion added in CarbonRowSchema
   *
   * VD: Read based on below logic
   * if not last variable column schema
   * X = read actual variable column offset based on byte postion added in CarbonRowSchema
   * Y = read next variable column offset (next 4 bytes)
   * get the length
   * len  = (X-Y)
   * read data from offset X of size len
   *
   * if last variable column
   * X = read actual variable column offset based on byte postion added in CarbonRowSchema
   * Y = read last offset (next 4 bytes)
   * get the length
   * len  = (X-Y)
   * read data from offset X of size len
   *
   * @param indexRow
   */
  public void addIndexRow(CarbonRowSchema[] schema, DataMapRow indexRow) {
    // First calculate the required memory to keep the row in unsafe
    int rowSize = indexRow.getTotalSizeInBytes();
    // Check whether allocated memory is sufficient or not.
    ensureSize(rowSize);
    int pointer = runningLength;
    int bytePosition = 0;
    for (int i = 0; i < schema.length; i++) {
      switch (schema[i].getSchemaType()) {
        case STRUCT:
          CarbonRowSchema[] childSchemas =
              ((CarbonRowSchema.StructCarbonRowSchema) schema[i]).getChildSchemas();
          for (int j = 0; j < childSchemas.length; j++) {
            if (childSchemas[j].getBytePosition() > bytePosition) {
              bytePosition = childSchemas[j].getBytePosition();
            }
          }
          break;
        default:
          if (schema[i].getBytePosition() > bytePosition) {
            bytePosition = schema[i].getBytePosition();
          }
      }
    }
    // byte position of Last offset
    bytePosition += CarbonCommonConstants.INT_SIZE_IN_BYTE;
    // start byte position of variable length data
    int varColPosition = bytePosition + CarbonCommonConstants.INT_SIZE_IN_BYTE;
    // current position refers to current byte postion in memory block
    int currentPosition;
    for (int i = 0; i < schema.length; i++) {
      switch (schema[i].getSchemaType()) {
        case STRUCT:
          CarbonRowSchema[] childSchemas =
              ((CarbonRowSchema.StructCarbonRowSchema) schema[i]).getChildSchemas();
          DataMapRow row = indexRow.getRow(i);
          for (int j = 0; j < childSchemas.length; j++) {
            currentPosition = addToUnsafe(childSchemas[j], row, j, pointer, varColPosition);
            if (currentPosition > 0) {
              varColPosition = currentPosition;
            }
          }
          break;
        default:
          currentPosition = addToUnsafe(schema[i], indexRow, i, pointer, varColPosition);
          if (currentPosition > 0) {
            varColPosition = currentPosition;
          }
          break;
      }
    }
    // writting the last offset
    getUnsafe()
        .putInt(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + pointer + bytePosition,
            varColPosition);
    // after adding last offset increament the length by 4 bytes as last postion
    // written as INT
    runningLength += CarbonCommonConstants.INT_SIZE_IN_BYTE;
    pointers[rowCount++] = pointer;
  }

  private int addToUnsafe(CarbonRowSchema schema, DataMapRow row, int index, int startOffset,
      int varPosition) {
    switch (schema.getSchemaType()) {
      case FIXED:
        DataType dataType = schema.getDataType();
        if (dataType == DataTypes.BYTE) {
          getUnsafe().putByte(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getByte(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.BOOLEAN) {
          getUnsafe().putBoolean(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getBoolean(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.SHORT) {
          getUnsafe().putShort(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getShort(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.INT) {
          getUnsafe().putInt(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getInt(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.LONG) {
          getUnsafe().putLong(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getLong(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.FLOAT) {
          getUnsafe().putFloat(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getFloat(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.DOUBLE) {
          getUnsafe().putDouble(memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(),
              row.getDouble(index));
          runningLength += row.getSizeInBytes(index);
        } else if (dataType == DataTypes.BYTE_ARRAY) {
          byte[] data = row.getByteArray(index);
          getUnsafe().copyMemory(data, BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
              memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(), data.length);
          runningLength += row.getSizeInBytes(index);
        } else {
          throw new UnsupportedOperationException(
              "unsupported data type for unsafe storage: " + schema.getDataType());
        }
        return 0;
      case VARIABLE_SHORT:
      case VARIABLE_INT:
        byte[] data = row.getByteArray(index);
        getUnsafe().putInt(memoryBlock.getBaseObject(),
            memoryBlock.getBaseOffset() + startOffset + schema.getBytePosition(), varPosition);
        runningLength += 4;
        if (data != null) {
          getUnsafe().copyMemory(data, BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
                  memoryBlock.getBaseOffset() + startOffset + varPosition, data.length);
          runningLength += data.length;
          varPosition += data.length;
        }
        return varPosition;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type for unsafe storage: " + schema.getDataType());
    }
  }

  public DataMapRow getDataMapRow(CarbonRowSchema[] schema, int index) {
    assert (index < rowCount);
    return new UnsafeDataMapRow(schema, memoryBlock, pointers[index]);
  }

  public void finishWriting() {
    if (runningLength < allocatedSize) {
      MemoryBlock allocate =
          UnsafeMemoryManager.allocateMemoryWithRetry(MemoryType.ONHEAP, taskId, runningLength);
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

  public int getRowCount() {
    return rowCount;
  }

  public void serializeMemoryBlock() {
    this.data = new byte[runningLength];
    CarbonUnsafe.getUnsafe().copyMemory(memoryBlock.getBaseObject(),
        memoryBlock.getBaseOffset(), data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, data.length);
    freeMemory();
    isSerialized = true;
  }

  public void copyToMemoryBlock() {
    this.memoryBlock =
        UnsafeMemoryManager.allocateMemoryWithRetry(MemoryType.ONHEAP, taskId, this.data.length);
    isMemoryFreed = false;
    CarbonUnsafe.getUnsafe()
        .copyMemory(data, CarbonUnsafe.BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
            memoryBlock.getBaseOffset(), this.data.length);
    isSerialized = false;
    this.data = null;
  }
}