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

package org.apache.carbondata.core.datastore.page;

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;

/**
 * This extension uses unsafe memory to store page data, for variable length data type (string)
 */
public class UnsafeVarLengthColumnPage extends VarLengthColumnPageBase {

  /**
   * create a page
   */
  UnsafeVarLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize)
      throws MemoryException {
    super(columnPageEncoderMeta, pageSize);
    capacity = (int) (pageSize * DEFAULT_ROW_SIZE * FACTOR);
    memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, (long) (capacity));
    baseAddress = memoryBlock.getBaseObject();
    baseOffset = memoryBlock.getBaseOffset();
  }

  @Override
  public void freeMemory() {
    if (memoryBlock != null) {
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = null;
      baseAddress = null;
      baseOffset = 0;
      super.freeMemory();
    }
  }

  @Override
  public void putBytesAtRow(int rowId, byte[] bytes) {
    putBytes(rowId, bytes, 0, bytes.length);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    try {
      ensureMemory(length);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    CarbonUnsafe.getUnsafe().copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET + offset,
        baseAddress, baseOffset + rowOffset.getInt(rowId), length);
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    if (totalLength != 0) {
      throw new IllegalStateException("page is not empty");
    }
    for (int i = 0; i < byteArray.length; i++) {
      putBytes(i, byteArray[i]);
    }
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {

  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytes(int rowId) {
    int length = rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId);
    byte[] bytes = new byte[length];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset.getInt(rowId),
        bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return bytes;
  }

  @Override
  public byte[][] getByteArrayPage() {
    byte[][] bytes = new byte[rowOffset.getActualRowCount() - 1][];
    for (int rowId = 0; rowId < rowOffset.getActualRowCount() - 1; rowId++) {
      int length = rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId);
      byte[] rowData = new byte[length];
      CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset.getInt(rowId),
          rowData, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
      bytes[rowId] = rowData;
    }
    return bytes;
  }

  @Override
  void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset.getInt(rowId),
        dest, CarbonUnsafe.BYTE_ARRAY_OFFSET + destOffset, length);
  }

}
