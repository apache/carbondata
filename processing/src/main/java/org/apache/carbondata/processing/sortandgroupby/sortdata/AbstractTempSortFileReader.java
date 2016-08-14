/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.impl.MemoryMappedFileHolderImpl;

public abstract class AbstractTempSortFileReader implements TempSortFileReader {
  /**
   * measure count
   */
  protected int measureCount;

  /**
   * Measure count
   */
  protected int dimensionCount;

  /**
   * complexDimension count
   */
  protected int complexDimensionCount;

  /**
   * entryCount
   */
  protected int entryCount;

  /**
   * fileHolder
   */
  protected FileHolder fileHolder;

  /**
   * Temp file path
   */
  protected String filePath;

  /**
   * eachRecordSize
   */
  protected int eachRecordSize;

  /**
   * AbstractTempSortFileReader
   *
   * @param measureCount
   * @param dimensionCount
   * @param tempFile
   */
  public AbstractTempSortFileReader(int dimensionCount, int complexDimensionCount, int measureCount,
      File tempFile, int noDictionaryCount) {
    this.measureCount = measureCount;
    this.dimensionCount = dimensionCount;
    this.complexDimensionCount = complexDimensionCount;
    this.fileHolder = new MemoryMappedFileHolderImpl(1);
    this.filePath = tempFile.getAbsolutePath();
    entryCount = fileHolder.readInt(filePath);
    eachRecordSize = dimensionCount + complexDimensionCount + measureCount;
  }

  /**
   * below method will be used to close the file holder
   */
  public void finish() {
    this.fileHolder.finish();
  }

  /**
   * Below method will be used to get the total row count in temp file
   *
   * @return
   */
  public int getEntryCount() {
    return entryCount;
  }

  /**
   * Below method will be used to get the row
   */
  public abstract Object[][] getRow();

  protected Object[][] prepareRecordFromByteBuffer(int recordLength, byte[] byteArrayFromFile) {
    Object[][] records = new Object[recordLength][];
    Object[] record = null;
    ByteBuffer buffer = ByteBuffer.allocate(byteArrayFromFile.length);

    buffer.put(byteArrayFromFile);
    buffer.rewind();

    int index = 0;
    byte b = 0;

    for (int i = 0; i < recordLength; i++) {
      record = new Object[eachRecordSize];
      index = 0;

      for (int j = 0; j < dimensionCount; j++) {
        record[index++] = buffer.getInt();
      }

      for (int j = 0; j < complexDimensionCount; j++) {
        byte[] complexByteArray = new byte[buffer.getInt()];
        buffer.get(complexByteArray);
        record[index++] = complexByteArray;
      }

      for (int j = 0; j < measureCount; j++) {
        b = buffer.get();
        if (b == 1) {
          record[index++] = buffer.getDouble();
        } else {
          record[index++] = null;
        }
      }

      records[i] = record;
    }
    return records;
  }
}
