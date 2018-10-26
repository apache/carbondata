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

import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

public abstract class VarLengthColumnPageBase extends ColumnPage {

  static final int byteBits = DataTypes.BYTE.getSizeBits();
  static final int shortBits = DataTypes.SHORT.getSizeBits();
  static final int intBits = DataTypes.INT.getSizeBits();
  static final int longBits = DataTypes.LONG.getSizeBits();
  // default size for each row, grows as needed
  static final int DEFAULT_ROW_SIZE = 8;

  static final double FACTOR = 1.25;

  final String taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  // memory allocated by Unsafe
  MemoryBlock memoryBlock;

  // base address of memoryBlock
  Object baseAddress;

  // the offset of row in the unsafe memory, its size is pageSize + 1
  ColumnPage rowOffset;

  // the length of bytes added in the page
  int totalLength;

  // base offset of memoryBlock
  long baseOffset;

  // size of the allocated memory, in bytes
  int capacity;

  VarLengthColumnPageBase(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    TableSpec.ColumnSpec spec = TableSpec.ColumnSpec.newInstance(
        columnPageEncoderMeta.getColumnSpec().getFieldName(), DataTypes.INT, ColumnType.MEASURE);
    try {
      rowOffset = ColumnPage.newPage(
          new ColumnPageEncoderMeta(spec, DataTypes.INT, columnPageEncoderMeta.getCompressorName()),
          pageSize);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    totalLength = 0;
  }

  @Override
  public void setBytePage(byte[] byteData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setShortPage(short[] shortData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setIntPage(int[] intData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setLongPage(long[] longData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  /**
   * Create a new column page for decimal page
   */
  public static ColumnPage newDecimalColumnPage(ColumnPageEncoderMeta meta,
      byte[] lvEncodedBytes) throws MemoryException {
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    DecimalConverterFactory.DecimalConverter decimalConverter =
        DecimalConverterFactory.INSTANCE.getDecimalConverter(columnSpec.getPrecision(),
            columnSpec.getScale());
    int size = decimalConverter.getSize();
    if (size < 0) {
      return getLVBytesColumnPage(columnSpec, lvEncodedBytes,
          DataTypes.createDecimalType(columnSpec.getPrecision(), columnSpec.getScale()),
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else {
      // Here the size is always fixed.
      return getDecimalColumnPage(meta, lvEncodedBytes, size);
    }
  }

  /**
   * Create a new column page based on the LV (Length Value) encoded bytes
   */
  static ColumnPage newLVBytesColumnPage(TableSpec.ColumnSpec columnSpec, byte[] lvEncodedBytes,
      int lvLength, String compressorName) throws MemoryException {
    return getLVBytesColumnPage(columnSpec, lvEncodedBytes, DataTypes.BYTE_ARRAY,
        lvLength, compressorName);
  }

  /**
   * Create a new column page based on the LV (Length Value) encoded bytes
   */
  static ColumnPage newComplexLVBytesColumnPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedBytes, int lvLength, String compressorName) throws MemoryException {
    return getComplexLVBytesColumnPage(columnSpec, lvEncodedBytes, DataTypes.BYTE_ARRAY,
        lvLength, compressorName);
  }

  private static ColumnPage getDecimalColumnPage(ColumnPageEncoderMeta meta,
      byte[] lvEncodedBytes, int size) throws MemoryException {
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    String compressorName = meta.getCompressorName();
    TableSpec.ColumnSpec spec = TableSpec.ColumnSpec
        .newInstance(columnSpec.getFieldName(), DataTypes.INT, ColumnType.MEASURE);
    ColumnPage rowOffset = ColumnPage.newPage(
        new ColumnPageEncoderMeta(spec, DataTypes.INT, compressorName),
        CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
    int offset;
    int rowId = 0;
    int counter = 0;
    for (offset = 0; offset < lvEncodedBytes.length; offset += size) {
      rowOffset.putInt(counter, offset);
      rowId++;
      counter++;
    }
    rowOffset.putInt(counter, offset);

    VarLengthColumnPageBase page;
    if (isUnsafeEnabled(meta)) {
      page = new UnsafeDecimalColumnPage(
          new ColumnPageEncoderMeta(columnSpec, columnSpec.getSchemaDataType(), compressorName),
          rowId);
    } else {
      page = new SafeDecimalColumnPage(
          new ColumnPageEncoderMeta(columnSpec, columnSpec.getSchemaDataType(), compressorName),
          rowId);
    }

    // set total length and rowOffset in page
    page.totalLength = offset;
    page.rowOffset.freeMemory();
    page.rowOffset = rowOffset;
    for (int i = 0; i < rowId; i++) {
      page.putBytes(i, lvEncodedBytes, i * size, size);
    }
    return page;
  }

  private static ColumnPage getLVBytesColumnPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedBytes, DataType dataType, int lvLength, String compressorName)
      throws MemoryException {
    // extract length and data, set them to rowOffset and unsafe memory correspondingly
    int rowId = 0;
    TableSpec.ColumnSpec spec = TableSpec.ColumnSpec
        .newInstance(columnSpec.getFieldName(), DataTypes.INT, ColumnType.MEASURE);
    ColumnPage rowOffset = ColumnPage.newPage(
        new ColumnPageEncoderMeta(spec, DataTypes.INT, compressorName),
        CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
    int length;
    int offset;
    int lvEncodedOffset = 0;
    int counter = 0;
    // extract Length field in input and calculate total length
    for (offset = 0; lvEncodedOffset < lvEncodedBytes.length; offset += length) {
      length = ByteUtil.toInt(lvEncodedBytes, lvEncodedOffset);
      rowOffset.putInt(counter, offset);
      lvEncodedOffset += lvLength + length;
      rowId++;
      counter++;
    }
    rowOffset.putInt(counter, offset);
    return getVarLengthColumnPage(columnSpec, lvEncodedBytes, dataType,
        lvLength, rowId, rowOffset, offset, compressorName);
  }

  private static ColumnPage getComplexLVBytesColumnPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedBytes, DataType dataType, int lvLength, String compressorName)
      throws MemoryException {
    // extract length and data, set them to rowOffset and unsafe memory correspondingly
    int rowId = 0;
    TableSpec.ColumnSpec spec = TableSpec.ColumnSpec
        .newInstance(columnSpec.getFieldName(), DataTypes.INT, ColumnType.MEASURE);
    ColumnPage rowOffset = ColumnPage.newPage(
        new ColumnPageEncoderMeta(spec, DataTypes.INT, compressorName),
        CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
    int length;
    int offset;
    int lvEncodedOffset = 0;
    int counter = 0;
    // extract Length field in input and calculate total length
    for (offset = 0; lvEncodedOffset < lvEncodedBytes.length; offset += length) {
      length = ByteUtil.toShort(lvEncodedBytes, lvEncodedOffset);
      rowOffset.putInt(counter, offset);
      lvEncodedOffset += lvLength + length;
      rowId++;
      counter++;
    }
    rowOffset.putInt(counter, offset);

    return getVarLengthColumnPage(columnSpec, lvEncodedBytes, dataType,
        lvLength, rowId, rowOffset, offset, compressorName);
  }

  private static VarLengthColumnPageBase getVarLengthColumnPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedBytes, DataType dataType, int lvLength, int rowId, ColumnPage rowOffset,
      int offset, String compressorName) throws MemoryException {
    int lvEncodedOffset;
    int length;
    int numRows = rowId;

    VarLengthColumnPageBase page;
    int inputDataLength = offset;
    if (unsafe) {
      page = new UnsafeDecimalColumnPage(
          new ColumnPageEncoderMeta(columnSpec, dataType, compressorName), numRows,
          inputDataLength);
    } else {
      page = new SafeDecimalColumnPage(
          new ColumnPageEncoderMeta(columnSpec, dataType, compressorName), numRows);
    }

    // set total length and rowOffset in page
    page.totalLength = offset;
    page.rowOffset.freeMemory();
    page.rowOffset = rowOffset;

    // set data in page
    lvEncodedOffset = 0;
    for (int i = 0; i < numRows; i++) {
      length = rowOffset.getInt(i + 1) - rowOffset.getInt(i);
      page.putBytes(i, lvEncodedBytes, lvEncodedOffset + lvLength, length);
      lvEncodedOffset += lvLength + length;
    }
    return page;
  }

  @Override
  public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putShortInt(int rowId, int value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putInt(int rowId, int value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putLong(int rowId, long value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  abstract void putBytesAtRow(int rowId, byte[] bytes);

  @Override
  public void putBytes(int rowId, byte[] bytes) {
    // rowId * 4 represents the length of L in LV
    if (bytes.length > (Integer.MAX_VALUE - totalLength - rowId * 4)) {
      // since we later store a column page in a byte array, so its maximum size is 2GB
      throw new RuntimeException("Carbondata only support maximum 2GB size for one column page,"
          + " exceed this limit at rowId " + rowId);
    }
    if (rowId == 0) {
      rowOffset.putInt(0, 0);
    }
    rowOffset.putInt(rowId + 1, rowOffset.getInt(rowId) + bytes.length);
    putBytesAtRow(rowId, bytes);
    totalLength += bytes.length;
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int getShortInt(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public short[] getShortPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getShortIntPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int[] getIntPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public long[] getLongPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public float[] getFloatPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public double[] getDoublePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getDecimalPage() {
    // output LV encoded byte array
    int offset = 0;
    byte[] data = new byte[totalLength];
    for (int rowId = 0; rowId < pageSize; rowId++) {
      int length = rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId);
      copyBytes(rowId, data, offset, length);
      offset += length;
    }
    return data;
  }

  /**
   * Copy `length` bytes from data at rowId to dest start from destOffset
   */
  abstract void copyBytes(int rowId, byte[] dest, int destOffset, int length);

  @Override
  public byte[] getLVFlattenedBytePage() throws IOException {
    // output LV encoded byte array
    int offset = 0;
    byte[] data = new byte[totalLength + ((rowOffset.getActualRowCount() - 1) * 4)];
    for (int rowId = 0; rowId < rowOffset.getActualRowCount() - 1; rowId++) {
      int length = rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId);
      ByteUtil.setInt(data, offset, length);
      copyBytes(rowId, data, offset + 4, length);
      offset += 4 + length;
    }
    return data;
  }

  @Override public byte[] getComplexChildrenLVFlattenedBytePage() throws IOException {
    // output LV encoded byte array
    int offset = 0;
    byte[] data = new byte[totalLength + ((rowOffset.getActualRowCount() - 1) * 2)];
    for (int rowId = 0; rowId < rowOffset.getActualRowCount() - 1; rowId++) {
      short length = (short) (rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId));
      ByteUtil.setShort(data, offset, length);
      copyBytes(rowId, data, offset + 2, length);
      offset += 2 + length;
    }
    return data;
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() throws IOException {
    // output LV encoded byte array
    int offset = 0;
    byte[] data = new byte[totalLength];
    for (int rowId = 0; rowId < rowOffset.getActualRowCount() - 1; rowId++) {
      int length =  (rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId));
      copyBytes(rowId, data, offset, length);
      offset += length;
    }
    return data;
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  /**
   * reallocate memory if capacity length than current size + request size
   */
  protected void ensureMemory(int requestSize) throws MemoryException {
    if (totalLength + requestSize > capacity) {
      int newSize = Math.max(2 * capacity, totalLength + requestSize);
      MemoryBlock newBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, newSize);
      CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
          newBlock.getBaseObject(), newBlock.getBaseOffset(), capacity);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = newBlock;
      baseAddress = newBlock.getBaseObject();
      baseOffset = newBlock.getBaseOffset();
      capacity = newSize;
    }
  }

  /**
   * free memory as needed
   */
  public void freeMemory() {
    if (null != rowOffset) {
      rowOffset.freeMemory();
      rowOffset = null;
    }
  }

  @Override
  public long getPageLengthInBytes() throws IOException {
    return totalLength;
  }
}
