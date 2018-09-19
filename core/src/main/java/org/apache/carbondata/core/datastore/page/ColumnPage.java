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
import java.math.BigDecimal;
import java.util.BitSet;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.localdictionary.PageLevelDictionary;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;

public abstract class ColumnPage {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      ColumnPage.class.getName());
  // number of row in this page
  protected int pageSize;

  protected ColumnPageEncoderMeta columnPageEncoderMeta;

  // The index of the rowId whose value is null, will be set to 1
  protected BitSet nullBitSet;

  // statistics collector for this column page
  protected ColumnPageStatsCollector statsCollector;

  protected static final boolean unsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
          CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT));

  /**
   * Create a new column page with input data type and page size.
   */
  protected ColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    this.pageSize = pageSize;
    this.nullBitSet = new BitSet(pageSize);
    this.columnPageEncoderMeta = columnPageEncoderMeta;
  }

  public DataType getDataType() {
    return columnPageEncoderMeta.getStoreDataType();
  }

  public SimpleStatsResult getStatistics() {
    return statsCollector.getPageStats();
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setStatsCollector(ColumnPageStatsCollector statsCollector) {
    this.statsCollector = statsCollector;
  }

  public abstract void setFlattenContentInBytes(byte[] flattenContentInBytes);

  public abstract byte[] getFlattenContentInBytes();

  /**
   * Set byte array to page
   */
  public abstract void setByteArrayPage(byte[][] byteArray);

  /**
   * free memory as needed
   */
  public abstract void freeMemory();

  /**
   * Set value at rowId
   */
  public void putData(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
      statsCollector.updateNull(rowId);
      nullBitSet.set(rowId);
      return;
    }
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      if (columnPageEncoderMeta.getColumnSpec().getSchemaDataType() == DataTypes.BOOLEAN) {
        value = BooleanConvert.boolean2Byte((Boolean) value);
      }
      putByte(rowId, (byte) value);
      statsCollector.update((byte) value);
    } else if (dataType == DataTypes.SHORT) {
      putShort(rowId, (short) value);
      statsCollector.update((short) value);
    } else if (dataType == DataTypes.INT) {
      putInt(rowId, (int) value);
      statsCollector.update((int) value);
    } else if (dataType == DataTypes.LONG) {
      putLong(rowId, (long) value);
      statsCollector.update((long) value);
    } else if (dataType == DataTypes.DOUBLE) {
      putDouble(rowId, (double) value);
      statsCollector.update((double) value);
    } else if (DataTypes.isDecimal(dataType)) {
      putDecimal(rowId, (BigDecimal) value);
      statsCollector.update((BigDecimal) value);
    } else if (dataType == DataTypes.STRING
        || dataType == DataTypes.BYTE_ARRAY
        || dataType == DataTypes.VARCHAR) {
      putBytes(rowId, (byte[]) value);
      statsCollector.update((byte[]) value);
    } else if (dataType == DataTypes.FLOAT) {
      putFloat(rowId, (float) value);
      statsCollector.update((float) value);
    } else {
      throw new RuntimeException("unsupported data type: " + dataType);
    }
  }

  /**
   * get value at rowId, note that the value of string&bytes is LV format
   * @param rowId rowId
   * @return value
   */
  public Object getData(int rowId) {
    if (nullBitSet.get(rowId)) {
      return getNull(rowId);
    }
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      byte value = getByte(rowId);
      if (columnPageEncoderMeta.getColumnSpec().getSchemaDataType() == DataTypes.BOOLEAN) {
        return BooleanConvert.byte2Boolean(value);
      }
      return value;
    } else if (dataType == DataTypes.SHORT) {
      return getShort(rowId);
    } else if (dataType == DataTypes.INT) {
      return getInt(rowId);
    } else if (dataType == DataTypes.LONG) {
      return getLong(rowId);
    } else if (dataType == DataTypes.DOUBLE) {
      return getDouble(rowId);
    } else if (DataTypes.isDecimal(dataType)) {
      return getDecimal(rowId);
    } else if (dataType == DataTypes.STRING
        || dataType == DataTypes.BYTE_ARRAY
        || dataType == DataTypes.VARCHAR) {
      return getBytes(rowId);
    } else {
      throw new RuntimeException("unsupported data type: " + dataType);
    }
  }

  /**
   * Set byte value at rowId
   */
  public abstract void putByte(int rowId, byte value);

  /**
   * Set short value at rowId
   */
  public abstract void putShort(int rowId, short value);

  /**
   * Set integer value at rowId
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Set long value at rowId
   */
  public abstract void putLong(int rowId, long value);

  /**
   * Set double value at rowId
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Set float value at rowId
   */
  public abstract void putFloat(int rowId, float value);

  /**
   * Set byte array value at rowId
   */
  public abstract void putBytes(int rowId, byte[] bytes);

  /**
   * Set byte array value at rowId
   */
  public abstract void putDecimal(int rowId, BigDecimal decimal);

  /**
   * Type cast int value to 3 bytes value and set at rowId
   */
  public abstract void putShortInt(int rowId, int value);

  /**
   * Set boolean value at rowId
   */
  public void putBoolean(int rowId, boolean value) {
    putByte(rowId, BooleanConvert.boolean2Byte(value));
  }

  /**
   * Set byte array from offset to length at rowId
   */
  public abstract void putBytes(int rowId, byte[] bytes, int offset, int length);


  /**
   * Set null at rowId
   */
  protected void putNull(int rowId) {
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BOOLEAN) {
      putBoolean(rowId, false);
    } else if (dataType == DataTypes.BYTE) {
      putByte(rowId, (byte) 0);
    } else if (dataType == DataTypes.SHORT) {
      putShort(rowId, (short) 0);
    } else if (dataType == DataTypes.INT) {
      putInt(rowId, 0);
    } else if (dataType == DataTypes.LONG) {
      putLong(rowId, 0L);
    } else if (dataType == DataTypes.DOUBLE) {
      putDouble(rowId, 0.0);
    } else if (dataType == DataTypes.FLOAT) {
      putFloat(rowId, 0.0f);
    } else if (DataTypes.isDecimal(dataType)) {
      putDecimal(rowId, BigDecimal.ZERO);
    } else {
      throw new IllegalArgumentException("unsupported data type: " + dataType);
    }
  }

  /**
   * Get null at rowId
   */
  private Object getNull(int rowId) {
    Object result;
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BOOLEAN) {
      result = getBoolean(rowId);
    } else if (dataType == DataTypes.BYTE) {
      result = getByte(rowId);
      if (columnPageEncoderMeta.getColumnSpec().getSchemaDataType() == DataTypes.BOOLEAN) {
        result = BooleanConvert.byte2Boolean((byte)result);
      }
    } else if (dataType == DataTypes.SHORT) {
      result = getShort(rowId);
    } else if (dataType == DataTypes.INT) {
      result = getInt(rowId);
    } else if (dataType == DataTypes.LONG) {
      result = getLong(rowId);
    } else if (dataType == DataTypes.DOUBLE) {
      result = getDouble(rowId);
    } else if (DataTypes.isDecimal(dataType)) {
      result = getDecimal(rowId);
    } else {
      throw new IllegalArgumentException("unsupported data type: " + dataType);
    }
    return result;
  }

  /**
   * Get byte value at rowId
   */
  public abstract byte getByte(int rowId);

  /**
   * Get short value at rowId
   */
  public abstract short getShort(int rowId);

  /**
   * Get short int value at rowId
   */
  public abstract int getShortInt(int rowId);

  /**
   * Get boolean value at rowId
   */
  public boolean getBoolean(int rowId) {
    return BooleanConvert.byte2Boolean(getByte(rowId));
  }

  /**
   * Get int value at rowId
   */
  public abstract int getInt(int rowId);

  /**
   * Get long value at rowId
   */
  public abstract long getLong(int rowId);

  /**
   * Get float value at rowId
   */
  public abstract float getFloat(int rowId);

  /**
   * Get double value at rowId
   */
  public abstract double getDouble(int rowId);

  /**
   * Get decimal value at rowId
   */
  public abstract BigDecimal getDecimal(int rowId);

  /**
   * Get byte array at rowId
   */
  public abstract byte[] getBytes(int rowId);

  /**
   * Get variable length page data
   */
  public abstract byte[][] getByteArrayPage();

  /**
   * For variable length page, get the flattened data
   */
  public abstract byte[] getLVFlattenedBytePage() throws IOException;

  /**
   * For complex type columns
   * @return
   * @throws IOException
   */
  public abstract byte[] getComplexChildrenLVFlattenedBytePage() throws IOException;

  /**
   * For complex type columns
   * @return
   * @throws IOException
   */
  public abstract byte[] getComplexParentFlattenedBytePage() throws IOException;

  /**
   * For decimals
   */
  public abstract byte[] getDecimalPage();

  /**
   * Encode the page data by codec (Visitor)
   */
  public abstract void convertValue(ColumnPageValueConverter codec);

  public PageLevelDictionary getPageDictionary() {
    throw new UnsupportedOperationException("Operation Not Supported");
  }

  /**
   * Compress page data using specified compressor
   */
  public byte[] compress(Compressor compressor) throws MemoryException, IOException {
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BOOLEAN ||
        dataType == DataTypes.BYTE ||
        dataType == DataTypes.SHORT ||
        dataType == DataTypes.SHORT_INT ||
        dataType == DataTypes.INT ||
        dataType == DataTypes.LONG ||
        dataType == DataTypes.FLOAT ||
        dataType == DataTypes.DOUBLE) {
      return compressor.compressByte(getFlattenContentInBytes());
    } else if (DataTypes.isDecimal(dataType)) {
      return compressor.compressByte(getDecimalPage());
    } else if (dataType == DataTypes.BYTE_ARRAY
        && columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      return compressor.compressByte(getComplexChildrenLVFlattenedBytePage());
    } else if (dataType == DataTypes.BYTE_ARRAY
        && (columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_STRUCT
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_ARRAY
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.PLAIN_LONG_VALUE
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.PLAIN_VALUE)) {
      return compressor.compressByte(getComplexParentFlattenedBytePage());
    } else if (dataType == DataTypes.BYTE_ARRAY) {
      return compressor.compressByte(getLVFlattenedBytePage());
    } else {
      throw new UnsupportedOperationException("unsupport compress column page: " + dataType);
    }
  }

  public BitSet getNullBits() {
    return nullBitSet;
  }

  public void setNullBits(BitSet nullBitSet) {
    this.nullBitSet = nullBitSet;
  }

  public TableSpec.ColumnSpec getColumnSpec() {
    return columnPageEncoderMeta.getColumnSpec();
  }

  public boolean isLocalDictGeneratedPage() {
    return false;
  }

  public void disableLocalDictEncoding() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public PageLevelDictionary getColumnPageDictionary() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public int getActualRowCount() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public String getColumnCompressorName() {
    return columnPageEncoderMeta.getCompressorName();
  }

  public ColumnPageEncoderMeta getColumnPageEncoderMeta() {
    return columnPageEncoderMeta;
  }
}
