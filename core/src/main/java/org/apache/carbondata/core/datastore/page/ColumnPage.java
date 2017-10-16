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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;

import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE_ARRAY;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.DECIMAL;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.FLOAT;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.INT;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT_INT;

public abstract class ColumnPage {

  // number of row in this page
  protected final int pageSize;

  // data type of the page storage
  protected final DataType dataType;

  // specification of this column
  private final TableSpec.ColumnSpec columnSpec;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  // statistics collector for this column page
  private ColumnPageStatsCollector statsCollector;

  protected static final boolean unsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING,
          CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING_DEFAULT));

  /**
   * Create a new column page with input data type and page size.
   */
  protected ColumnPage(TableSpec.ColumnSpec columnSpec, DataType dataType, int pageSize) {
    this.columnSpec = columnSpec;
    this.dataType = dataType;
    this.pageSize = pageSize;
    this.nullBitSet = new BitSet(pageSize);
  }

  public DataType getDataType() {
    return dataType;
  }

  private static final SimpleStatsResult statsForComplexType = new SimpleStatsResult() {
    @Override public Object getMin() {
      return new byte[0];
    }

    @Override public Object getMax() {
      return new byte[0];
    }

    @Override public int getDecimalCount() {
      return 0;
    }

    @Override public DataType getDataType() {
      return BYTE_ARRAY;
    }

    @Override public int getScale() {
      return 0;
    }

    @Override public int getPrecision() {
      return 0;
    }
  };

  public SimpleStatsResult getStatistics() {
    if (statsCollector != null) {
      return statsCollector.getPageStats();
    } else {
      // TODO: for sub column of complex type, there no stats yet, return a dummy result
      return statsForComplexType;
    }
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setStatsCollector(ColumnPageStatsCollector statsCollector) {
    this.statsCollector = statsCollector;
  }

  private static ColumnPage createDecimalPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize) {
    if (unsafe) {
      try {
        return new UnsafeDecimalColumnPage(columnSpec, dataType, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeDecimalColumnPage(columnSpec, dataType, pageSize);
    }
  }

  private static ColumnPage createVarLengthPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize) {
    if (unsafe) {
      try {
        return new UnsafeVarLengthColumnPage(columnSpec, dataType, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeVarLengthColumnPage(columnSpec, dataType, pageSize);
    }
  }

  private static ColumnPage createFixLengthPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize) {
    if (unsafe) {
      try {
        return new UnsafeFixLengthColumnPage(columnSpec, dataType, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeFixLengthColumnPage(columnSpec, dataType, pageSize);
    }
  }

  private static ColumnPage createPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize) {
    if (dataType.equals(DECIMAL)) {
      return createDecimalPage(columnSpec, dataType, pageSize);
    } else if (dataType.equals(BYTE_ARRAY)) {
      return createVarLengthPage(columnSpec, dataType, pageSize);
    } else {
      return createFixLengthPage(columnSpec, dataType, pageSize);
    }
  }

  public static ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize)
    throws MemoryException {
    return newPage(columnSpec, dataType, pageSize);
  }

  /**
   * Create a new page of dataType and number of row = pageSize
   */
  public static ColumnPage newPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize) throws MemoryException {
    ColumnPage instance;
    if (unsafe) {
      if (dataType == DataTypes.BOOLEAN) {
        instance = new UnsafeFixLengthColumnPage(columnSpec, BYTE, pageSize);
      } else if (dataType == DataTypes.BYTE ||
          dataType == DataTypes.SHORT ||
          dataType == DataTypes.SHORT_INT ||
          dataType == DataTypes.INT ||
          dataType == DataTypes.LONG ||
          dataType == DataTypes.FLOAT ||
          dataType == DataTypes.DOUBLE) {
        instance = new UnsafeFixLengthColumnPage(columnSpec, dataType, pageSize);
      } else if (dataType == DataTypes.DECIMAL) {
        instance = new UnsafeDecimalColumnPage(columnSpec, dataType, pageSize);
      } else if (dataType == DataTypes.STRING || dataType == DataTypes.BYTE_ARRAY) {
        instance = new UnsafeVarLengthColumnPage(columnSpec, dataType, pageSize);
      } else {
        throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    } else {
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
        instance = newBytePage(columnSpec, new byte[pageSize]);
      } else if (dataType == DataTypes.SHORT) {
        instance = newShortPage(columnSpec, new short[pageSize]);
      } else if (dataType == DataTypes.SHORT_INT) {
        instance = newShortIntPage(columnSpec, new byte[pageSize * 3]);
      } else if (dataType == DataTypes.INT) {
        instance = newIntPage(columnSpec, new int[pageSize]);
      } else if (dataType == DataTypes.LONG) {
        instance = newLongPage(columnSpec, new long[pageSize]);
      } else if (dataType == DataTypes.FLOAT) {
        instance = newFloatPage(columnSpec, new float[pageSize]);
      } else if (dataType == DataTypes.DOUBLE) {
        instance = newDoublePage(columnSpec, new double[pageSize]);
      } else if (dataType == DataTypes.DECIMAL) {
        instance = newDecimalPage(columnSpec, new byte[pageSize][]);
      } else if (dataType == DataTypes.STRING || dataType == DataTypes.BYTE_ARRAY) {
        instance = new SafeVarLengthColumnPage(columnSpec, dataType, pageSize);
      } else {
        throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    }
    return instance;
  }

  public static ColumnPage wrapByteArrayPage(TableSpec.ColumnSpec columnSpec, byte[][] byteArray) {
    ColumnPage columnPage = createPage(columnSpec, BYTE_ARRAY, byteArray.length);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
  }

  private static ColumnPage newBytePage(TableSpec.ColumnSpec columnSpec, byte[] byteData) {
    ColumnPage columnPage = createPage(columnSpec, BYTE, byteData.length);
    columnPage.setBytePage(byteData);
    return columnPage;
  }

  private static ColumnPage newShortPage(TableSpec.ColumnSpec columnSpec, short[] shortData) {
    ColumnPage columnPage = createPage(columnSpec, SHORT, shortData.length);
    columnPage.setShortPage(shortData);
    return columnPage;
  }

  private static ColumnPage newShortIntPage(TableSpec.ColumnSpec columnSpec, byte[] shortIntData) {
    ColumnPage columnPage = createPage(columnSpec, SHORT_INT, shortIntData.length / 3);
    columnPage.setShortIntPage(shortIntData);
    return columnPage;
  }

  private static ColumnPage newIntPage(TableSpec.ColumnSpec columnSpec, int[] intData) {
    ColumnPage columnPage = createPage(columnSpec, INT, intData.length);
    columnPage.setIntPage(intData);
    return columnPage;
  }

  private static ColumnPage newLongPage(TableSpec.ColumnSpec columnSpec, long[] longData) {
    ColumnPage columnPage = createPage(columnSpec, LONG, longData.length);
    columnPage.setLongPage(longData);
    return columnPage;
  }

  private static ColumnPage newFloatPage(TableSpec.ColumnSpec columnSpec, float[] floatData) {
    ColumnPage columnPage = createPage(columnSpec, FLOAT, floatData.length);
    columnPage.setFloatPage(floatData);
    return columnPage;
  }

  private static ColumnPage newDoublePage(TableSpec.ColumnSpec columnSpec, double[] doubleData) {
    ColumnPage columnPage = createPage(columnSpec, DOUBLE, doubleData.length);
    columnPage.setDoublePage(doubleData);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec, byte[][] byteArray) {
    ColumnPage columnPage = createPage(columnSpec, DECIMAL, byteArray.length);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray) throws MemoryException {
    return VarLengthColumnPageBase.newDecimalColumnPage(columnSpec, lvEncodedByteArray);
  }

  private static ColumnPage newLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray) throws MemoryException {
    return VarLengthColumnPageBase.newLVBytesColumnPage(columnSpec, lvEncodedByteArray);
  }

  /**
   * Set byte values to page
   */
  public abstract void setBytePage(byte[] byteData);

  /**
   * Set short values to page
   */
  public abstract void setShortPage(short[] shortData);

  /**
   * Set short int values to page
   */
  public abstract void setShortIntPage(byte[] shortIntData);

  /**
   * Set int values to page
   */
  public abstract void setIntPage(int[] intData);

  /**
   * Set long values to page
   */
  public abstract void setLongPage(long[] longData);

  /**
   * Set float values to page
   */
  public abstract void setFloatPage(float[] floatData);

  /**
   * Set double value to page
   */
  public abstract void setDoublePage(double[] doubleData);

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
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      if (columnSpec.getSchemaDataType() == DataTypes.BOOLEAN) {
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
    } else if (dataType == DataTypes.DECIMAL) {
      putDecimal(rowId, (BigDecimal) value);
      statsCollector.update((BigDecimal) value);
    } else if (dataType == DataTypes.STRING || dataType == DataTypes.BYTE_ARRAY) {
      putBytes(rowId, (byte[]) value);
      statsCollector.update((byte[]) value);
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
  private void putNull(int rowId) {
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
    } else if (dataType == DataTypes.DECIMAL) {
      putDecimal(rowId, BigDecimal.ZERO);
    } else {
      throw new IllegalArgumentException("unsupported data type: " + dataType);
    }
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
   * Get byte value page
   */
  public abstract byte[] getBytePage();

  /**
   * Get short value page
   */
  public abstract short[] getShortPage();

  /**
   * Get short int value page
   */
  public abstract byte[] getShortIntPage();

  /**
   * Get boolean value page
   */
  public byte[] getBooleanPage() {
    return getBytePage();
  }

  /**
   * Get int value page
   */
  public abstract int[] getIntPage();

  /**
   * Get long value page
   */
  public abstract long[] getLongPage();

  /**
   * Get float value page
   */
  public abstract float[] getFloatPage();

  /**
   * Get double value page
   */
  public abstract double[] getDoublePage();

  /**
   * Get variable length page data
   */
  public abstract byte[][] getByteArrayPage();

  /**
   * For variable length page, get the flattened data
   */
  public abstract byte[] getLVFlattenedBytePage() throws IOException;

  /**
   * For decimals
   */
  public abstract byte[] getDecimalPage();

  /**
   * Encode the page data by codec (Visitor)
   */
  public abstract void convertValue(ColumnPageValueConverter codec);

  /**
   * Compress page data using specified compressor
   */
  public byte[] compress(Compressor compressor) throws MemoryException, IOException {
    if (dataType == DataTypes.BOOLEAN) {
      return compressor.compressByte(getBooleanPage());
    } else if (dataType == DataTypes.BYTE) {
      return compressor.compressByte(getBytePage());
    } else if (dataType == DataTypes.SHORT) {
      return compressor.compressShort(getShortPage());
    } else if (dataType == DataTypes.SHORT_INT) {
      return compressor.compressByte(getShortIntPage());
    } else if (dataType == DataTypes.INT) {
      return compressor.compressInt(getIntPage());
    } else if (dataType == DataTypes.LONG) {
      return compressor.compressLong(getLongPage());
    } else if (dataType == DataTypes.FLOAT) {
      return compressor.compressFloat(getFloatPage());
    } else if (dataType == DataTypes.DOUBLE) {
      return compressor.compressDouble(getDoublePage());
    } else if (dataType == DataTypes.DECIMAL) {
      return compressor.compressByte(getDecimalPage());
    } else if (dataType == DataTypes.BYTE_ARRAY) {
      return compressor.compressByte(getLVFlattenedBytePage());
    } else {
      throw new UnsupportedOperationException("unsupport compress column page: " + dataType);
    }
  }

  /**
   * Decompress data and create a column page using the decompressed data,
   * except for decimal page
   */
  public static ColumnPage decompress(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length)
      throws MemoryException {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BOOLEAN || storeDataType == DataTypes.BYTE) {
      byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
      return newBytePage(columnSpec, byteData);
    } else if (storeDataType == DataTypes.SHORT) {
      short[] shortData = compressor.unCompressShort(compressedData, offset, length);
      return newShortPage(columnSpec, shortData);
    } else if (storeDataType == DataTypes.SHORT_INT) {
      byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
      return newShortIntPage(columnSpec, shortIntData);
    } else if (storeDataType == DataTypes.INT) {
      int[] intData = compressor.unCompressInt(compressedData, offset, length);
      return newIntPage(columnSpec, intData);
    } else if (storeDataType == DataTypes.LONG) {
      long[] longData = compressor.unCompressLong(compressedData, offset, length);
      return newLongPage(columnSpec, longData);
    } else if (storeDataType == DataTypes.FLOAT) {
      float[] floatData = compressor.unCompressFloat(compressedData, offset, length);
      return newFloatPage(columnSpec, floatData);
    } else if (storeDataType == DataTypes.DOUBLE) {
      double[] doubleData = compressor.unCompressDouble(compressedData, offset, length);
      return newDoublePage(columnSpec, doubleData);
    } else if (storeDataType == DataTypes.BYTE_ARRAY) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newLVBytesPage(columnSpec, lvVarBytes);
    } else {
      throw new UnsupportedOperationException("unsupport uncompress column page: " +
          meta.getStoreDataType());
    }
  }

  /**
   * Decompress data and create a decimal column page using the decompressed data
   */
  public static ColumnPage decompressDecimalPage(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length) throws MemoryException {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    ColumnPage decimalPage = null;
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BYTE) {
      byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
      decimalPage = createDecimalPage(columnSpec, meta.getStoreDataType(), byteData.length);
      decimalPage.setBytePage(byteData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT) {
      short[] shortData = compressor.unCompressShort(compressedData, offset, length);
      decimalPage = createDecimalPage(columnSpec, meta.getStoreDataType(), shortData.length);
      decimalPage.setShortPage(shortData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT_INT) {
      byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
      decimalPage = createDecimalPage(columnSpec, meta.getStoreDataType(), shortIntData.length);
      decimalPage.setShortIntPage(shortIntData);
      return decimalPage;
    }  else if (storeDataType == DataTypes.INT) {
      int[] intData = compressor.unCompressInt(compressedData, offset, length);
      decimalPage = createDecimalPage(columnSpec, meta.getStoreDataType(), intData.length);
      decimalPage.setIntPage(intData);
      return decimalPage;
    } else if (storeDataType == DataTypes.LONG) {
      long[] longData = compressor.unCompressLong(compressedData, offset, length);
      decimalPage = createDecimalPage(columnSpec, meta.getStoreDataType(), longData.length);
      decimalPage.setLongPage(longData);
      return decimalPage;
    } else {
      byte[] lvEncodedBytes = compressor.unCompressByte(compressedData, offset, length);
      return newDecimalPage(columnSpec, lvEncodedBytes);
    }
  }

  public BitSet getNullBits() {
    return nullBitSet;
  }

  public void setNullBits(BitSet nullBitSet) {
    this.nullBitSet = nullBitSet;
  }

  public TableSpec.ColumnSpec getColumnSpec() {
    return columnSpec;
  }
}
