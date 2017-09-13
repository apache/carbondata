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
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.KeyPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.datastore.page.statistics.StringStatsCollector;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;

import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE_ARRAY;
import static org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL;
import static org.apache.carbondata.core.metadata.datatype.DataType.DOUBLE;
import static org.apache.carbondata.core.metadata.datatype.DataType.FLOAT;
import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT_INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.STRING;

/**
 * A columnar data page
 */
public abstract class ColumnPage {

  // number of row in this page
  protected final int pageSize;

  // data type of the page storage
  protected final DataType dataType;

  // specification of this column
  private final TableSpec.ColumnSpec columnSpec;

  // scale and precision is for decimal column page only
  // TODO: make DataType a class instead of enum
  protected int scale;
  protected int precision;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  // statistics collector for this column page
  private ColumnPageStatsCollector statsCollector;

  DecimalConverterFactory.DecimalConverter decimalConverter;

  protected static final boolean unsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING,
          CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING_DEFAULT));

  /**
   * Create a new column page with input data type and page size. `scale` and
   * `precision` is for decimal type only
   */
  protected ColumnPage(TableSpec.ColumnSpec columnSpec, DataType dataType, int pageSize,
      int scale, int precision) {
    this.pageSize = pageSize;
    this.columnSpec = columnSpec;
    this.dataType = dataType;
    this.scale = scale;
    this.precision = precision;
    this.nullBitSet = new BitSet(pageSize);
    if (dataType == DECIMAL) {
      decimalConverter = DecimalConverterFactory.INSTANCE.getDecimalConverter(precision, scale);
    }
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

  private static ColumnPage createVarLengthPage(TableSpec.ColumnSpec columnSpec,
      DataType dataType, int pageSize, int scale, int precision) {
    if (unsafe) {
      try {
        return new UnsafeVarLengthColumnPage(columnSpec, dataType, pageSize, scale, precision);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeVarLengthColumnPage(columnSpec, dataType, pageSize, scale, precision);
    }
  }

  private static ColumnPage createFixLengthPage(TableSpec.ColumnSpec columnSpec,
      DataType dataType, int pageSize, int scale, int precision) {
    if (unsafe) {
      try {
        return new UnsafeFixLengthColumnPage(columnSpec, dataType, pageSize, scale, precision);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeFixLengthColumnPage(columnSpec, dataType, pageSize, scale, pageSize);
    }
  }

  private static ColumnPage createPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize, int scale, int precision) {
    if (dataType.equals(BYTE_ARRAY) || dataType.equals(DECIMAL)) {
      return createVarLengthPage(columnSpec, dataType, pageSize, scale, precision);
    } else {
      return createFixLengthPage(columnSpec, dataType, pageSize, scale, precision);
    }
  }

  public static ColumnPage newPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize) throws MemoryException {
    return newPage(columnSpec, dataType, pageSize, -1, -1);
  }

  public static ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize, int scale, int precision) throws MemoryException {
    return newPage(columnSpec, dataType, pageSize, scale, precision);
  }

  /**
   * Create a new page of dataType and number of row = pageSize
   * `scale` and `precision` is for decimal type only
   */
  private static ColumnPage newPage(TableSpec.ColumnSpec columnSpec, DataType dataType,
      int pageSize, int scale, int precision)
      throws MemoryException {
    ColumnPage instance;
    if (unsafe) {
      switch (dataType) {
        case BYTE:
        case SHORT:
        case SHORT_INT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          instance = new UnsafeFixLengthColumnPage(
              columnSpec, dataType, pageSize, -1, -1);
          break;
        case STRING:
        case DECIMAL:
        case BYTE_ARRAY:
          instance = new UnsafeVarLengthColumnPage(
              columnSpec, dataType, pageSize, scale, precision);
          break;
        default:
          throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    } else {
      switch (dataType) {
        case BYTE:
          instance = newBytePage(columnSpec, new byte[pageSize]);
          break;
        case SHORT:
          instance = newShortPage(columnSpec, new short[pageSize]);
          break;
        case SHORT_INT:
          instance = newShortIntPage(columnSpec, new byte[pageSize * 3]);
          break;
        case INT:
          instance = newIntPage(columnSpec, new int[pageSize]);
          break;
        case LONG:
          instance = newLongPage(columnSpec, new long[pageSize]);
          break;
        case FLOAT:
          instance = newFloatPage(columnSpec, new float[pageSize]);
          break;
        case DOUBLE:
          instance = newDoublePage(columnSpec, new double[pageSize]);
          break;
        case DECIMAL:
          instance = newDecimalPage(columnSpec, new byte[pageSize][], scale, precision);
          break;
        case STRING:
        case BYTE_ARRAY:
          instance = new SafeVarLengthColumnPage(columnSpec, dataType, pageSize, -1, -1);
          break;
        default:
          throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    }
    instance.setStatsCollector(dataType, scale, precision);
    return instance;
  }

  private void setStatsCollector(DataType dataType, int scale, int precision) {
    ColumnPageStatsCollector collector;
    switch (dataType) {
      case BYTE:
      case SHORT:
      case SHORT_INT:
      case INT:
      case LONG:
      case DOUBLE:
        collector = PrimitivePageStatsCollector.newInstance(
            columnSpec.getColumnType(), dataType, -1, -1);
        break;
      case DECIMAL:
        collector = PrimitivePageStatsCollector.newInstance(
            columnSpec.getColumnType(), dataType, scale, precision);
        break;
      case STRING:
        collector = StringStatsCollector.newInstance();
        break;
      case BYTE_ARRAY:
        collector = KeyPageStatsCollector.newInstance(dataType);
        break;
      default:
        throw new RuntimeException("internal error");
    }
    this.statsCollector = collector;
  }

  public static ColumnPage wrapByteArrayPage(TableSpec.ColumnSpec columnSpec,
      byte[][] byteArray) {
    ColumnPage columnPage = createPage(columnSpec, BYTE_ARRAY, byteArray.length, -1, -1);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
  }

  private static ColumnPage newBytePage(TableSpec.ColumnSpec columnSpec, byte[] byteData) {
    ColumnPage columnPage = createPage(columnSpec, BYTE, byteData.length,  -1, -1);
    columnPage.setBytePage(byteData);
    return columnPage;
  }

  private static ColumnPage newShortPage(TableSpec.ColumnSpec columnSpec, short[] shortData) {
    ColumnPage columnPage = createPage(columnSpec, SHORT, shortData.length,  -1, -1);
    columnPage.setShortPage(shortData);
    return columnPage;
  }

  private static ColumnPage newShortIntPage(TableSpec.ColumnSpec columnSpec, byte[] shortIntData) {
    ColumnPage columnPage = createPage(columnSpec, SHORT_INT, shortIntData.length / 3,  -1, -1);
    columnPage.setShortIntPage(shortIntData);
    return columnPage;
  }

  private static ColumnPage newIntPage(TableSpec.ColumnSpec columnSpec, int[] intData) {
    ColumnPage columnPage = createPage(columnSpec, INT, intData.length,  -1, -1);
    columnPage.setIntPage(intData);
    return columnPage;
  }

  private static ColumnPage newLongPage(TableSpec.ColumnSpec columnSpec, long[] longData) {
    ColumnPage columnPage = createPage(columnSpec, LONG, longData.length,  -1, -1);
    columnPage.setLongPage(longData);
    return columnPage;
  }

  private static ColumnPage newFloatPage(TableSpec.ColumnSpec columnSpec, float[] floatData) {
    ColumnPage columnPage = createPage(columnSpec, FLOAT, floatData.length,  -1, -1);
    columnPage.setFloatPage(floatData);
    return columnPage;
  }

  private static ColumnPage newDoublePage(TableSpec.ColumnSpec columnSpec, double[] doubleData) {
    ColumnPage columnPage = createPage(columnSpec, DOUBLE, doubleData.length, -1, -1);
    columnPage.setDoublePage(doubleData);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec, byte[][] byteArray,
      int scale, int precision) {
    ColumnPage columnPage = createPage(columnSpec, DECIMAL, byteArray.length, scale, precision);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int scale, int precision) throws MemoryException {
    return VarLengthColumnPageBase.newDecimalColumnPage(
        columnSpec, lvEncodedByteArray, scale, precision);
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

  private static final byte[] NULL_STRING = new byte[0];

  private Object getValue(Object value) {
    ColumnType columnType = columnSpec.getColumnType();
    if (columnType == ColumnType.PLAIN_VALUE) {
      if (dataType != STRING) {
        byte[] bytes = (byte[]) value;
        return DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(
            bytes, columnSpec.getSchemaDataType());
      }
    }
    return value;
  }

  /**
   * Set value at rowId
   */
  public void putData(int rowId, Object value) {
    if (isNull(value)) {
      putNull(rowId);
      return;
    }
    Object v = getValue(value);
    switch (dataType) {
      case BYTE:
        putByte(rowId, (byte) v);
        statsCollector.update((byte) v);
        break;
      case SHORT:
        putShort(rowId, (short) v);
        statsCollector.update((short) v);
        break;
      case INT:
        putInt(rowId, (int) v);
        statsCollector.update((int) v);
        break;
      case LONG:
        putLong(rowId, (long) v);
        statsCollector.update((long) v);
        break;
      case DOUBLE:
        putDouble(rowId, (double) v);
        statsCollector.update((double) v);
        break;
      case DECIMAL:
        putDecimal(rowId, (BigDecimal) v);
        statsCollector.update((BigDecimal) v);
        break;
      case STRING:
        putBytes(rowId, (byte[]) v);
        statsCollector.update((byte[]) v);
        break;
      case BYTE_ARRAY:
        putBytes(rowId, (byte[]) v);
        statsCollector.update((byte[]) v);
        break;
      default:
        throw new RuntimeException("unsupported data type: " + dataType);
    }
  }

  private boolean isNull(Object value) {
    ColumnType columnType = columnSpec.getColumnType();
    if (columnType == ColumnType.DIRECT_DICTIONARY) {
      if ((int) value == 1) {
        return true;
      }
    } else if (columnType == ColumnType.PLAIN_VALUE) {
      switch (dataType) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case STRING:
          if (ByteUtil.UnsafeComparer.INSTANCE.equals(
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, (byte[]) value)) {
            return true;
          }
          break;
        default:
          throw new RuntimeException("invalid type: " + dataType);
      }
    } else if (columnType == ColumnType.MEASURE) {
      if (value == null) {
        return true;
      }
    }
    return false;
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
   * Set byte array from offset to length at rowId
   */
  public abstract void putBytes(int rowId, byte[] bytes, int offset, int length);


  /**
   * Set null at rowId
   */
  private void putNull(int rowId) {
    int nullValue = columnSpec.getColumnType() == ColumnType.MEASURE ? 0 : 1;
    switch (dataType) {
      case BYTE:
        putByte(rowId, (byte) nullValue);
        break;
      case SHORT:
        putShort(rowId, (short) nullValue);
        break;
      case INT:
        putInt(rowId, nullValue);
        break;
      case LONG:
        putLong(rowId, nullValue);
        break;
      case DOUBLE:
        putDouble(rowId, nullValue);
        break;
      case DECIMAL:
        putDecimal(rowId, BigDecimal.ZERO);
        break;
      case STRING:
        putBytes(rowId, NULL_STRING);
        break;
      default:
        throw new IllegalArgumentException("unsupported data type: " + dataType);
    }
    statsCollector.updateNull(rowId);
    nullBitSet.set(rowId);
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
   * For variable length page, get the Length-Value flattened data
   */
  public abstract byte[] getLVFlattenedBytePage();

  /**
   * For variable length page, get the directly flattened data
   */
  public abstract byte[] getDirectFlattenedBytePage();

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
    switch (dataType) {
      case BYTE:
        return compressor.compressByte(getBytePage());
      case SHORT:
        return compressor.compressShort(getShortPage());
      case SHORT_INT:
        return compressor.compressByte(getShortIntPage());
      case INT:
        return compressor.compressInt(getIntPage());
      case LONG:
        return compressor.compressLong(getLongPage());
      case FLOAT:
        return compressor.compressFloat(getFloatPage());
      case DOUBLE:
        return compressor.compressDouble(getDoublePage());
      case DECIMAL:
        return compressor.compressByte(getDecimalPage());
      case BYTE_ARRAY:
        return compressor.compressByte(getLVFlattenedBytePage());
      default:
        throw new UnsupportedOperationException("unsupport compress column page: " + dataType);
    }
  }

  /**
   * Decompress data and create a column page using the decompressed data,
   * except for decimal page
   */
  public static ColumnPage decompress(TableSpec.ColumnSpec columnSpec, Compressor compressor,
      DataType dataType, byte[] compressedData, int offset, int length) throws MemoryException {
    switch (dataType) {
      case BYTE:
        byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
        return newBytePage(columnSpec, byteData);
      case SHORT:
        short[] shortData = compressor.unCompressShort(compressedData, offset, length);
        return newShortPage(columnSpec, shortData);
      case SHORT_INT:
        byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
        return newShortIntPage(columnSpec, shortIntData);
      case INT:
        int[] intData = compressor.unCompressInt(compressedData, offset, length);
        return newIntPage(columnSpec, intData);
      case LONG:
        long[] longData = compressor.unCompressLong(compressedData, offset, length);
        return newLongPage(columnSpec, longData);
      case FLOAT:
        float[] floatData = compressor.unCompressFloat(compressedData, offset, length);
        return newFloatPage(columnSpec, floatData);
      case DOUBLE:
        double[] doubleData = compressor.unCompressDouble(compressedData, offset, length);
        return newDoublePage(columnSpec, doubleData);
      case BYTE_ARRAY:
        byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
        return newLVBytesPage(columnSpec, lvVarBytes);
      default:
        throw new UnsupportedOperationException("unsupport uncompress column page: " + dataType);
    }
  }

  /**
   * Decompress decimal data and create a column page
   */
  public static ColumnPage decompressDecimalPage(TableSpec.ColumnSpec columnSpec,
      Compressor compressor, byte[] compressedData, int offset, int length, int scale,
      int precision) throws MemoryException {
    byte[] lvEncodedBytes = compressor.unCompressByte(compressedData, offset, length);
    return newDecimalPage(columnSpec, lvEncodedBytes, scale, precision);
  }

  /**
   * Decompress and return a String page.
   */
  public static ColumnPage decompressStringPage(TableSpec.ColumnSpec columnSpec,
      Compressor compressor, byte[] compressedData, int offset, int length, short[] lengths)
      throws MemoryException {
    byte[] bytes = compressor.unCompressByte(compressedData, offset, length);
    return SafeVarLengthColumnPage.newStringPage(columnSpec, bytes, lengths);
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
