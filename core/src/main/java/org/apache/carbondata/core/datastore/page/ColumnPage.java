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
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.blocklet.PresenceMeta;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.localdictionary.PageLevelDictionary;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE_ARRAY;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.FLOAT;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.INT;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT;

public abstract class ColumnPage {

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

  private PresenceMeta presenceMeta;

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

  private static ColumnPage createDecimalPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize) {
    if (isUnsafeEnabled(columnPageEncoderMeta)) {
      return new UnsafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
    } else {
      return new SafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createVarLengthPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize) {
    if (isUnsafeEnabled(columnPageEncoderMeta)) {
      return new UnsafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
    } else {
      return new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createFixLengthPage(
      ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    if (isUnsafeEnabled(columnPageEncoderMeta)) {
      return new UnsafeFixLengthColumnPage(columnPageEncoderMeta, pageSize);
    } else {
      return new SafeFixLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createFixLengthByteArrayPage(
      ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize, int eachValueSize) {
    if (isUnsafeEnabled(columnPageEncoderMeta)) {
      return new UnsafeFixLengthColumnPage(columnPageEncoderMeta, pageSize, eachValueSize);
    } else {
      return new SafeFixLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    if (DataTypes.isDecimal(columnPageEncoderMeta.getStoreDataType())) {
      return createDecimalPage(columnPageEncoderMeta, pageSize);
    } else if (columnPageEncoderMeta.getStoreDataType().equals(BYTE_ARRAY)) {
      return createVarLengthPage(columnPageEncoderMeta, pageSize);
    } else {
      return createFixLengthPage(columnPageEncoderMeta, pageSize);
    }
  }

  public static ColumnPage newDecimalPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize) {
    return newPage(columnPageEncoderMeta, pageSize);
  }

  public static ColumnPage newLocalDictPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize, LocalDictionaryGenerator localDictionaryGenerator) {
    boolean isDecoderBasedFallBackEnabled = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK,
            CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK_DEFAULT));
    ColumnPageStatsCollector primitivePageStatsCollector =
        PrimitivePageStatsCollector.newInstance(DataTypes.INT);
    ColumnPage actualPage;
    ColumnPage encodedPage;
    TableSpec.MeasureSpec encodedSpec = TableSpec.MeasureSpec
        .newInstance(columnPageEncoderMeta.getColumnSpec().getFieldName(), DataTypes.INT);
    if (isUnsafeEnabled(columnPageEncoderMeta)) {
      DataType dataType = columnPageEncoderMeta.getStoreDataType();
      if (dataType == DataTypes.STRING ||
          dataType == DataTypes.VARCHAR ||
          dataType == DataTypes.BINARY) {
        actualPage = new LVByteBufferColumnPage(columnPageEncoderMeta, pageSize);
      } else {
        actualPage = new UnsafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      }
    } else {
      actualPage = new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
    encodedPage =
        newIntPage(new ColumnPageEncoderMeta(encodedSpec, DataTypes.INT,
            columnPageEncoderMeta.getCompressorName()), new int[pageSize]);
    encodedPage.setStatsCollector(primitivePageStatsCollector);
    return new LocalDictColumnPage(actualPage, encodedPage, localDictionaryGenerator,
        isDecoderBasedFallBackEnabled);
  }

  /**
   * Create a new page of dataType and number of row = pageSize
   */
  public static ColumnPage newPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    ColumnPage instance;
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    TableSpec.ColumnSpec columnSpec = columnPageEncoderMeta.getColumnSpec();
    String compressorName = columnPageEncoderMeta.getCompressorName();
    if (isUnsafeEnabled(columnPageEncoderMeta)) {
      if (dataType == DataTypes.BOOLEAN) {
        instance = new UnsafeFixLengthColumnPage(
            new ColumnPageEncoderMeta(columnSpec, BYTE, compressorName), pageSize);
      } else if (dataType == BYTE ||
          dataType == SHORT ||
          dataType == DataTypes.SHORT_INT ||
          dataType == INT ||
          dataType == LONG ||
          dataType == DataTypes.FLOAT ||
          dataType == DataTypes.DOUBLE) {
        instance = new UnsafeFixLengthColumnPage(columnPageEncoderMeta, pageSize);
      } else if (dataType == DataTypes.TIMESTAMP) {
        instance = new UnsafeFixLengthColumnPage(
            new ColumnPageEncoderMeta(columnSpec, LONG, compressorName), pageSize);
      } else if (DataTypes.isDecimal(dataType)) {
        instance = new UnsafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
      } else if (dataType == DataTypes.STRING ||
          dataType == DataTypes.VARCHAR ||
          dataType == DataTypes.BINARY) {
        instance = new LVByteBufferColumnPage(columnPageEncoderMeta, pageSize);
      } else if (dataType == BYTE_ARRAY) {
        instance = new UnsafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      } else {
        throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    } else {
      if (dataType == DataTypes.BOOLEAN || dataType == BYTE) {
        instance = newBytePage(columnPageEncoderMeta, new byte[pageSize]);
      } else if (dataType == SHORT) {
        instance = newShortPage(columnPageEncoderMeta, new short[pageSize]);
      } else if (dataType == DataTypes.SHORT_INT) {
        instance = newShortIntPage(columnPageEncoderMeta, new byte[pageSize * 3]);
      } else if (dataType == INT) {
        instance = newIntPage(columnPageEncoderMeta, new int[pageSize]);
      } else if (dataType == LONG || dataType == DataTypes.TIMESTAMP) {
        instance = newLongPage(
            new ColumnPageEncoderMeta(columnPageEncoderMeta.getColumnSpec(), LONG,
                columnPageEncoderMeta.getCompressorName()), new long[pageSize]);
      } else if (dataType == DataTypes.FLOAT) {
        instance = newFloatPage(columnPageEncoderMeta, new float[pageSize]);
      } else if (dataType == DataTypes.DOUBLE) {
        instance = newDoublePage(columnPageEncoderMeta, new double[pageSize]);
      } else if (DataTypes.isDecimal(dataType)) {
        instance = newDecimalPage(columnPageEncoderMeta, new byte[pageSize][]);
      } else if (dataType == DataTypes.STRING ||
          dataType == DataTypes.VARCHAR ||
          dataType == DataTypes.BINARY) {
        instance = new LVByteBufferColumnPage(columnPageEncoderMeta, pageSize);
      } else if (dataType == BYTE_ARRAY) {
        instance = new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      } else {
        throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    }
    return instance;
  }

  private static ColumnPage newBytePage(ColumnPageEncoderMeta meta, byte[] byteData) {
    ColumnPageEncoderMeta encoderMeta =
        new ColumnPageEncoderMeta(meta.getColumnSpec(), BYTE, meta.getCompressorName());
    encoderMeta.setFillCompleteVector(meta.isFillCompleteVector());
    ColumnPage columnPage = createPage(
        encoderMeta, byteData.length);
    columnPage.setBytePage(byteData);
    return columnPage;
  }

  private static ColumnPage newShortPage(ColumnPageEncoderMeta meta, short[] shortData) {
    ColumnPage columnPage = createPage(meta, shortData.length);
    columnPage.setShortPage(shortData);
    return columnPage;
  }

  private static ColumnPage newShortIntPage(ColumnPageEncoderMeta meta, byte[] shortIntData) {
    ColumnPage columnPage = createPage(meta, shortIntData.length / 3);
    columnPage.setShortIntPage(shortIntData);
    return columnPage;
  }

  private static ColumnPage newIntPage(ColumnPageEncoderMeta meta, int[] intData) {
    ColumnPage columnPage = createPage(meta, intData.length);
    columnPage.setIntPage(intData);
    return columnPage;
  }

  private static ColumnPage newLongPage(ColumnPageEncoderMeta meta, long[] longData) {
    ColumnPage columnPage = createPage(meta, longData.length);
    columnPage.setLongPage(longData);
    return columnPage;
  }

  private static ColumnPage newFloatPage(ColumnPageEncoderMeta meta, float[] floatData) {
    ColumnPage columnPage = createPage(meta, floatData.length);
    columnPage.setFloatPage(floatData);
    return columnPage;
  }

  private static ColumnPage newDoublePage(ColumnPageEncoderMeta meta, double[] doubleData) {
    ColumnPage columnPage = createPage(meta, doubleData.length);
    columnPage.setDoublePage(doubleData);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(ColumnPageEncoderMeta meta, byte[][] byteArray) {
    ColumnPageEncoderMeta encoderMeta =
        new ColumnPageEncoderMeta(meta.getColumnSpec(), meta.getColumnSpec().getSchemaDataType(),
            meta.getCompressorName());
    encoderMeta.setFillCompleteVector(meta.isFillCompleteVector());
    ColumnPage columnPage = createPage(
        encoderMeta,
        byteArray.length);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(ColumnPageEncoderMeta meta,
      byte[] lvEncodedByteArray) {
    return VarLengthColumnPageBase
        .newDecimalColumnPage(meta, lvEncodedByteArray, lvEncodedByteArray.length);
  }

  private static ColumnPage newLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int lvLength, String compressorName) {
    return VarLengthColumnPageBase.newLVBytesColumnPage(
        columnSpec, lvEncodedByteArray, lvLength, compressorName);
  }

  private static ColumnPage newComplexLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int lvLength, String compressorName) {
    return VarLengthColumnPageBase.newComplexLVBytesColumnPage(
        columnSpec, lvEncodedByteArray, lvLength, compressorName);
  }

  private static ColumnPage newFixedByteArrayPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int eachValueSize, String compressorName) {
    int pageSize = lvEncodedByteArray.length / eachValueSize;
    ColumnPage fixLengthByteArrayPage = createFixLengthByteArrayPage(
        new ColumnPageEncoderMeta(columnSpec, columnSpec.getSchemaDataType(), compressorName),
        pageSize, eachValueSize);
    byte[] data = null;
    int offset = 0;
    for (int i = 0; i < pageSize; i++) {
      data = new byte[eachValueSize];
      System.arraycopy(lvEncodedByteArray, offset, data, 0, eachValueSize);
      fixLengthByteArrayPage.putBytes(i, data);
      offset += eachValueSize;
    }
    return fixLengthByteArrayPage;
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
   * PutData with default null value used for dictionary/direct dictionary columns
   * @param rowId
   * @param value
   * @param nullValue
   */
  public void putData(int rowId, Object value, Object nullValue) {
    if (value == null) {
      putNull(rowId, nullValue);
      statsCollector.updateNull(rowId, nullValue);
      nullBitSet.set(rowId);
      return;
    }
    putData(rowId, value);
  }

  /**
   * Set null at rowId with null values passed
   * this will be used for dictionary columns
   */
  protected void putNull(int rowId, Object nullValue) {
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.INT) {
      putInt(rowId, (int)nullValue);
    } else {
      throw new UnsupportedOperationException("Operation Not supported");
    }
  }

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
    if (dataType == DataTypes.BOOLEAN || dataType == BYTE) {
      if (columnPageEncoderMeta.getColumnSpec().getSchemaDataType() == DataTypes.BOOLEAN) {
        value = BooleanConvert.boolean2Byte((Boolean) value);
      }
      putByte(rowId, (byte) value);
      statsCollector.update((byte) value);
    } else if (dataType == SHORT) {
      putShort(rowId, (short) value);
      statsCollector.update((short) value);
    } else if (dataType == INT) {
      putInt(rowId, (int) value);
      statsCollector.update((int) value);
    } else if (dataType == LONG) {
      putLong(rowId, (long) value);
      statsCollector.update((long) value);
    } else if (dataType == DataTypes.DOUBLE) {
      putDouble(rowId, (double) value);
      statsCollector.update((double) value);
    } else if (DataTypes.isDecimal(dataType)) {
      putDecimal(rowId, (BigDecimal) value);
      statsCollector.update((BigDecimal) value);
    } else if (dataType == DataTypes.STRING
        || dataType == BYTE_ARRAY
        || dataType == DataTypes.VARCHAR) {
      putBytes(rowId, (byte[]) value);
      statsCollector.update((byte[]) value);
    } else if (dataType == DataTypes.FLOAT) {
      putFloat(rowId, (float) value);
      statsCollector.update((float) value);
    } else if (dataType == DataTypes.BINARY) {
      putBytes(rowId, (byte[]) value);
      statsCollector.update((byte[]) value);
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
    if (dataType == DataTypes.BOOLEAN || dataType == BYTE) {
      byte value = getByte(rowId);
      if (columnPageEncoderMeta.getColumnSpec().getSchemaDataType() == DataTypes.BOOLEAN) {
        return BooleanConvert.byte2Boolean(value);
      }
      return value;
    } else if (dataType == SHORT) {
      return getShort(rowId);
    } else if (dataType == INT) {
      return getInt(rowId);
    } else if (dataType == LONG) {
      return getLong(rowId);
    } else if (dataType == DataTypes.DOUBLE) {
      return getDouble(rowId);
    } else if (DataTypes.isDecimal(dataType)) {
      return getDecimal(rowId);
    } else if (dataType == DataTypes.STRING
        || dataType == BYTE_ARRAY
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
    } else if (dataType == BYTE) {
      putByte(rowId, (byte) 0);
    } else if (dataType == SHORT) {
      putShort(rowId, (short) 0);
    } else if (dataType == INT) {
      putInt(rowId, 0);
    } else if (dataType == LONG) {
      putLong(rowId, 0L);
    } else if (dataType == DataTypes.DOUBLE) {
      putDouble(rowId, 0.0);
    } else if (dataType == DataTypes.FLOAT) {
      putFloat(rowId, 0.0f);
    } else if (DataTypes.isDecimal(dataType)) {
      putDecimal(rowId, BigDecimal.ZERO);
    } else {
      putBytes(rowId, new byte[0]);
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
    } else if (dataType == BYTE) {
      result = getByte(rowId);
      if (columnPageEncoderMeta.getColumnSpec().getSchemaDataType() == DataTypes.BOOLEAN) {
        result = BooleanConvert.byte2Boolean((byte)result);
      }
    } else if (dataType == SHORT) {
      result = getShort(rowId);
    } else if (dataType == INT) {
      result = getInt(rowId);
    } else if (dataType == LONG) {
      result = getLong(rowId);
    } else if (dataType == DataTypes.DOUBLE) {
      result = getDouble(rowId);
    } else if (DataTypes.isDecimal(dataType)) {
      result = getDecimal(rowId);
    } else {
      return new byte[0];
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
   * For complex type columns
   * @return
   */
  public abstract byte[] getComplexChildrenLVFlattenedBytePage(DataType dataType)
      throws IOException;

  /**
   * For complex type columns
   * @return
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

  /**
   * Return total page data length in bytes
   */
  public long getPageLengthInBytes() throws IOException {
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BOOLEAN) {
      return getBooleanPage().length;
    } else if (dataType == BYTE) {
      return getBytePage().length;
    } else if (dataType == SHORT) {
      return getShortPage().length * SHORT.getSizeInBytes();
    } else if (dataType == DataTypes.SHORT_INT) {
      return getShortIntPage().length;
    } else if (dataType == INT) {
      return getIntPage().length * INT.getSizeInBytes();
    } else if (dataType == LONG) {
      return getLongPage().length * LONG.getSizeInBytes();
    } else if (dataType == DataTypes.FLOAT) {
      return getFloatPage().length * FLOAT.getSizeInBytes();
    } else if (dataType == DataTypes.DOUBLE) {
      return getDoublePage().length * DOUBLE.getSizeInBytes();
    } else if (DataTypes.isDecimal(dataType)) {
      return getDecimalPage().length;
    } else if (dataType == BYTE_ARRAY
        && columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      return getComplexChildrenLVFlattenedBytePage(
          columnPageEncoderMeta.getColumnSpec().getSchemaDataType()).length;
    } else if (dataType == BYTE_ARRAY
        && (columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_STRUCT
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_ARRAY
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.PLAIN_LONG_VALUE
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.PLAIN_VALUE)) {
      return getComplexParentFlattenedBytePage().length;
    } else if (dataType == BYTE_ARRAY) {
      return getLVFlattenedBytePage().length;
    } else {
      throw new UnsupportedOperationException("unsupported compress column page: " + dataType);
    }
  }

  /**
   * Compress page data using specified compressor
   */
  public ByteBuffer compress(Compressor compressor) throws IOException {
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.STRING) {
      return compressor.compressByte(getByteBuffer());
    } else if (dataType == DataTypes.BOOLEAN) {
      return compressor.compressByte(getBooleanPage());
    } else if (dataType == BYTE) {
      return compressor.compressByte(getBytePage());
    } else if (dataType == SHORT) {
      return compressor.compressShort(getShortPage());
    } else if (dataType == DataTypes.SHORT_INT) {
      return compressor.compressByte(getShortIntPage());
    } else if (dataType == INT) {
      return compressor.compressInt(getIntPage());
    } else if (dataType == LONG) {
      return compressor.compressLong(getLongPage());
    } else if (dataType == DataTypes.FLOAT) {
      return compressor.compressFloat(getFloatPage());
    } else if (dataType == DataTypes.DOUBLE) {
      return compressor.compressDouble(getDoublePage());
    } else if (DataTypes.isDecimal(dataType)) {
      return compressor.compressByte(getDecimalPage());
    } else if (dataType == BYTE_ARRAY
        && columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      return compressor.compressByte(getComplexChildrenLVFlattenedBytePage(
          columnPageEncoderMeta.getColumnSpec().getSchemaDataType()));
    } else if (dataType == BYTE_ARRAY
        && (columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_STRUCT
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.COMPLEX_ARRAY
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.PLAIN_LONG_VALUE
        || columnPageEncoderMeta.getColumnSpec().getColumnType() == ColumnType.PLAIN_VALUE)) {
      return compressor.compressByte(getComplexParentFlattenedBytePage());
    } else if (dataType == DataTypes.BINARY) {
      return ByteBuffer.wrap(getLVFlattenedBytePage());
    } else if (dataType == BYTE_ARRAY) {
      return compressor.compressByte(getLVFlattenedBytePage());
    } else {
      throw new UnsupportedOperationException("unsupported compress column page: " + dataType);
    }
  }

  /**
   * Decompress data and create a column page using the decompressed data,
   * except for decimal page
   */
  public static ColumnPage decompress(ColumnPageEncoderMeta meta, byte[] compressedData, int offset,
      int length, boolean isLVEncoded, boolean isComplexPrimitiveIntLengthEncoding) {
    return decompress(meta, compressedData, offset, length, isLVEncoded,
        isComplexPrimitiveIntLengthEncoding, false, 0);
  }

  /**
   * Decompress data and create a column page using the decompressed data,
   * except for decimal page
   */
  public static ColumnPage decompress(ColumnPageEncoderMeta meta, byte[] compressedData, int offset,
      int length, boolean isLVEncoded, boolean isComplexPrimitiveIntLengthEncoding,
      boolean isRLEEncoded, int rlePageLength) {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BOOLEAN || storeDataType == BYTE) {
      byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
      if (isRLEEncoded) {
        int[] rlePage;
        offset += length;
        rlePage = CarbonUtil.getIntArray(ByteBuffer.wrap(compressedData), offset, rlePageLength);
        // uncompress the data with rle indexes
        byteData = UnBlockIndexer
            .uncompressData(byteData, rlePage, BYTE.getSizeInBytes(), byteData.length);
      }
      return newBytePage(meta, byteData);
    } else if (storeDataType == SHORT) {
      short[] shortData = compressor.unCompressShort(compressedData, offset, length);
      return newShortPage(meta, shortData);
    } else if (storeDataType == DataTypes.SHORT_INT) {
      byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
      return newShortIntPage(meta, shortIntData);
    } else if (storeDataType == INT) {
      int[] intData = compressor.unCompressInt(compressedData, offset, length);
      return newIntPage(meta, intData);
    } else if (storeDataType == LONG) {
      long[] longData = compressor.unCompressLong(compressedData, offset, length);
      return newLongPage(meta, longData);
    } else if (storeDataType == DataTypes.FLOAT) {
      float[] floatData = compressor.unCompressFloat(compressedData, offset, length);
      return newFloatPage(meta, floatData);
    } else if (storeDataType == DataTypes.DOUBLE) {
      double[] doubleData = compressor.unCompressDouble(compressedData, offset, length);
      return newDoublePage(meta, doubleData);
    } else if (!isLVEncoded && storeDataType == BYTE_ARRAY && (
        columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE
            || columnSpec.getColumnType() == ColumnType.PLAIN_VALUE)) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      if (isComplexPrimitiveIntLengthEncoding) {
        // decode as int length
        return newComplexLVBytesPage(columnSpec, lvVarBytes,
            CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
      } else {
        return newComplexLVBytesPage(columnSpec, lvVarBytes,
            CarbonCommonConstants.SHORT_SIZE_IN_BYTE, meta.getCompressorName());
      }
    } else if (isLVEncoded && storeDataType == BYTE_ARRAY &&
        columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newFixedByteArrayPage(columnSpec, lvVarBytes, 3, meta.getCompressorName());
    } else if (storeDataType == BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.COMPLEX_STRUCT) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newFixedByteArrayPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.SHORT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.COMPLEX_ARRAY) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newFixedByteArrayPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.LONG_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.PLAIN_LONG_VALUE) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newLVBytesPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == BYTE_ARRAY) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newLVBytesPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else {
      throw new UnsupportedOperationException(
          "unsupported uncompress column page: " + meta.getStoreDataType());
    }
  }

  /**
   * Decompress data and create a decimal column page using the decompressed data
   */
  public static ColumnPage decompressDecimalPage(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length) {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    ColumnPage decimalPage;
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == BYTE) {
      byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, byteData.length);
      decimalPage.setBytePage(byteData);
      return decimalPage;
    } else if (storeDataType == SHORT) {
      short[] shortData = compressor.unCompressShort(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, shortData.length);
      decimalPage.setShortPage(shortData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT_INT) {
      byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, shortIntData.length / 3);
      decimalPage.setShortIntPage(shortIntData);
      return decimalPage;
    }  else if (storeDataType == INT) {
      int[] intData = compressor.unCompressInt(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, intData.length);
      decimalPage.setIntPage(intData);
      return decimalPage;
    } else if (storeDataType == LONG) {
      long[] longData = compressor.unCompressLong(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, longData.length);
      decimalPage.setLongPage(longData);
      return decimalPage;
    } else {
      byte[] lvEncodedBytes = compressor.unCompressByte(compressedData, offset, length);
      return newDecimalPage(meta, lvEncodedBytes);
    }
  }

  /**
   * Whether unsafe enabled or not. In case of filling complete vector flow there is no need to use
   * unsafe flow as we don't store the data in memory for long time.
   * @param meta ColumnPageEncoderMeta
   * @return boolean Whether unsafe enabled or not
   */
  protected static boolean isUnsafeEnabled(ColumnPageEncoderMeta meta) {
    return unsafe && !meta.isFillCompleteVector();
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

  public ByteBuffer getByteBuffer() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public boolean isLVByteBufferPage() {
    return false;
  }

  public ColumnPage getLocalDictPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public boolean isLVActualColumnPage() {
    return false;
  }

  /**
   * Convert the data of the page based on the data type for each row
   * While preparing the inverted index for the page,
   * we need the data based on data type for no dict measure column if adaptive encoding is applied
   * This is similar to page.getByteArrayPage()
   *
   * @return
   */
  public Object[] getPageBasedOnDataType() {
    Object[] data = new Object[getActualRowCount()];
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType == DataTypes.BYTE || dataType == DataTypes.BOOLEAN) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getByte(i);
      }
    } else if (dataType == DataTypes.SHORT) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getShort(i);
      }
    } else if (dataType == DataTypes.SHORT_INT) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getShortInt(i);
      }
    } else if (dataType == DataTypes.INT) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getInt(i);
      }
    } else if (dataType == DataTypes.LONG) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getLong(i);
      }
    } else if (dataType == DataTypes.FLOAT) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getFloat(i);
      }
    } else if (dataType == DataTypes.DOUBLE) {
      for (int i = 0; i < getActualRowCount(); i++) {
        data[i] = getDouble(i);
      }
    }
    return data;
  }

  public PresenceMeta getPresenceMeta() {
    return presenceMeta;
  }

  public void setPresenceMeta(PresenceMeta presenceMeta) {
    this.presenceMeta = presenceMeta;
  }
}
