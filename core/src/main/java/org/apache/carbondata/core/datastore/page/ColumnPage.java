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
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.localdictionary.PageLevelDictionary;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;

import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE_ARRAY;
import static org.apache.carbondata.core.metadata.datatype.DataTypes.LONG;

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
    if (unsafe && !columnPageEncoderMeta.isFillCompleteVector()) {
      try {
        return new UnsafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createVarLengthPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize) {
    if (unsafe && !columnPageEncoderMeta.isFillCompleteVector()) {
      try {
        return new UnsafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createFixLengthPage(
      ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    if (unsafe && !columnPageEncoderMeta.isFillCompleteVector()) {
      try {
        return new UnsafeFixLengthColumnPage(columnPageEncoderMeta, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeFixLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private static ColumnPage createFixLengthByteArrayPage(
      ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize, int eachValueSize) {
    if (unsafe && !columnPageEncoderMeta.isFillCompleteVector()) {
      try {
        return new UnsafeFixLengthColumnPage(columnPageEncoderMeta, pageSize, eachValueSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
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
      int pageSize) throws MemoryException {
    return newPage(columnPageEncoderMeta, pageSize);
  }

  public static ColumnPage newLocalDictPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize, LocalDictionaryGenerator localDictionaryGenerator,
      boolean isComplexTypePrimitive) throws MemoryException {
    boolean isDecoderBasedFallBackEnabled = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK,
            CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK_DEFAULT));
    ColumnPage actualPage;
    ColumnPage encodedPage;
    if (unsafe && !columnPageEncoderMeta.isFillCompleteVector()) {
      actualPage = new UnsafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      encodedPage = new UnsafeFixLengthColumnPage(
          new ColumnPageEncoderMeta(columnPageEncoderMeta.getColumnSpec(), DataTypes.BYTE_ARRAY,
              columnPageEncoderMeta.getCompressorName()),
          pageSize,
          CarbonCommonConstants.LOCAL_DICT_ENCODED_BYTEARRAY_SIZE);
    } else {
      actualPage = new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      encodedPage = new SafeFixLengthColumnPage(
          new ColumnPageEncoderMeta(columnPageEncoderMeta.getColumnSpec(), DataTypes.BYTE_ARRAY,
              columnPageEncoderMeta.getCompressorName()),
          pageSize);
    }
    return new LocalDictColumnPage(actualPage, encodedPage, localDictionaryGenerator,
        isComplexTypePrimitive, isDecoderBasedFallBackEnabled);
  }

  /**
   * Create a new page of dataType and number of row = pageSize
   */
  public static ColumnPage newPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize)
      throws MemoryException {
    ColumnPage instance;
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    TableSpec.ColumnSpec columnSpec = columnPageEncoderMeta.getColumnSpec();
    String compressorName = columnPageEncoderMeta.getCompressorName();
    if (unsafe && !columnPageEncoderMeta.isFillCompleteVector()) {
      if (dataType == DataTypes.BOOLEAN) {
        instance = new UnsafeFixLengthColumnPage(
            new ColumnPageEncoderMeta(columnSpec, BYTE, compressorName), pageSize);
      } else if (dataType == DataTypes.BYTE ||
          dataType == DataTypes.SHORT ||
          dataType == DataTypes.SHORT_INT ||
          dataType == DataTypes.INT ||
          dataType == DataTypes.LONG ||
          dataType == DataTypes.FLOAT ||
          dataType == DataTypes.DOUBLE) {
        instance = new UnsafeFixLengthColumnPage(
            new ColumnPageEncoderMeta(columnSpec, dataType, compressorName), pageSize);
      } else if (dataType == DataTypes.TIMESTAMP) {
        instance = new UnsafeFixLengthColumnPage(
            new ColumnPageEncoderMeta(columnSpec, DataTypes.LONG, compressorName), pageSize);
      } else if (DataTypes.isDecimal(dataType)) {
        instance = new UnsafeDecimalColumnPage(
            new ColumnPageEncoderMeta(columnSpec, dataType, compressorName), pageSize);
      } else if (dataType == DataTypes.STRING
          || dataType == DataTypes.BYTE_ARRAY
          || dataType == DataTypes.VARCHAR) {
        instance = new UnsafeVarLengthColumnPage(
            new ColumnPageEncoderMeta(columnSpec, dataType, compressorName), pageSize);
      } else {
        throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    } else {
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
        instance = newBytePage(columnPageEncoderMeta, new byte[pageSize]);
      } else if (dataType == DataTypes.SHORT) {
        instance = newShortPage(columnPageEncoderMeta, new short[pageSize]);
      } else if (dataType == DataTypes.SHORT_INT) {
        instance = newShortIntPage(columnPageEncoderMeta, new byte[pageSize * 3]);
      } else if (dataType == DataTypes.INT) {
        instance = newIntPage(columnPageEncoderMeta, new int[pageSize]);
      } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
        instance = newLongPage(
            new ColumnPageEncoderMeta(columnPageEncoderMeta.getColumnSpec(), LONG,
                columnPageEncoderMeta.getCompressorName()), new long[pageSize]);
      } else if (dataType == DataTypes.FLOAT) {
        instance = newFloatPage(columnPageEncoderMeta, new float[pageSize]);
      } else if (dataType == DataTypes.DOUBLE) {
        instance = newDoublePage(columnPageEncoderMeta, new double[pageSize]);
      } else if (DataTypes.isDecimal(dataType)) {
        instance = newDecimalPage(columnPageEncoderMeta, new byte[pageSize][]);
      } else if (dataType == DataTypes.STRING
          || dataType == DataTypes.BYTE_ARRAY
          || dataType == DataTypes.VARCHAR) {
        instance = new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      } else {
        throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    }
    return instance;
  }

  public static ColumnPage wrapByteArrayPage(TableSpec.ColumnSpec columnSpec, byte[][] byteArray,
      String compressorName) {
    ColumnPage columnPage = createPage(
        new ColumnPageEncoderMeta(columnSpec, BYTE_ARRAY, compressorName), byteArray.length);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
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
      byte[] lvEncodedByteArray) throws MemoryException {
    return VarLengthColumnPageBase.newDecimalColumnPage(meta, lvEncodedByteArray);
  }

  private static ColumnPage newLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int lvLength, String compressorName) throws MemoryException {
    return VarLengthColumnPageBase.newLVBytesColumnPage(
        columnSpec, lvEncodedByteArray, lvLength, compressorName);
  }

  private static ColumnPage newComplexLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int lvLength, String compressorName) throws MemoryException {
    return VarLengthColumnPageBase.newComplexLVBytesColumnPage(
        columnSpec, lvEncodedByteArray, lvLength, compressorName);
  }

  private static ColumnPage newFixedByteArrayPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int eachValueSize, String compressorName) throws MemoryException {
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
   * Get byte value at rowId
   */
  public abstract byte[] getByteData();

  /**
   * Get short value at rowId
   */
  public abstract short[] getShortData();

  /**
   * Get short int value at rowId
   */
  public abstract int[] getShortIntData();

  /**
   * Get boolean value at rowId
   */
  public byte[] getBooleanData() {
    return getByteData();
  }

  /**
   * Get int value at rowId
   */
  public abstract int[] getIntData();

  /**
   * Get long value at rowId
   */
  public abstract long[] getLongData();

  /**
   * Get float value at rowId
   */
  public abstract float[] getFloatData();

  /**
   * Get double value at rowId
   */
  public abstract double[] getDoubleData();





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

  /**
   * Decompress data and create a column page using the decompressed data,
   * except for decimal page
   */
  public static ColumnPage decompress(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length, boolean isLVEncoded)
      throws MemoryException {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BOOLEAN || storeDataType == DataTypes.BYTE) {
      byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
      return newBytePage(meta, byteData);
    } else if (storeDataType == DataTypes.SHORT) {
      short[] shortData = compressor.unCompressShort(compressedData, offset, length);
      return newShortPage(meta, shortData);
    } else if (storeDataType == DataTypes.SHORT_INT) {
      byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
      return newShortIntPage(meta, shortIntData);
    } else if (storeDataType == DataTypes.INT) {
      int[] intData = compressor.unCompressInt(compressedData, offset, length);
      return newIntPage(meta, intData);
    } else if (storeDataType == DataTypes.LONG) {
      long[] longData = compressor.unCompressLong(compressedData, offset, length);
      return newLongPage(meta, longData);
    } else if (storeDataType == DataTypes.FLOAT) {
      float[] floatData = compressor.unCompressFloat(compressedData, offset, length);
      return newFloatPage(meta, floatData);
    } else if (storeDataType == DataTypes.DOUBLE) {
      double[] doubleData = compressor.unCompressDouble(compressedData, offset, length);
      return newDoublePage(meta, doubleData);
    } else if (!isLVEncoded && storeDataType == DataTypes.BYTE_ARRAY && (
        columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE
            || columnSpec.getColumnType() == ColumnType.PLAIN_VALUE)) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newComplexLVBytesPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.SHORT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (isLVEncoded && storeDataType == DataTypes.BYTE_ARRAY &&
        columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newFixedByteArrayPage(columnSpec, lvVarBytes, 3, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.COMPLEX_STRUCT) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newFixedByteArrayPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.SHORT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.COMPLEX_ARRAY) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newFixedByteArrayPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.LONG_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.PLAIN_LONG_VALUE) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newLVBytesPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY) {
      byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
      return newLVBytesPage(columnSpec, lvVarBytes,
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else {
      throw new UnsupportedOperationException(
          "unsupport uncompress column page: " + meta.getStoreDataType());
    }
  }

  /**
   * Decompress data and create a decimal column page using the decompressed data
   */
  public static ColumnPage decompressDecimalPage(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length) throws MemoryException {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    ColumnPage decimalPage;
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BYTE) {
      byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, byteData.length);
      decimalPage.setBytePage(byteData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT) {
      short[] shortData = compressor.unCompressShort(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, shortData.length);
      decimalPage.setShortPage(shortData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT_INT) {
      byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, shortIntData.length / 3);
      decimalPage.setShortIntPage(shortIntData);
      return decimalPage;
    }  else if (storeDataType == DataTypes.INT) {
      int[] intData = compressor.unCompressInt(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, intData.length);
      decimalPage.setIntPage(intData);
      return decimalPage;
    } else if (storeDataType == DataTypes.LONG) {
      long[] longData = compressor.unCompressLong(compressedData, offset, length);
      decimalPage = createDecimalPage(meta, longData.length);
      decimalPage.setLongPage(longData);
      return decimalPage;
    } else {
      byte[] lvEncodedBytes = compressor.unCompressByte(compressedData, offset, length);
      return newDecimalPage(meta, lvEncodedBytes);
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
