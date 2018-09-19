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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;

import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE_ARRAY;

public class ColumnPageFactory {
  private static final ColumnPageFactory COLUMN_PAGE_FACTORY = new ColumnPageFactory();
  private static final boolean unsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
          CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT));


  private ColumnPageFactory() {

  }

  public static ColumnPageFactory getInstance() {
    return COLUMN_PAGE_FACTORY;
  }

  private ColumnPage createEmptyDecimalPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize) {
    if (unsafe) {
      try {
        return new UnsafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeDecimalColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private ColumnPage createEmptyVarLengthPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize) {
    if (unsafe) {
      try {
        return new UnsafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeVarLengthColumnPage(columnPageEncoderMeta, pageSize);
    }
  }

  private ColumnPage createEmptyFixLengthPage(
      ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize, int eachValueSize) {
    if (unsafe) {
      try {
        return new UnsafeFixLengthColumnPage(columnPageEncoderMeta, pageSize, eachValueSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeFixLengthColumnPage(columnPageEncoderMeta, pageSize, eachValueSize);
    }
  }

  private ColumnPage createEmptyPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    if (DataTypes.isDecimal(columnPageEncoderMeta.getStoreDataType())) {
      return createEmptyDecimalPage(columnPageEncoderMeta, pageSize);
    } else if (columnPageEncoderMeta.getStoreDataType().equals(BYTE_ARRAY)) {
      return createEmptyVarLengthPage(columnPageEncoderMeta, pageSize);
    } else {
      return createEmptyFixLengthPage(columnPageEncoderMeta, pageSize,
          columnPageEncoderMeta.getStoreDataType().getSizeInBytes());
    }
  }

  public ColumnPage newLocalDictPage(ColumnPageEncoderMeta columnPageEncoderMeta,
      int pageSize, LocalDictionaryGenerator localDictionaryGenerator,
      boolean isComplexTypePrimitive) throws MemoryException {
    boolean isDecoderBasedFallBackEnabled = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK,
            CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK_DEFAULT));
    ColumnPage actualPage;
    ColumnPage encodedPage;
    if (unsafe) {
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
          pageSize, DataTypes.BYTE_ARRAY.getSizeInBytes());
    }
    return new LocalDictColumnPage(actualPage, encodedPage, localDictionaryGenerator,
        isComplexTypePrimitive, isDecoderBasedFallBackEnabled);
  }

  private DataType getCorrespondingStoreType(DataType dataType) {
    if (DataTypes.BOOLEAN == dataType) {
      return DataTypes.BYTE;
    } else if (DataTypes.TIMESTAMP == dataType) {
      return DataTypes.LONG;
    } else if (DataTypes.STRING == dataType || DataTypes.VARCHAR == dataType) {
      return DataTypes.BYTE_ARRAY;
    } else {
      return dataType;
    }
  }

  /**
   * Create a new page of dataType and number of row = pageSize
   */
  public ColumnPage newPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize)
      throws MemoryException {
    ColumnPage instance;
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    TableSpec.ColumnSpec columnSpec = columnPageEncoderMeta.getColumnSpec();
    if (columnSpec.getFieldName().equals("stringlocaldictfield")) {
      DataType t = getCorrespondingStoreType(dataType);
    }
    String compressorName = columnPageEncoderMeta.getCompressorName();
    instance = createEmptyPage(
        new ColumnPageEncoderMeta(columnSpec, getCorrespondingStoreType(dataType), compressorName),
        pageSize);
    return instance;
  }

  private ColumnPage createFilledFixedPrimitivePage(TableSpec.ColumnSpec columnSpec,
      DataType storeType, byte[] flattenBytes, String compressorName) {
    ColumnPageEncoderMeta meta = new ColumnPageEncoderMeta(columnSpec, storeType, compressorName);
    ColumnPage columnPage = createEmptyPage(meta, flattenBytes.length / storeType.getSizeInBytes());
    columnPage.setFlattenContentInBytes(flattenBytes);
    return columnPage;
  }

  private ColumnPage createFilledFixedByteArrayPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int eachValueSize, String compressorName) throws MemoryException {
    int pageSize = lvEncodedByteArray.length / eachValueSize;
    ColumnPage fixLengthByteArrayPage = createEmptyFixLengthPage(
        new ColumnPageEncoderMeta(columnSpec, columnSpec.getSchemaDataType(), compressorName),
        pageSize,
        eachValueSize);
    fixLengthByteArrayPage.setFlattenContentInBytes(lvEncodedByteArray);
    return fixLengthByteArrayPage;
  }

  private ColumnPage newDecimalPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, String compressorName) throws MemoryException {
    return VarLengthColumnPageBase.newDecimalColumnPage(
        columnSpec, lvEncodedByteArray, compressorName);
  }

  private ColumnPage newLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int lvLength, String compressorName) throws MemoryException {
    return VarLengthColumnPageBase.newLVBytesColumnPage(
        columnSpec, lvEncodedByteArray, lvLength, compressorName);
  }

  private ColumnPage newComplexLVBytesPage(TableSpec.ColumnSpec columnSpec,
      byte[] lvEncodedByteArray, int lvLength, String compressorName) throws MemoryException {
    return VarLengthColumnPageBase.newComplexLVBytesColumnPage(
        columnSpec, lvEncodedByteArray, lvLength, compressorName);
  }

  /**
   * Decompress data and create a column page using the decompressed data,
   * except for decimal page
   */
  public ColumnPage decompress(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length, boolean isLVEncoded)
      throws MemoryException {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    byte[] decompressedData = compressor.unCompressByte(compressedData, offset, length);
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BOOLEAN ||
        storeDataType == DataTypes.BYTE ||
        storeDataType == DataTypes.SHORT ||
        storeDataType == DataTypes.SHORT_INT ||
        storeDataType == DataTypes.INT ||
        storeDataType == DataTypes.LONG ||
        storeDataType == DataTypes.FLOAT ||
        storeDataType == DataTypes.DOUBLE) {
      return createFilledFixedPrimitivePage(columnSpec, storeDataType, decompressedData,
          meta.getCompressorName());
    } else if (!isLVEncoded && storeDataType == DataTypes.BYTE_ARRAY && (
        columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE
            || columnSpec.getColumnType() == ColumnType.PLAIN_VALUE)) {
      return newComplexLVBytesPage(columnSpec, decompressedData,
          CarbonCommonConstants.SHORT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (isLVEncoded && storeDataType == DataTypes.BYTE_ARRAY &&
        columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE) {
      return createFilledFixedByteArrayPage(
          columnSpec, decompressedData, 3, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.COMPLEX_STRUCT) {
      return createFilledFixedByteArrayPage(columnSpec, decompressedData,
          CarbonCommonConstants.SHORT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.COMPLEX_ARRAY) {
      return createFilledFixedByteArrayPage(columnSpec, decompressedData,
          CarbonCommonConstants.LONG_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY
        && columnSpec.getColumnType() == ColumnType.PLAIN_LONG_VALUE) {
      return newLVBytesPage(columnSpec, decompressedData,
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else if (storeDataType == DataTypes.BYTE_ARRAY) {
      return newLVBytesPage(columnSpec, decompressedData,
          CarbonCommonConstants.INT_SIZE_IN_BYTE, meta.getCompressorName());
    } else {
      throw new UnsupportedOperationException(
          "unsupport uncompress column page: " + meta.getStoreDataType());
    }
  }

  /**
   * Decompress data and create a decimal column page using the decompressed data
   */
  public ColumnPage decompressDecimalPage(ColumnPageEncoderMeta meta, byte[] compressedData,
      int offset, int length) throws MemoryException {
    Compressor compressor = CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
    TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
    ColumnPage decimalPage = null;
    byte[] decompressedData = compressor.unCompressByte(compressedData, offset, length);
    DataType storeDataType = meta.getStoreDataType();
    if (storeDataType == DataTypes.BYTE) {
      decimalPage = createEmptyDecimalPage(meta, decompressedData.length);
      decimalPage.setFlattenContentInBytes(decompressedData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT) {
      short[] shortData  = new short[decompressedData.length / ByteUtil.SIZEOF_SHORT];
      ByteBuffer.wrap(decompressedData).asShortBuffer().get(shortData);
      decimalPage = createEmptyDecimalPage(meta, shortData.length);
      decimalPage.setFlattenContentInBytes(decompressedData);
      return decimalPage;
    } else if (storeDataType == DataTypes.SHORT_INT) {
      decimalPage = createEmptyDecimalPage(meta, decompressedData.length);
      decimalPage.setFlattenContentInBytes(decompressedData);
      return decimalPage;
    } else if (storeDataType == DataTypes.INT) {
      int[] intData  = new int[decompressedData.length / ByteUtil.SIZEOF_INT];
      ByteBuffer.wrap(decompressedData).asIntBuffer().get(intData);
      decimalPage = createEmptyDecimalPage(meta, intData.length);
      decimalPage.setFlattenContentInBytes(decompressedData);
      return decimalPage;
    } else if (storeDataType == DataTypes.LONG) {
      long[] longData  = new long[decompressedData.length / ByteUtil.SIZEOF_LONG];
      ByteBuffer.wrap(decompressedData).asLongBuffer().get(longData);
      decimalPage = createEmptyDecimalPage(meta, longData.length);
      decimalPage.setFlattenContentInBytes(decompressedData);
      return decimalPage;
    } else {
      return newDecimalPage(columnSpec, decompressedData, meta.getCompressorName());
    }
  }
}
