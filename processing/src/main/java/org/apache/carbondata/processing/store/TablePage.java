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

package org.apache.carbondata.processing.store;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.datastore.DimensionType;
import org.apache.carbondata.core.datastore.GenericDataType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForShort;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingStrategy;
import org.apache.carbondata.core.datastore.page.encoding.EncodedDimensionPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.datastore.page.encoding.EncodingStrategy;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.datastore.page.statistics.VarLengthPageStatsCollector;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.spark.sql.types.Decimal;

/**
 * Represent a page data for all columns, we store its data in columnar layout, so that
 * all processing apply to TablePage can be done in vectorized fashion.
 */
public class TablePage {

  // For all dimension and measure columns, we store the column data directly in the page,
  // the length of the page is the number of rows.

  // TODO: we should have separate class for key columns so that keys are stored together in
  // one vector to make it efficient for sorting
  private ColumnPage[] dictDimensionPage;
  private ColumnPage[] noDictDimensionPage;
  private ComplexColumnPage[] complexDimensionPage;
  private ColumnPage[] measurePage;

  // the num of rows in this page, it must be less than short value (65536)
  private int pageSize;

  private CarbonFactDataHandlerModel model;

  private TablePageKey key;

  private ColumnarFormatVersion version = CarbonProperties.getInstance().getFormatVersion();

  TablePage(CarbonFactDataHandlerModel model, int pageSize) throws MemoryException {
    this.model = model;
    this.pageSize = pageSize;
    int numDictDimension = model.getMDKeyGenerator().getDimCount();
    dictDimensionPage = new ColumnPage[numDictDimension];
    for (int i = 0; i < dictDimensionPage.length; i++) {
      ColumnPage page = ColumnPage.newVarLengthPath(DataType.BYTE_ARRAY, pageSize);
      page.setStatsCollector(VarLengthPageStatsCollector.newInstance());
      dictDimensionPage[i] = page;
    }
    noDictDimensionPage = new ColumnPage[model.getNoDictionaryCount()];
    for (int i = 0; i < noDictDimensionPage.length; i++) {
      ColumnPage page = ColumnPage.newVarLengthPath(DataType.BYTE_ARRAY, pageSize);
      page.setStatsCollector(VarLengthPageStatsCollector.newInstance());
      noDictDimensionPage[i] = page;
    }
    complexDimensionPage = new ComplexColumnPage[model.getComplexColumnCount()];
    for (int i = 0; i < complexDimensionPage.length; i++) {
      // here we still do not the depth of the complex column, it will be initialized when
      // we get the first row.
      complexDimensionPage[i] = null;
    }
    measurePage = new ColumnPage[model.getMeasureCount()];
    DataType[] dataTypes = model.getMeasureDataType();
    for (int i = 0; i < measurePage.length; i++) {
      ColumnPage page = ColumnPage.newPage(dataTypes[i], pageSize);
      page.setStatsCollector(PrimitivePageStatsCollector.newInstance(dataTypes[i], pageSize));
      measurePage[i] = page;
    }
    boolean hasNoDictionary = noDictDimensionPage.length > 0;
    this.key = new TablePageKey(pageSize, model.getMDKeyGenerator(), model.getSegmentProperties(),
        hasNoDictionary);
  }

  /**
   * Add one row to the internal store
   *
   * @param rowId Id of the input row
   * @param row   row object
   */
  public void addRow(int rowId, CarbonRow row) throws KeyGenException {
    // convert each column category, update key and stats
    convertToColumnar(rowId, row);
    key.update(rowId, row);
  }

  private void convertToColumnar(int rowId, CarbonRow row) throws KeyGenException {
    // 1. convert dictionary columns
    byte[] mdk = WriteStepRowUtil.getMdk(row, model.getMDKeyGenerator());
    byte[][] keys = model.getSegmentProperties().getFixedLengthKeySplitter().splitKey(mdk);
    for (int i = 0; i < dictDimensionPage.length; i++) {
      dictDimensionPage[i].putData(rowId, keys[i]);
    }

    // 2. convert noDictionary columns and complex columns.
    int noDictionaryCount = noDictDimensionPage.length;
    int complexColumnCount = complexDimensionPage.length;
    if (noDictionaryCount > 0 || complexColumnCount > 0) {
      byte[][] noDictAndComplex = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      for (int i = 0; i < noDictAndComplex.length; i++) {
        if (i < noDictionaryCount) {
          // noDictionary columns, since it is variable length, we need to prepare each
          // element as LV result byte array (first two bytes are the length of the array)
          byte[] valueWithLength = addLengthToByteArray(noDictAndComplex[i]);
          noDictDimensionPage[i].putData(rowId, valueWithLength);
        } else {
          // complex columns
          addComplexColumn(i - noDictionaryCount, rowId, noDictAndComplex[i]);
        }
      }
    }

    // 3. convert measure columns
    Object[] measureColumns = WriteStepRowUtil.getMeasure(row);
    for (int i = 0; i < measurePage.length; i++) {
      Object value = measureColumns[i];

      // in compaction flow the measure with decimal type will come as Spark decimal.
      // need to convert it to byte array.
      if (measurePage[i].getDataType() == DataType.DECIMAL &&
          model.isCompactionFlow() &&
          value != null) {
        BigDecimal bigDecimal = ((Decimal) value).toJavaBigDecimal();
        value = DataTypeUtil.bigDecimalToByte(bigDecimal);
      }
      measurePage[i].putData(rowId, value);
    }
  }

  /**
   * add a complex column into internal member compleDimensionPage
   *
   * @param index          index of the complexDimensionPage
   * @param rowId          Id of the input row
   * @param complexColumns byte array the complex columm to be added, extracted of input row
   */
  // TODO: this function should be refactoried, ColumnPage should support complex type encoding
  // directly instead of doing it here
  private void addComplexColumn(int index, int rowId, byte[] complexColumns) {
    GenericDataType complexDataType = model.getComplexIndexMap().get(
        index + model.getPrimitiveDimLens().length);

    // initialize the page if first row
    if (rowId == 0) {
      int depthInComplexColumn = complexDataType.getColsCount();
      complexDimensionPage[index] = new ComplexColumnPage(pageSize, depthInComplexColumn);
    }

    int depthInComplexColumn = complexDimensionPage[index].getDepth();
    // this is the result columnar data which will be added to page,
    // size of this list is the depth of complex column, we will fill it by input data
    List<ArrayList<byte[]>> encodedComplexColumnar = new ArrayList<>();
    for (int k = 0; k < depthInComplexColumn; k++) {
      encodedComplexColumnar.add(new ArrayList<byte[]>());
    }

    // apply the complex type data and fill columnsArray
    try {
      ByteBuffer byteArrayInput = ByteBuffer.wrap(complexColumns);
      ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutput);
      complexDataType.parseAndBitPack(byteArrayInput, dataOutputStream,
          model.getComplexDimensionKeyGenerator());
      complexDataType.getColumnarDataForComplexType(encodedComplexColumnar,
          ByteBuffer.wrap(byteArrayOutput.toByteArray()));
      byteArrayOutput.close();
    } catch (IOException | KeyGenException e) {
      throw new CarbonDataWriterException("Problem while bit packing and writing complex datatype",
          e);
    }

    for (int depth = 0; depth < depthInComplexColumn; depth++) {
      complexDimensionPage[index].putComplexData(rowId, depth, encodedComplexColumnar.get(depth));
    }
  }

  void freeMemory() {
    for (ColumnPage page : dictDimensionPage) {
      page.freeMemory();
    }
    for (ColumnPage page : noDictDimensionPage) {
      page.freeMemory();
    }
    for (ColumnPage page : measurePage) {
      page.freeMemory();
    }
  }

  // Adds length as a short element (first 2 bytes) to the head of the input byte array
  private byte[] addLengthToByteArray(byte[] input) {
    if (input.length > Short.MAX_VALUE) {
      throw new RuntimeException("input data length " + input.length +
          " bytes too long, maximum length supported is " + Short.MAX_VALUE + " bytes");
    }
    byte[] output = new byte[input.length + 2];
    ByteBuffer buffer = ByteBuffer.wrap(output);
    buffer.putShort((short)input.length);
    buffer.put(input, 0, input.length);
    return output;
  }

  EncodedTablePage encode() throws KeyGenException, MemoryException, IOException {
    // encode dimensions and measure
    EncodedDimensionPage[] dimensions = encodeAndCompressDimensions();
    EncodedMeasurePage[] measures = encodeAndCompressMeasures();
    return EncodedTablePage.newInstance(pageSize, dimensions, measures, key);
  }

  private EncodingStrategy encodingStrategy = new DefaultEncodingStrategy();

  // apply measure and set encodedData in `encodedData`
  private EncodedMeasurePage[] encodeAndCompressMeasures()
      throws MemoryException, IOException {
    EncodedMeasurePage[] encodedMeasures = new EncodedMeasurePage[measurePage.length];
    for (int i = 0; i < measurePage.length; i++) {
      ColumnPageCodec encoder =
          encodingStrategy.createCodec((SimpleStatsResult)(measurePage[i].getStatistics()));
      encodedMeasures[i] = (EncodedMeasurePage) encoder.encode(measurePage[i]);
    }
    return encodedMeasures;
  }

  private IndexStorage encodeAndCompressDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex, boolean isRleApplicable) throws KeyGenException {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, isRleApplicable, false, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, isRleApplicable, false, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(data, false);
      } else {
        return new BlockIndexerStorageForNoInvertedIndexForInt(data);
      }
    }
  }

  private IndexStorage encodeAndCompressDirectDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex, boolean isRleApplicable) throws KeyGenException {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, isRleApplicable, false, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, isRleApplicable, false, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(data, false);
      } else {
        return new BlockIndexerStorageForNoInvertedIndexForInt(data);
      }
    }
  }

  private IndexStorage encodeAndCompressComplexDimension(byte[][] data) {
    if (version == ColumnarFormatVersion.V3) {
      return new BlockIndexerStorageForShort(data, false, false, false);
    } else {
      return new BlockIndexerStorageForInt(data, false, false, false);
    }
  }

  private IndexStorage encodeAndCompressNoDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex, boolean isRleApplicable) {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, isRleApplicable, true, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, isRleApplicable, true, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(data, true);
      } else {
        return new BlockIndexerStorageForNoInvertedIndexForInt(data);
      }
    }
  }

  // apply and compress each dimension, set encoded data in `encodedData`
  private EncodedDimensionPage[] encodeAndCompressDimensions()
      throws KeyGenException {
    TableSpec.DimensionSpec dimensionSpec = model.getTableSpec().getDimensionSpec();
    int dictionaryColumnCount = -1;
    int noDictionaryColumnCount = -1;
    int indexStorageOffset = 0;
    IndexStorage[] indexStorages = new IndexStorage[dimensionSpec.getNumExpandedDimensions()];
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    EncodedDimensionPage[] compressedColumns = new EncodedDimensionPage[indexStorages.length];
    boolean[] isUseInvertedIndex = model.getIsUseInvertedIndex();
    for (int i = 0; i < dimensionSpec.getNumSimpleDimensions(); i++) {
      ColumnPage page;
      byte[] flattened;
      boolean isSortColumn = model.isSortColumn(i);
      switch (dimensionSpec.getType(i)) {
        case GLOBAL_DICTIONARY:
          // dictionary dimension
          page = dictDimensionPage[++dictionaryColumnCount];
          indexStorages[indexStorageOffset] = encodeAndCompressDictDimension(
              page.getByteArrayPage(),
              isSortColumn,
              isUseInvertedIndex[i] & isSortColumn,
              CarbonDataProcessorUtil.isRleApplicableForColumn(dimensionSpec.getType(i)));
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getDataPage());
          break;
        case DIRECT_DICTIONARY:
          // timestamp and date column
          page = dictDimensionPage[++dictionaryColumnCount];
          indexStorages[indexStorageOffset] = encodeAndCompressDirectDictDimension(
              page.getByteArrayPage(),
              isSortColumn,
              isUseInvertedIndex[i] & isSortColumn,
              CarbonDataProcessorUtil.isRleApplicableForColumn(dimensionSpec.getType(i)));
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getDataPage());
          break;
        case PLAIN_VALUE:
          // high cardinality dimension, encoded as plain string
          page = noDictDimensionPage[++noDictionaryColumnCount];
          indexStorages[indexStorageOffset] = encodeAndCompressNoDictDimension(
              page.getByteArrayPage(),
              isSortColumn,
              isUseInvertedIndex[i] & isSortColumn,
              CarbonDataProcessorUtil.isRleApplicableForColumn(dimensionSpec.getType(i)));
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getDataPage());
          break;
        case COMPLEX:
          // we need to add complex column at last, so skipping it here
          continue;
        default:
          throw new RuntimeException("unsupported dimension type: " + dimensionSpec.getType(i));
      }
      byte[] compressedData = compressor.compressByte(flattened);
      compressedColumns[indexStorageOffset] = new EncodedDimensionPage(
          pageSize, compressedData, indexStorages[indexStorageOffset], dimensionSpec.getType(i));
      indexStorageOffset++;
    }

    // handle complex type column
    for (int i = 0; i < dimensionSpec.getNumComplexDimensions(); i++) {
      Iterator<byte[][]> iterator = complexDimensionPage[i].iterator();
      while (iterator.hasNext()) {
        byte[][] data = iterator.next();
        indexStorages[indexStorageOffset] = encodeAndCompressComplexDimension(data);
        byte[] flattened = ByteUtil.flatten(data);
        byte[] compressedData = compressor.compressByte(flattened);
        compressedColumns[indexStorageOffset] = new EncodedDimensionPage(
            pageSize, compressedData, indexStorages[indexStorageOffset], DimensionType.COMPLEX);
        indexStorageOffset++;
      }
    }
    return compressedColumns;
  }
}


