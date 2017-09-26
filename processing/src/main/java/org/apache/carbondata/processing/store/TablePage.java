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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;
import org.apache.carbondata.core.datastore.page.statistics.KeyPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.LVStringStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;

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
  private ColumnPage[] dictDimensionPages;
  private ColumnPage[] noDictDimensionPages;
  private ComplexColumnPage[] complexDimensionPages;
  private ColumnPage[] measurePages;

  // the num of rows in this page, it must be less than short value (65536)
  private int pageSize;

  private CarbonFactDataHandlerModel model;

  private TablePageKey key;

  private EncodedTablePage encodedTablePage;

  private EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();

  // true if it is last page of all input rows
  private boolean isLastPage;

  // used for complex column to deserilize the byte array in input CarbonRow
  private Map<Integer, GenericDataType> complexIndexMap = null;

  TablePage(CarbonFactDataHandlerModel model, int pageSize) throws MemoryException {
    this.model = model;
    this.pageSize = pageSize;
    int numDictDimension = model.getMDKeyGenerator().getDimCount();
    TableSpec tableSpec = model.getTableSpec();
    dictDimensionPages = new ColumnPage[numDictDimension];
    for (int i = 0; i < dictDimensionPages.length; i++) {
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i);
      ColumnPage page = ColumnPage.newPage(spec, DataType.BYTE_ARRAY, pageSize);
      page.setStatsCollector(KeyPageStatsCollector.newInstance(DataType.BYTE_ARRAY));
      dictDimensionPages[i] = page;
    }
    noDictDimensionPages = new ColumnPage[model.getNoDictionaryCount()];
    for (int i = 0; i < noDictDimensionPages.length; i++) {
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i + numDictDimension);
      ColumnPage page = ColumnPage.newPage(spec, DataType.STRING, pageSize);
      page.setStatsCollector(LVStringStatsCollector.newInstance());
      noDictDimensionPages[i] = page;
    }
    complexDimensionPages = new ComplexColumnPage[model.getComplexColumnCount()];
    for (int i = 0; i < complexDimensionPages.length; i++) {
      // here we still do not the depth of the complex column, it will be initialized when
      // we get the first row.
      complexDimensionPages[i] = null;
    }
    measurePages = new ColumnPage[model.getMeasureCount()];
    DataType[] dataTypes = model.getMeasureDataType();
    for (int i = 0; i < measurePages.length; i++) {
      TableSpec.MeasureSpec spec = model.getTableSpec().getMeasureSpec(i);
      ColumnPage page;
      if (spec.getSchemaDataType() == DataType.DECIMAL) {
        page = ColumnPage.newDecimalPage(spec, dataTypes[i], pageSize);
      } else {
        page = ColumnPage.newPage(spec, dataTypes[i], pageSize);
      }
      page.setStatsCollector(
          PrimitivePageStatsCollector.newInstance(
              dataTypes[i], spec.getScale(), spec.getPrecision()));
      measurePages[i] = page;
    }
    boolean hasNoDictionary = noDictDimensionPages.length > 0;
    this.key = new TablePageKey(pageSize, model.getMDKeyGenerator(), model.getSegmentProperties(),
        hasNoDictionary);

    // for complex type, `complexIndexMap` is used in multithread (in multiple Producer),
    // we need to clone the index map to make it thread safe
    this.complexIndexMap = new HashMap<>();
    for (Map.Entry<Integer, GenericDataType> entry: model.getComplexIndexMap().entrySet()) {
      this.complexIndexMap.put(entry.getKey(), entry.getValue().deepCopy());
    }
  }

  /**
   * Add one row to the internal store
   *
   * @param rowId Id of the input row
   * @param row   row object
   */
  public void addRow(int rowId, CarbonRow row) throws KeyGenException {
    // convert each column category, update key and stats
    byte[] mdk = WriteStepRowUtil.getMdk(row, model.getMDKeyGenerator());
    convertToColumnarAndAddToPages(rowId, row, mdk);
    key.update(rowId, row, mdk);
  }

  // convert the input row object to columnar data and add to column pages
  private void convertToColumnarAndAddToPages(int rowId, CarbonRow row, byte[] mdk)
      throws KeyGenException {
    // 1. convert dictionary columns
    byte[][] keys = model.getSegmentProperties().getFixedLengthKeySplitter().splitKey(mdk);
    for (int i = 0; i < dictDimensionPages.length; i++) {
      dictDimensionPages[i].putData(rowId, keys[i]);
    }

    // 2. convert noDictionary columns and complex columns.
    int noDictionaryCount = noDictDimensionPages.length;
    int complexColumnCount = complexDimensionPages.length;
    if (noDictionaryCount > 0 || complexColumnCount > 0) {
      byte[][] noDictAndComplex = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      for (int i = 0; i < noDictAndComplex.length; i++) {
        if (i < noDictionaryCount) {
          // noDictionary columns, since it is variable length, we need to prepare each
          // element as LV result byte array (first two bytes are the length of the array)
          byte[] valueWithLength = addLengthToByteArray(noDictAndComplex[i]);
          noDictDimensionPages[i].putData(rowId, valueWithLength);
        } else {
          // complex columns
          addComplexColumn(i - noDictionaryCount, rowId, noDictAndComplex[i]);
        }
      }
    }

    // 3. convert measure columns
    Object[] measureColumns = WriteStepRowUtil.getMeasure(row);
    for (int i = 0; i < measurePages.length; i++) {
      Object value = measureColumns[i];

      // in compaction flow the measure with decimal type will come as Spark decimal.
      // need to convert it to byte array.
      if (measurePages[i].getDataType() == DataType.DECIMAL &&
          model.isCompactionFlow() &&
          value != null) {
        value = ((Decimal) value).toJavaBigDecimal();
      }
      measurePages[i].putData(rowId, value);
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
    GenericDataType complexDataType = complexIndexMap.get(
        index + model.getPrimitiveDimLens().length);

    // initialize the page if first row
    if (rowId == 0) {
      int depthInComplexColumn = complexDataType.getColsCount();
      complexDimensionPages[index] = new ComplexColumnPage(pageSize, depthInComplexColumn);
    }

    int depthInComplexColumn = complexDimensionPages[index].getDepth();
    // this is the result columnar data which will be added to page,
    // size of this list is the depth of complex column, we will fill it by input data
    List<ArrayList<byte[]>> encodedComplexColumnar = new ArrayList<>(depthInComplexColumn);
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
      complexDimensionPages[index].putComplexData(rowId, depth, encodedComplexColumnar.get(depth));
    }
  }

  void freeMemory() {
    for (ColumnPage page : dictDimensionPages) {
      page.freeMemory();
    }
    for (ColumnPage page : noDictDimensionPages) {
      page.freeMemory();
    }
    for (ColumnPage page : measurePages) {
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

  void encode() throws KeyGenException, MemoryException, IOException {
    // encode dimensions and measure
    EncodedColumnPage[] dimensions = encodeAndCompressDimensions();
    EncodedColumnPage[] measures = encodeAndCompressMeasures();
    this.encodedTablePage = EncodedTablePage.newInstance(pageSize, dimensions, measures, key);
  }

  public EncodedTablePage getEncodedTablePage() {
    return encodedTablePage;
  }

  // apply measure and set encodedData in `encodedData`
  private EncodedColumnPage[] encodeAndCompressMeasures()
      throws MemoryException, IOException {
    EncodedColumnPage[] encodedMeasures = new EncodedColumnPage[measurePages.length];
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageEncoder encoder = encodingFactory.createEncoder(
          model.getTableSpec().getMeasureSpec(i), measurePages[i]);
      encodedMeasures[i] = encoder.encode(measurePages[i]);
    }
    return encodedMeasures;
  }

  // apply and compress each dimension, set encoded data in `encodedData`
  private EncodedColumnPage[] encodeAndCompressDimensions()
      throws KeyGenException, IOException, MemoryException {
    List<EncodedColumnPage> encodedDimensions = new ArrayList<>();
    List<EncodedColumnPage> encodedComplexDimenions = new ArrayList<>();
    TableSpec tableSpec = model.getTableSpec();
    int dictIndex = 0;
    int noDictIndex = 0;
    int complexDimIndex = 0;
    int numDimensions = tableSpec.getNumDimensions();
    for (int i = 0; i < numDimensions; i++) {
      ColumnPageEncoder columnPageEncoder;
      EncodedColumnPage encodedPage;
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i);
      switch (spec.getColumnType()) {
        case GLOBAL_DICTIONARY:
        case DIRECT_DICTIONARY:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              dictDimensionPages[dictIndex]);
          encodedPage = columnPageEncoder.encode(dictDimensionPages[dictIndex++]);
          encodedDimensions.add(encodedPage);
          break;
        case PLAIN_VALUE:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              noDictDimensionPages[noDictIndex]);
          encodedPage = columnPageEncoder.encode(noDictDimensionPages[noDictIndex++]);
          encodedDimensions.add(encodedPage);
          break;
        case COMPLEX:
          EncodedColumnPage[] encodedPages = ColumnPageEncoder.encodeComplexColumn(
              complexDimensionPages[complexDimIndex++]);
          encodedComplexDimenions.addAll(Arrays.asList(encodedPages));
          break;
        default:
          throw new IllegalArgumentException("unsupported dimension type:" + spec
              .getColumnType());
      }
    }

    encodedDimensions.addAll(encodedComplexDimenions);
    return encodedDimensions.toArray(new EncodedColumnPage[encodedDimensions.size()]);
  }

  /**
   * return column page of specified column name
   */
  public ColumnPage getColumnPage(String columnName) {
    int dictDimensionIndex = -1;
    int noDictDimensionIndex = -1;
    ColumnPage page = null;
    TableSpec spec = model.getTableSpec();
    int numDimensions = spec.getNumDimensions();
    for (int i = 0; i < numDimensions; i++) {
      ColumnType type = spec.getDimensionSpec(i).getColumnType();
      if ((type == ColumnType.GLOBAL_DICTIONARY) || (type == ColumnType.DIRECT_DICTIONARY)) {
        page = dictDimensionPages[++dictDimensionIndex];
      } else if (type == ColumnType.PLAIN_VALUE) {
        page = noDictDimensionPages[++noDictDimensionIndex];
      } else {
        // do not support datamap on complex column
        continue;
      }
      String fieldName = spec.getDimensionSpec(i).getFieldName();
      if (fieldName.equalsIgnoreCase(columnName)) {
        return page;
      }
    }
    int numMeasures = spec.getNumMeasures();
    for (int i = 0; i < numMeasures; i++) {
      String fieldName = spec.getMeasureSpec(i).getFieldName();
      if (fieldName.equalsIgnoreCase(columnName)) {
        return measurePages[i];
      }
    }
    throw new IllegalArgumentException("DataMap: must have '" + columnName + "' column in schema");
  }

  public boolean isLastPage() {
    return isLastPage;
  }

  public void setIsLastPage(boolean isWriteAll) {
    this.isLastPage = isWriteAll;
  }

  public int getPageSize() {
    return pageSize;
  }
}


