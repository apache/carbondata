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
import java.util.List;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodingStrategyFactory;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;
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

  private ColumnPage[] dimensionPages;
  private ComplexColumnPage[] complexDimensionPages;
  private ColumnPage[] measurePages;

  // the num of rows in this page, it must be less than short value (65536)
  private int pageSize;

  private CarbonFactDataHandlerModel model;

  private TableSpec tableSpec;

  private TablePageKey key;

  private EncodedTablePage encodedTablePage;

  private EncodingFactory encodingFactory = EncodingStrategyFactory.getStrategy();

  // true if it is last page of all input rows
  private boolean isLastPage;

  TablePage(CarbonFactDataHandlerModel model, int pageSize) throws MemoryException {
    this.model = model;
    this.pageSize = pageSize;
    this.tableSpec = model.getTableSpec();
    boolean hasNoDictionary = false;
    TableSpec.DimensionSpec[] dictDimensionSpec = tableSpec.getDimensionSpec();
    dimensionPages = new ColumnPage[dictDimensionSpec.length];
    for (int i = 0; i < dimensionPages.length; i++) {
      TableSpec.DimensionSpec spec = dictDimensionSpec[i];
      switch (spec.getColumnType()) {
        case GLOBAL_DICTIONARY:
          ColumnPage page = ColumnPage.newPage(spec, DataType.BYTE_ARRAY, pageSize);
          dimensionPages[i] = page;
          break;
        case DIRECT_DICTIONARY:
          page = ColumnPage.newPage(spec, DataType.INT, pageSize);
          dimensionPages[i] = page;
          break;
        case PLAIN_VALUE:
          page = ColumnPage.newPage(spec, spec.getSchemaDataType(), pageSize);
          dimensionPages[i] = page;
          hasNoDictionary = true;
          break;
        default:
          throw new RuntimeException("internal error");
      }
    }

    TableSpec.DimensionSpec[] complexDimensionSpec = tableSpec.getComplexDimensionSpec();
    complexDimensionPages = new ComplexColumnPage[complexDimensionSpec.length];
    for (int i = 0; i < complexDimensionPages.length; i++) {
      // here we still do not the depth of the complex column, it will be initialized when
      // we get the first row.
      complexDimensionPages[i] = null;
    }

    TableSpec.MeasureSpec[] measureSpec = tableSpec.getMeasureSpec();
    measurePages = new ColumnPage[measureSpec.length];
    for (int i = 0; i < measurePages.length; i++) {
      TableSpec.MeasureSpec spec = measureSpec[i];
      DataType dataType = spec.getSchemaDataType();
      ColumnPage page;
      if (dataType == DataType.DECIMAL) {
        page = ColumnPage.newDecimalPage(spec, DataType.DECIMAL, pageSize,
            spec.getScale(), spec.getPrecision());
      } else {
        page = ColumnPage.newPage(spec, dataType, pageSize);
      }
      measurePages[i] = page;
    }
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
    convertToColumnarAndAddToPages(rowId, row);
    key.update(rowId, row);
  }

  // convert the input row object to columnar data and add to column pages
  private void convertToColumnarAndAddToPages(int rowId, CarbonRow row)
      throws KeyGenException {
    // 1. convert dictionary and plan columns
    TableSpec.DimensionSpec[] dimensionSpecs = model.getTableSpec().getDimensionSpec();
    int[] surrogate = WriteStepRowUtil.getDictDimension(row);
    byte[] mdk = null;
    byte[][] keys = null;
    if (surrogate.length > 0) {
      mdk = model.getMDKeyGenerator().generateKey(surrogate);
      keys = model.getSegmentProperties().getFixedLengthKeySplitter().splitKey(mdk);
    }
    byte[][] plainValues = WriteStepRowUtil.getNoDictAndComplexDimension(row);
    int dictValueIndex = 0;
    int plainValueIndex = 0;
    for (int i = 0; i < dimensionSpecs.length; i++) {
      TableSpec.DimensionSpec spec = dimensionSpecs[i];
      switch (spec.getColumnType()) {
        case GLOBAL_DICTIONARY:
          assert (keys != null);
          dimensionPages[i].putData(rowId, keys[dictValueIndex++]);
          break;
        case DIRECT_DICTIONARY:
          dimensionPages[i].putData(rowId, surrogate[dictValueIndex++]);
          break;
        case PLAIN_VALUE:
          dimensionPages[i].putData(rowId, plainValues[plainValueIndex]);
          plainValueIndex++;
          break;
        default:
      }
    }

    // 2. convert complex columns.
    int numComplexDimensions = tableSpec.getComplexDimensionSpec().length;
    if (numComplexDimensions > 0) {
      for (int i = 0; i < numComplexDimensions; i++) {
        addComplexColumn(i, rowId, plainValues[plainValueIndex++]);
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
    GenericDataType complexDataType = model.getComplexIndexMap().get(
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
    for (ColumnPage page : dimensionPages) {
      page.freeMemory();
    }
    for (ColumnPage page : measurePages) {
      page.freeMemory();
    }
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
    TableSpec.MeasureSpec[] measureSpecs = tableSpec.getMeasureSpec();
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageEncoder encoder =
          encodingFactory.createEncoder(measureSpecs[i], measurePages[i]);
      encodedMeasures[i] = encoder.encode(measurePages[i]);
    }
    return encodedMeasures;
  }

  // apply and compress each dimension, set encoded data in `encodedData`
  private EncodedColumnPage[] encodeAndCompressDimensions()
      throws KeyGenException, IOException, MemoryException {
    List<EncodedColumnPage> encodedDimensions = new ArrayList<>();
    List<EncodedColumnPage> encodedComplexDimenions = new ArrayList<>();
    TableSpec.DimensionSpec[] dictDimension = tableSpec.getDimensionSpec();
    for (int i = 0; i < dimensionPages.length; i++) {
      ColumnPageEncoder columnPageEncoder =
          encodingFactory.createEncoder(dictDimension[i], dimensionPages[i]);
      EncodedColumnPage encodedPage = columnPageEncoder.encode(dimensionPages[i]);
      encodedDimensions.add(encodedPage);
    }

    for (int i = 0; i < complexDimensionPages.length; i++) {
      EncodedColumnPage[] encodedPages =
          ColumnPageEncoder.encodeComplexColumn(complexDimensionPages[i]);
      encodedComplexDimenions.addAll(Arrays.asList(encodedPages));
    }

    encodedDimensions.addAll(encodedComplexDimenions);
    return encodedDimensions.toArray(new EncodedColumnPage[encodedDimensions.size()]);
  }

  /**
   * return column page of specified column name
   */
  public ColumnPage getColumnPage(String columnName) {
    TableSpec.DimensionSpec[] dictDimensions = tableSpec.getDimensionSpec();
    for (int i = 0; i < dictDimensions.length; i++) {
      String fieldName = dictDimensions[i].getFieldName();
      if (fieldName.equalsIgnoreCase(columnName)) {
        return dimensionPages[i];
      }
    }

    TableSpec.MeasureSpec[] measures = tableSpec.getMeasureSpec();
    for (int i = 0; i < measures.length; i++) {
      String fieldName = measures[i].getFieldName();
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


