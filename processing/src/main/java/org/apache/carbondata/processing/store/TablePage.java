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
import java.util.List;

import org.apache.carbondata.core.datastore.GenericDataType;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.KeyColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.MeasurePageStatsVO;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.DataTypeUtil;

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
  private KeyColumnPage keyColumnPage;
  private ColumnPage[] noDictDimensionPage;
  private ComplexColumnPage[] complexDimensionPage;
  private ColumnPage[] measurePage;

  private MeasurePageStatsVO measurePageStatistics;

  // the num of rows in this page, it must be less than short value (65536)
  private int pageSize;

  private CarbonFactDataHandlerModel model;

  TablePage(CarbonFactDataHandlerModel model, int pageSize) {
    this.model = model;
    this.pageSize = pageSize;
    keyColumnPage = new KeyColumnPage(pageSize,
        model.getSegmentProperties().getDimensionPartitions().length);
    noDictDimensionPage = new ColumnPage[model.getNoDictionaryCount()];
    for (int i = 0; i < noDictDimensionPage.length; i++) {
      noDictDimensionPage[i] = new ColumnPage(DataType.STRING, pageSize);
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
      measurePage[i] = new ColumnPage(dataTypes[i], pageSize);
    }
  }

  /**
   * Add one row to the internal store, it will be converted into columnar layout
   *
   * @param rowId Id of the input row
   * @param row   row object
   */
  void addRow(int rowId, CarbonRow row) throws KeyGenException {
    // convert each column category

    // 1. convert dictionary columns
    byte[] mdk = WriteStepRowUtil.getMdk(row, model.getMDKeyGenerator());
    byte[][] keys = model.getSegmentProperties().getFixedLengthKeySplitter().splitKey(mdk);
    keyColumnPage.putKey(rowId, keys);

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

    // update statistics if it is last row
    if (rowId + 1 == pageSize) {
      this.measurePageStatistics = new MeasurePageStatsVO(measurePage);
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
      getComplexDimensionPage()[index] = new ComplexColumnPage(pageSize, depthInComplexColumn);
    }

    int depthInComplexColumn = getComplexDimensionPage()[index].getDepth();
    // this is the result columnar data which will be added to page,
    // size of this list is the depth of complex column, we will fill it by input data
    List<ArrayList<byte[]>> encodedComplexColumnar = new ArrayList<>();
    for (int k = 0; k < depthInComplexColumn; k++) {
      encodedComplexColumnar.add(new ArrayList<byte[]>());
    }

    // encode the complex type data and fill columnsArray
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
      getComplexDimensionPage()[index]
          .putComplexData(rowId, depth, encodedComplexColumnar.get(depth));
    }
  }

  // Adds length as a short element (first 2 bytes) to the head of the input byte array
  private byte[] addLengthToByteArray(byte[] input) {
    byte[] output = new byte[input.length + 2];
    ByteBuffer buffer = ByteBuffer.wrap(output);
    buffer.putShort((short) input.length);
    buffer.put(input, 0, input.length);
    return output;
  }

  public KeyColumnPage getKeyColumnPage() {
    return keyColumnPage;
  }

  public ColumnPage[] getNoDictDimensionPage() {
    return noDictDimensionPage;
  }

  public ComplexColumnPage[] getComplexDimensionPage() {
    return complexDimensionPage;
  }

  public ColumnPage[] getMeasurePage() {
    return measurePage;
  }

  public MeasurePageStatsVO getMeasureStats() {
    return measurePageStatistics;
  }

  public int getPageSize() {
    return pageSize;
  }
}


