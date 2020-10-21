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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.StringStatsCollector;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;

import org.apache.log4j.Logger;

/**
 * Represent a page data for all columns, we store its data in columnar layout, so that
 * all processing apply to TablePage can be done in vectorized fashion.
 */
public class TablePage {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(TablePage.class.getName());

  // For all dimension and measure columns, we store the column data directly in the page,
  // the length of the page is the number of rows.

  // TODO: we should have separate class for key columns so that keys are stored together in
  // one vector to make it efficient for sorting
  private ColumnPage[] dictDimensionPages;
  private ColumnPage[] noDictDimensionPages;
  private ColumnPage[] measurePages;
  private ComplexColumnPage[] complexDimensionPages;

  // the num of rows in this page, it must be less than short value (65536)
  private int pageSize;

  private CarbonFactDataHandlerModel model;

  private EncodedTablePage encodedTablePage;

  private EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();

  // true if it is last page of all input rows
  private boolean isLastPage;

  // used for complex column to deserilize the byte array in input CarbonRow
  private Map<Integer, GenericDataType> complexIndexMap = null;
  // name of compressor that used to compress column data,
  // currently all the columns share the same compressor.
  private String columnCompressor;

  TablePage(CarbonFactDataHandlerModel model, int pageSize) {
    this.model = model;
    this.pageSize = pageSize;
    TableSpec tableSpec = model.getTableSpec();
    this.columnCompressor = model.getColumnCompressor();

    dictDimensionPages = new ColumnPage[model.getDictDimensionCount()];
    noDictDimensionPages = new ColumnPage[model.getNoDictionaryCount()];
    int tmpNumDictDimIdx = 0;
    int tmpNumNoDictDimIdx = 0;
    for (int i = 0; i < tableSpec.getNumDimensions(); i++) {
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i);
      if (spec.getSchemaDataType().isComplexType()) {
        // skip complex columns and go other dimensions.
        // partition scenario dimensions can present after complex columns.
        continue;
      }
      ColumnPage page;
      if (spec.getSchemaDataType() == DataTypes.DATE) {
        TableSpec.DimensionSpec dimensionSpec = TableSpec.DimensionSpec
            .newInstance(spec.getFieldName(), DataTypes.INT, spec.getColumnType());
        ColumnPageEncoderMeta columnPageEncoderMeta =
            new ColumnPageEncoderMeta(dimensionSpec, DataTypes.INT, columnCompressor);
        page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
        page.setStatsCollector(PrimitivePageStatsCollector.newInstance(DataTypes.INT));
        dictDimensionPages[tmpNumDictDimIdx++] = page;
      } else {
        if (DataTypeUtil.isPrimitiveColumn(spec.getSchemaDataType())) {
          ColumnPageEncoderMeta columnPageEncoderMeta;
          if (spec.getSchemaDataType() == DataTypes.TIMESTAMP) {
            columnPageEncoderMeta =
                new ColumnPageEncoderMeta(spec, DataTypes.LONG, columnCompressor);
          } else {
            columnPageEncoderMeta =
                new ColumnPageEncoderMeta(spec, spec.getSchemaDataType(), columnCompressor);
          }
          // create the column page according to the data type for no dictionary numeric columns
          if (DataTypes.isDecimal(spec.getSchemaDataType())) {
            page = ColumnPage.newDecimalPage(columnPageEncoderMeta, pageSize);
          } else {
            page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
          }
          if (spec.getSchemaDataType() == DataTypes.TIMESTAMP) {
            page.setStatsCollector(PrimitivePageStatsCollector.newInstance(DataTypes.LONG));
          } else {
            page.setStatsCollector(
                PrimitivePageStatsCollector.newInstance(spec.getSchemaDataType()));
          }
        } else {
          DataType dataType = DataTypes.STRING;
          if (DataTypes.VARCHAR == spec.getSchemaDataType()) {
            dataType = DataTypes.VARCHAR;
          } else if (DataTypes.BINARY == spec.getSchemaDataType()) {
            dataType = DataTypes.BINARY;
          }
          // will be encoded using string page
          LocalDictionaryGenerator localDictionaryGenerator =
              model.getColumnLocalDictGenMap().get(spec.getFieldName());
          ColumnPageEncoderMeta columnPageEncoderMeta =
              new ColumnPageEncoderMeta(spec, dataType, columnCompressor);
          if (null != localDictionaryGenerator) {
            page = ColumnPage
                .newLocalDictPage(columnPageEncoderMeta, pageSize, localDictionaryGenerator);
          } else {
            page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
          }
          page.setStatsCollector(new StringStatsCollector());
        }
        noDictDimensionPages[tmpNumNoDictDimIdx++] = page;
      }
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
      ColumnPageEncoderMeta columnPageEncoderMeta =
          new ColumnPageEncoderMeta(model.getTableSpec().getMeasureSpec(i), dataTypes[i],
              columnCompressor);
      ColumnPage page;
      if (DataTypes.isDecimal(columnPageEncoderMeta.getSchemaDataType())) {
        page = ColumnPage.newDecimalPage(columnPageEncoderMeta, pageSize);
      } else {
        page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
      }
      page.setStatsCollector(PrimitivePageStatsCollector.newInstance(dataTypes[i]));
      measurePages[i] = page;
    }

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
  public void addRow(int rowId, CarbonRow row) {
    // convert each column category, update key and stats
    convertToColumnarAndAddToPages(rowId, row);
  }

  // convert the input row object to columnar data and add to column pages
  private void convertToColumnarAndAddToPages(int rowId, CarbonRow row) {
    // 1. convert dictionary columns
    int[] dictDimensions = WriteStepRowUtil.getDictDimension(row);
    for (int i = 0; i < dictDimensionPages.length; i++) {
      dictDimensionPages[i].putData(rowId,
          dictDimensions[i] == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY ?
              null :
              dictDimensions[i], CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY);
    }

    // 2. convert noDictionary columns and complex columns and varchar, binary columns.
    int noDictionaryCount = noDictDimensionPages.length;
    int complexColumnCount = complexDimensionPages.length;
    if (noDictionaryCount > 0 || complexColumnCount > 0) {
      Object[] noDictAndComplex = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      for (int i = 0; i < noDictAndComplex.length; i++) {
        if (i < noDictionaryCount) {
          if (DataTypeUtil
              .isPrimitiveColumn(noDictDimensionPages[i].getColumnSpec().getSchemaDataType())) {
            // put the actual data to the row
            Object value = noDictAndComplex[i];
            // in compaction flow the measure with decimal type will come as Spark decimal.
            // need to convert it to byte array.
            if (DataTypes.isDecimal(noDictDimensionPages[i].getDataType()) && model
                .isCompactionFlow() && value != null) {
              value = DataTypeUtil.getDataTypeConverter().convertFromDecimalToBigDecimal(value);
            }
            noDictDimensionPages[i].putData(rowId, value);
          } else {
            noDictDimensionPages[i].putData(rowId, noDictAndComplex[i]);
          }
        } else {
          // complex columns
          addComplexColumn(i - noDictionaryCount, rowId,
              (List<ArrayList<byte[]>>) noDictAndComplex[i]);
        }
      }
    }
    // 3. convert measure columns
    Object[] measureColumns = WriteStepRowUtil.getMeasure(row);
    for (int i = 0; i < measurePages.length; i++) {
      Object value = measureColumns[i];

      // in compaction flow the measure with decimal type will come as Spark decimal.
      // need to convert it to byte array.
      if (DataTypes.isDecimal(measurePages[i].getDataType()) &&
          model.isCompactionFlow() &&
          value != null) {
        value = DataTypeUtil.getDataTypeConverter().convertFromDecimalToBigDecimal(value);
      }
      measurePages[i].putData(rowId, value);
    }
  }

  /**
   * add a complex column into internal member complexDimensionPage
   *
   * @param index          index of the complexDimensionPage
   * @param rowId          Id of the input row
   * @param encodedComplexColumnar flatten data of complex column
   */
  // TODO: this function should be refactoried, ColumnPage should support complex type encoding
  // directly instead of doing it here
  private void addComplexColumn(int index, int rowId,
      List<ArrayList<byte[]>> encodedComplexColumnar) {
    GenericDataType complexDataType = complexIndexMap.get(
        index + model.getSegmentProperties().getNumberOfPrimitiveDimensions());
    // initialize the page if first row
    if (rowId == 0) {
      List<ComplexColumnInfo> complexColumnInfoList = new ArrayList<>();
      complexDataType.getComplexColumnInfo(complexColumnInfoList);
      complexDimensionPages[index] = new ComplexColumnPage(complexColumnInfoList);
      complexDimensionPages[index].initialize(
          model.getColumnLocalDictGenMap(), pageSize, columnCompressor);
    }
    int depthInComplexColumn = complexDimensionPages[index].getComplexColumnIndex();
    for (int depth = 0; depth < depthInComplexColumn; depth++) {
      complexDimensionPages[index].putComplexData(depth, encodedComplexColumnar.get(depth));
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
    for (ComplexColumnPage page : complexDimensionPages) {
      if (null != page) {
        page.freeMemory();
      }
    }
  }

  // Adds length as a short element (first 2 bytes) to the head of the input byte array
  private byte[] addShortLengthToByteArray(byte[] input) {
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

  void encode() throws IOException {
    // encode dimensions and measure
    EncodedColumnPage[] dimensions = encodeAndCompressDimensions();
    EncodedColumnPage[] measures = encodeAndCompressMeasures();
    this.encodedTablePage = EncodedTablePage.newInstance(pageSize, dimensions, measures);
  }

  public EncodedTablePage getEncodedTablePage() {
    return encodedTablePage;
  }

  // apply measure and set encodedData in `encodedData`
  private EncodedColumnPage[] encodeAndCompressMeasures() throws IOException {
    EncodedColumnPage[] encodedMeasures = new EncodedColumnPage[measurePages.length];
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageEncoder encoder = encodingFactory.createEncoder(
          model.getTableSpec().getMeasureSpec(i), measurePages[i], null);
      encodedMeasures[i] = encoder.encode(measurePages[i]);
    }
    return encodedMeasures;
  }

  // apply and compress each dimension, set encoded data in `encodedData`
  private EncodedColumnPage[] encodeAndCompressDimensions() throws IOException {
    List<EncodedColumnPage> encodedDimensions = new ArrayList<>();
    List<EncodedColumnPage> encodedComplexDimensions = new ArrayList<>();
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
        case DIRECT_DICTIONARY:
          KeyGenerator keyGenerator =
              KeyGeneratorFactory.getKeyGenerator(new int[] { Integer.MAX_VALUE });
          Map<String, Object> encoderParameter = new HashMap<>();
          encoderParameter.put("keygenerator", keyGenerator);
          columnPageEncoder =
              encodingFactory.createEncoder(spec, dictDimensionPages[dictIndex], encoderParameter);
          encodedPage = columnPageEncoder.encode(dictDimensionPages[dictIndex++]);
          encodedDimensions.add(encodedPage);
          break;
        case PLAIN_VALUE:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              noDictDimensionPages[noDictIndex], null);
          encodedPage = columnPageEncoder.encode(noDictDimensionPages[noDictIndex]);
          if (LOGGER.isDebugEnabled()) {
            DataType targetDataType =
                columnPageEncoder.getTargetDataType(noDictDimensionPages[noDictIndex]);
            if (null != targetDataType) {
              LOGGER.debug(
                  "Encoder result ---> Source data type: " + noDictDimensionPages[noDictIndex]
                      .getDataType().getName() + " Destination data type: " + targetDataType
                      .getName() + " for the column: " + noDictDimensionPages[noDictIndex]
                      .getColumnSpec().getFieldName() + " having encoding type: "
                      + columnPageEncoder.getEncodingType());
            }
          }
          noDictIndex++;
          encodedDimensions.add(encodedPage);
          break;
        case COMPLEX:
          EncodedColumnPage[] encodedPages = ColumnPageEncoder.encodeComplexColumn(
              complexDimensionPages[complexDimIndex++]);
          encodedComplexDimensions.addAll(Arrays.asList(encodedPages));
          break;
        default:
          throw new IllegalArgumentException("unsupported dimension type:" + spec
              .getColumnType());
      }
    }

    encodedDimensions.addAll(encodedComplexDimensions);
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
      if (spec.getDimensionSpec(i).getSchemaDataType() == DataTypes.DATE) {
        page = dictDimensionPages[++dictDimensionIndex];
      } else if (type == ColumnType.PLAIN_VALUE) {
        page = noDictDimensionPages[++noDictDimensionIndex];
      } else {
        // do not support index on complex column
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
    throw new IllegalArgumentException("Index: must have '" + columnName + "' column in schema");
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


