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

package org.apache.carbondata.core.scan.filter.executer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.store.ColumnPageWrapper;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.MatchExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.log4j.Logger;

public class RowLevelFilterExecuterImpl implements FilterExecuter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(RowLevelFilterExecuterImpl.class.getName());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
  List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList;
  List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList;
  protected Expression exp;
  protected AbsoluteTableIdentifier tableIdentifier;
  protected SegmentProperties segmentProperties;
  /**
   * it has index at which given dimension is stored in file
   */
  int[] dimensionChunkIndex;

  /**
   * it has index at which given measure is stored in file.
   * Note: Its value can be used only for isScanRequired method as the value is incremented by
   * last dimension ordinal in segmentProperties because of data writing order
   * (first dimensions are written and then measures). Therefore avoid its usage in in methods other
   * than isScanRequired
   */
  int[] measureChunkIndex;

  private Map<Integer, GenericQueryType> complexDimensionInfoMap;

  /**
   * flag to check whether the filter dimension is present in current block list of dimensions.
   * Applicable for restructure scenarios
   */
  boolean[] isDimensionPresentInCurrentBlock;

  /**
   * flag to check whether the filter measure is present in current block list of measures.
   * Applicable for restructure scenarios
   */
  boolean[] isMeasurePresentInCurrentBlock;

  /**
   * is dimension column data is natural sorted
   */
  boolean isNaturalSorted;

  public RowLevelFilterExecuterImpl(List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap) {
    this.segmentProperties = segmentProperties;
    if (null == dimColEvaluatorInfoList) {
      this.dimColEvaluatorInfoList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    } else {
      this.dimColEvaluatorInfoList = dimColEvaluatorInfoList;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    if (this.dimColEvaluatorInfoList.size() > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
      this.isDimensionPresentInCurrentBlock = new boolean[this.dimColEvaluatorInfoList.size()];
      this.dimensionChunkIndex = new int[this.dimColEvaluatorInfoList.size()];
    } else {
      this.isDimensionPresentInCurrentBlock = new boolean[]{false};
      this.dimensionChunkIndex = new int[]{0};
    }
    if (null == msrColEvalutorInfoList) {
      this.msrColEvalutorInfoList = new ArrayList<MeasureColumnResolvedFilterInfo>(20);
    } else {
      this.msrColEvalutorInfoList = msrColEvalutorInfoList;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    if (this.msrColEvalutorInfoList.size() > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
      this.isMeasurePresentInCurrentBlock = new boolean[this.msrColEvalutorInfoList.size()];
      this.measureChunkIndex = new int[this.msrColEvalutorInfoList.size()];
    } else {
      this.isMeasurePresentInCurrentBlock = new boolean[]{false};
      this.measureChunkIndex = new int[] {0};
    }
    this.exp = exp;
    this.tableIdentifier = tableIdentifier;
    this.complexDimensionInfoMap = complexDimensionInfoMap;
    initDimensionChunkIndexes();
    initMeasureChunkIndexes();
  }

  /**
   * This method will initialize the dimension info for the current block to be
   * used for filtering the data
   */
  private void initDimensionChunkIndexes() {
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      // find the dimension in the current block dimensions list
      CarbonDimension dimensionFromCurrentBlock = segmentProperties
          .getDimensionFromCurrentBlock(dimColEvaluatorInfoList.get(i).getDimension());
      if (null != dimensionFromCurrentBlock) {
        dimColEvaluatorInfoList.get(i).setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
        this.dimensionChunkIndex[i] =
            dimColEvaluatorInfoList.get(i).getColumnIndexInMinMaxByteArray();
        isDimensionPresentInCurrentBlock[i] = true;
      }
    }
  }

  /**
   * This method will initialize the measure info for the current block to be
   * used for filtering the data
   */
  private void initMeasureChunkIndexes() {
    for (int i = 0; i < msrColEvalutorInfoList.size(); i++) {
      // find the measure in the current block measures list
      CarbonMeasure measureFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(msrColEvalutorInfoList.get(i).getMeasure());
      if (null != measureFromCurrentBlock) {
        msrColEvalutorInfoList.get(i).setColumnIndex(measureFromCurrentBlock.getOrdinal());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
        this.measureChunkIndex[i] = msrColEvalutorInfoList.get(i).getColumnIndexInMinMaxByteArray();
        isMeasurePresentInCurrentBlock[i] = true;
      }
    }
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
    if (exp instanceof MatchExpression) {
      BitSetGroup bitSetGroup = rawBlockletColumnChunks.getBitSetGroup();
      if (bitSetGroup == null) {
        // It means there are no index created on this table
        throw new FilterUnsupportedException(String.format("%s is not supported on table %s",
            exp.getFilterExpressionType().name(), tableIdentifier.getTableName()));
      }
      return bitSetGroup;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
    readColumnChunks(rawBlockletColumnChunks);
    // CHECKSTYLE:ON

    int[] numberOfRows = null;
    int pageNumbers = 0;

    if (dimColEvaluatorInfoList.size() > 0) {
      if (isDimensionPresentInCurrentBlock[0]) {
        pageNumbers = rawBlockletColumnChunks.getDimensionRawColumnChunks()[dimensionChunkIndex[0]]
            .getPagesCount();
        numberOfRows = rawBlockletColumnChunks.getDimensionRawColumnChunks()[dimensionChunkIndex[0]]
            .getRowCount();
      } else {
        // specific for restructure case where default values need to be filled
        pageNumbers = rawBlockletColumnChunks.getDataBlock().numberOfPages();
        numberOfRows = new int[pageNumbers];
        for (int i = 0; i < pageNumbers; i++) {
          numberOfRows[i] = rawBlockletColumnChunks.getDataBlock().getPageRowCount(i);
        }
      }
    }
    if (msrColEvalutorInfoList.size() > 0) {
      if (isMeasurePresentInCurrentBlock[0]) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
        pageNumbers =
            rawBlockletColumnChunks.getMeasureRawColumnChunks()[msrColEvalutorInfoList.get(0)
                .getColumnIndex()].getPagesCount();
        numberOfRows =
            rawBlockletColumnChunks.getMeasureRawColumnChunks()[msrColEvalutorInfoList.get(0)
                .getColumnIndex()].getRowCount();
      } else {
        // specific for restructure case where default values need to be filled
        pageNumbers = rawBlockletColumnChunks.getDataBlock().numberOfPages();
        numberOfRows = new int[pageNumbers];
        for (int i = 0; i < pageNumbers; i++) {
          numberOfRows[i] = rawBlockletColumnChunks.getDataBlock().getPageRowCount(i);
        }
      }
    }
    BitSetGroup bitSetGroup = new BitSetGroup(pageNumbers);
    for (int i = 0; i < pageNumbers; i++) {
      BitSet set = new BitSet(numberOfRows[i]);
      RowIntf row = new RowImpl();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1538
      BitSet prvBitset = null;
      // if bitset pipe line is enabled then use rowid from previous bitset
      // otherwise use older flow
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
      if (!useBitsetPipeLine ||
          null == rawBlockletColumnChunks.getBitSetGroup() ||
          null == bitSetGroup.getBitSet(i) ||
          rawBlockletColumnChunks.getBitSetGroup().getBitSet(i).isEmpty()) {
        for (int index = 0; index < numberOfRows[i]; index++) {
          createRow(rawBlockletColumnChunks, row, i, index);
          Boolean rslt = false;
          try {
            rslt = exp.evaluate(row).getBoolean();
          }
          // Any invalid member while evaluation shall be ignored, system will log the
          // error only once since all rows the evaluation happens so inorder to avoid
          // too much log inforation only once the log will be printed.
          catch (FilterIllegalMemberException e) {
            FilterUtil.logError(e, false);
          }
          if (null != rslt && rslt) {
            set.set(index);
          }
        }
      } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
        prvBitset = rawBlockletColumnChunks.getBitSetGroup().getBitSet(i);
        for (int index = prvBitset.nextSetBit(0);
             index >= 0; index = prvBitset.nextSetBit(index + 1)) {
          createRow(rawBlockletColumnChunks, row, i, index);
          Boolean rslt = false;
          try {
            rslt = exp.evaluate(row).getBoolean();
          } catch (FilterIllegalMemberException e) {
            FilterUtil.logError(e, false);
          }
          if (null != rslt && rslt) {
            set.set(index);
          }
        }
      }
      bitSetGroup.setBitSet(set, i);
    }
    return bitSetGroup;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3013
    readColumnChunks(rawBlockletColumnChunks);
    int pages = rawBlockletColumnChunks.getDataBlock().numberOfPages();
    BitSet bitSet = new BitSet();
    bitSet.set(0, pages);
    return bitSet;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      throws FilterUnsupportedException {
    try {
      Boolean result = exp.evaluate(convertRow(value, dimOrdinalMax)).getBoolean();
      return result == null ? false : result;
    } catch (FilterIllegalMemberException e) {
      throw new FilterUnsupportedException(e);
    }
  }

  /**
   * convert encoded row to actual value row for filter to evaluate expression
   * @param value this row will be converted to actual value
   * @param dimOrdinalMax for measure column, its index in row = dimOrdinalMax + its ordinal
   * @return actual value row
   */
  private RowIntf convertRow(RowIntf value, int dimOrdinalMax) {
    Object[] record = new Object[value.size()];
    String memberString;
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
      int index = dimColumnEvaluatorInfo.getDimension().getOrdinal();
      // if filter dimension is not present in the current add its default value
      if (!isDimensionPresentInCurrentBlock[i]) {
        // fill default value here
        record[index] = getDimensionDefaultValue(dimColumnEvaluatorInfo);
        // already set value, so continue to set next dimension
        continue;
      }
      if (!dimColumnEvaluatorInfo.getDimension().getDataType().isComplexType()) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[index] = dimColumnEvaluatorInfo.getDimension().getDefaultValue();
        }
        byte[] memberBytes = (byte[]) value.getVal(index);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
        if (null != memberBytes) {
          if (Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, memberBytes)) {
            memberBytes = null;
          } else if (memberBytes.length == 0) {
            memberBytes = null;
          }
          record[index] = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(memberBytes,
              dimColumnEvaluatorInfo.getDimension().getDataType());
        }
      } else {
        // complex
        record[index] = value.getVal(index);
      }
    }

    for (int i = 0; i < msrColEvalutorInfoList.size(); i++) {
      MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo = msrColEvalutorInfoList.get(i);
      int index = msrColumnEvalutorInfo.getMeasure().getOrdinal() + dimOrdinalMax;
      // add default value for the measure in case filter measure is not present
      // in the current block measure list
      if (!isMeasurePresentInCurrentBlock[i]) {
        byte[] defaultValue = msrColumnEvalutorInfo.getCarbonColumn().getDefaultValue();
        record[index] = RestructureUtil.getMeasureDefaultValue(
            msrColumnEvalutorInfo.getCarbonColumn().getColumnSchema(), defaultValue);
        // already set value, so continue to set next measure
        continue;
      }
      // measure
      record[index] = value.getVal(index);
    }
    RowIntf row = new RowImpl();
    row.setValues(record);
    return row;
  }

  /**
   * Method will read the members of particular dimension block and create
   * a row instance for further processing of the filters
   *
   * @param blockChunkHolder
   * @param row
   * @param index
   */
  private void createRow(RawBlockletColumnChunks blockChunkHolder, RowIntf row, int pageIndex,
      int index) {
    Object[] record = new Object[dimColEvaluatorInfoList.size() + msrColEvalutorInfoList.size()];
    String memberString;
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
      // if filter dimension is not present in the current add its default value
      if (!isDimensionPresentInCurrentBlock[i]) {
        // fill default value here
        record[dimColumnEvaluatorInfo.getRowIndex()] =
            getDimensionDefaultValue(dimColumnEvaluatorInfo);
        continue;
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1662
      if (!dimColumnEvaluatorInfo.getDimension().getDataType().isComplexType()) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] =
              dimColumnEvaluatorInfo.getDimension().getDefaultValue();
        }
        DimensionColumnPage columnDataChunk =
            blockChunkHolder.getDimensionRawColumnChunks()[dimensionChunkIndex[i]]
                .decodeColumnPage(pageIndex);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
        if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataTypes.DATE &&
            (columnDataChunk instanceof VariableLengthDimensionColumnPage ||
                columnDataChunk instanceof ColumnPageWrapper)) {

          byte[] memberBytes = columnDataChunk.getChunkData(index);
          if (null != memberBytes) {
            if (Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, memberBytes)) {
              memberBytes = null;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1450
            } else if (memberBytes.length == 0) {
              memberBytes = null;
            }
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
                .getDataBasedOnDataTypeForNoDictionaryColumn(memberBytes,
                    dimColumnEvaluatorInfo.getDimension().getDataType());
          }
        }
      } else {
        try {
          GenericQueryType complexType = complexDimensionInfoMap.get(dimensionChunkIndex[i]);
          ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
          complexType.parseBlocksAndReturnComplexColumnByteArray(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3145
              blockChunkHolder.getDimensionRawColumnChunks(), null, index, pageIndex,
              dataOutputStream);
          record[dimColumnEvaluatorInfo.getRowIndex()] =
              complexType.getDataBasedOnDataType(ByteBuffer.wrap(byteStream.toByteArray()));
          byteStream.close();
        } catch (IOException e) {
          LOGGER.info(e.getMessage());
        }
      }
    }

    DataType msrType;
    for (int i = 0; i < msrColEvalutorInfoList.size(); i++) {
      MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo = msrColEvalutorInfoList.get(i);
      DataType dataType = msrColumnEvalutorInfo.getType();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1444
      if (dataType == DataTypes.BOOLEAN) {
        msrType = DataTypes.BOOLEAN;
      } else if (dataType == DataTypes.SHORT) {
        msrType = DataTypes.SHORT;
      } else if (dataType == DataTypes.INT) {
        msrType = DataTypes.INT;
      } else if (dataType == DataTypes.LONG) {
        msrType = DataTypes.LONG;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      } else if (DataTypes.isDecimal(dataType)) {
        msrType = DataTypes.createDefaultDecimalType();
      } else {
        msrType = DataTypes.DOUBLE;
      }
      // add default value for the measure in case filter measure is not present
      // in the current block measure list
      if (!isMeasurePresentInCurrentBlock[i]) {
        byte[] defaultValue = msrColumnEvalutorInfo.getCarbonColumn().getDefaultValue();
        record[msrColumnEvalutorInfo.getRowIndex()] = RestructureUtil
            .getMeasureDefaultValue(msrColumnEvalutorInfo.getCarbonColumn().getColumnSchema(),
                defaultValue);
        continue;
      }

      Object msrValue;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1371
      ColumnPage columnPage =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
          blockChunkHolder.getMeasureRawColumnChunks()[msrColEvalutorInfoList.get(0)
              .getColumnIndex()].decodeColumnPage(pageIndex);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1444
      if (msrType == DataTypes.BOOLEAN) {
        msrValue = columnPage.getBoolean(index);
      } else if (msrType == DataTypes.SHORT) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
        msrValue = (short) columnPage.getLong(index);
      } else if (msrType == DataTypes.INT) {
        msrValue = (int) columnPage.getLong(index);
      } else if (msrType == DataTypes.LONG) {
        msrValue = columnPage.getLong(index);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      } else if (DataTypes.isDecimal(msrType)) {
        BigDecimal bigDecimalValue = columnPage.getDecimal(index);
        if (null != bigDecimalValue
            && msrColumnEvalutorInfo.getCarbonColumn().getColumnSchema().getScale()
            > bigDecimalValue.scale()) {
          bigDecimalValue = bigDecimalValue
              .setScale(msrColumnEvalutorInfo.getCarbonColumn().getColumnSchema().getScale(),
                  RoundingMode.HALF_UP);
        }
        msrValue = bigDecimalValue;
      } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1371
        msrValue = columnPage.getDouble(index);
      }
      record[msrColumnEvalutorInfo.getRowIndex()] =
          columnPage.getNullBits().get(index) ? null : msrValue;
    }
    row.setValues(record);
  }

  /**
   * This method will compute the default value for a dimension
   *
   * @param dimColumnEvaluatorInfo
   * @return
   */
  private Object getDimensionDefaultValue(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) {
    CarbonDimension dimension = dimColumnEvaluatorInfo.getDimension();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
    return RestructureUtil.validateAndGetDefaultValue(dimension);
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1662
      if (!dimColumnEvaluatorInfo.getDimension().getDataType().isComplexType()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3572
        if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[dimensionChunkIndex[i]]) {
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[dimensionChunkIndex[i]] =
              rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                  rawBlockletColumnChunks.getFileReader(), dimensionChunkIndex[i]);
        }
      } else {
        GenericQueryType complexType = complexDimensionInfoMap.get(dimensionChunkIndex[i]);
        complexType.fillRequiredBlockData(rawBlockletColumnChunks);
      }
    }

    for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
      int chunkIndex = msrColEvalutorInfoList.get(0).getColumnIndex();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock()
                .readMeasureChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
    }
  }
}
