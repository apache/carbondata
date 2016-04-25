/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.evaluators.conditional.row;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.evaluators.AbstractConditionalEvalutor;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.DimColumnEvaluatorInfo;
import org.carbondata.query.evaluators.FilterProcessorPlaceHolder;
import org.carbondata.query.evaluators.MsrColumnEvalutorInfo;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.QueryExecutorUtility;

@Deprecated public class RowLevelFilterEvalutor extends AbstractConditionalEvalutor {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowLevelFilterEvalutor.class.getName());

  public RowLevelFilterEvalutor(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter) {
    super(exp, isExpressionResolve, isIncludeFilter);
  }

  @Override public void resolve(FilterEvaluatorInfo info) {
    DimColumnEvaluatorInfo dimColumnEvaluatorInfo = null;
    MsrColumnEvalutorInfo msrColumnEvalutorInfo = null;
    int index = 0;
    if (exp instanceof ConditionalExpression) {
      ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      for (ColumnExpression columnExpression : columnList) {
        if (columnExpression.isDimension()) {
          dimColumnEvaluatorInfo = new DimColumnEvaluatorInfo();
          dimColumnEvaluatorInfo.setColumnIndex(
              getColumnStoreIndex(columnExpression.getDim().getOrdinal(),
                  info.getHybridStoreModel()));
          dimColumnEvaluatorInfo.setNeedCompressedData(false);
          dimColumnEvaluatorInfo.setRowIndex(index++);
          dimColumnEvaluatorInfo.setSlices(info.getSlices());
          dimColumnEvaluatorInfo.setCurrentSliceIndex(info.getCurrentSliceIndex());
          dimColumnEvaluatorInfo.setDims(columnExpression.getDim());
          dimColumnEvaluatorInfo
              .setComplexTypesWithBlockStartIndex(info.getComplexTypesWithBlockStartIndex());
          dimColumnEvaluatorInfo.setDimensions(info.getDimensions());
          int newDimensionIndex = QueryExecutorUtility
              .isNewDimension(info.getNewDimension(), columnExpression.getDim());
          if (newDimensionIndex > -1) {
            dimColumnEvaluatorInfo.setDimensionExistsInCurrentSilce(false);
            dimColumnEvaluatorInfo
                .setRsSurrogates(info.getNewDimensionSurrogates()[newDimensionIndex]);
            dimColumnEvaluatorInfo.setDefaultValue(
                info.getNewDimensionDefaultValue()[newDimensionIndex]
                    .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL) ?
                    null :
                    info.getNewDimensionDefaultValue()[newDimensionIndex]);
          }

          dimColEvaluatorInfoList.add(dimColumnEvaluatorInfo);
        } else {
          msrColumnEvalutorInfo = new MsrColumnEvalutorInfo();
          msrColumnEvalutorInfo.setRowIndex(index++);
          msrColumnEvalutorInfo.setAggregator(((Measure) columnExpression.getDim()).getAggName());
          int measureIndex = QueryExecutorUtility
              .isNewMeasure(info.getNewMeasures(), ((Measure) columnExpression.getDim()));
          // if measure is found then index returned will be > 0 .
          // else it will be -1 . here if the measure is a newly added
          // measure then index will be >0.
          if (measureIndex < 0) {
            msrColumnEvalutorInfo
                .setColumnIndex(((Measure) columnExpression.getDim()).getOrdinal());
            msrColumnEvalutorInfo.setUniqueValue(info.getSlices().get(info.getCurrentSliceIndex())
                .getDataCache(info.getFactTableName()).getUniqueValue()[((Measure) columnExpression
                .getDim()).getOrdinal()]);
            msrColumnEvalutorInfo.setCustomMeasureValue(
                info.getSlices().get(info.getCurrentSliceIndex())
                    .getDataCache(info.getFactTableName()).getType()[((Measure) columnExpression
                    .getDim()).getOrdinal()] == 'c' ? true : false);
            //  msrColumnEvalutorInfo.setType(info.getSlices().get(info.getCurrentSliceIndex())
            //  .getDataCache(info.getFactTableName()).getType()[((Measure) columnExpression
            //  .getDim()).getOrdinal()]);
          } else {
            msrColumnEvalutorInfo.setMeasureExistsInCurrentSlice(false);
            msrColumnEvalutorInfo.setDefaultValue(info.getNewDefaultValues()[measureIndex]);
          }
          msrColEvalutorInfoList.add(msrColumnEvalutorInfo);
        }
      }
    }
  }

  @Override
  public BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder,
      int[] noDictionaryColIndexes) {
    for (DimColumnEvaluatorInfo dimColumnEvaluatorInfo : dimColEvaluatorInfoList) {
      if (dimColumnEvaluatorInfo.getDims().getDataType() != Type.ARRAY
          && dimColumnEvaluatorInfo.getDims().getDataType() != Type.STRUCT) {
        if (null == blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo
            .getColumnIndex()]) {
          blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo.getColumnIndex()] =
              blockDataHolder.getLeafDataBlock()
                  .getColumnarKeyStore(blockDataHolder.getFileHolder(),
                      dimColumnEvaluatorInfo.getColumnIndex(), false, noDictionaryColIndexes);
        } else {
          if (!blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo.getColumnIndex()]
              .getColumnarKeyStoreMetadata().isUnCompressed()) {
            blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo.getColumnIndex()]
                .unCompress();
          }
        }
      } else {
        GenericQueryType complexType = dimColumnEvaluatorInfo.getComplexTypesWithBlockStartIndex()
            .get(dimColumnEvaluatorInfo.getColumnIndex());
        complexType.fillRequiredBlockData(blockDataHolder);
      }
    }

    //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
    for (MsrColumnEvalutorInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
      if (msrColumnEvalutorInfo.isMeasureExistsInCurrentSlice() && null == blockDataHolder
          .getMeasureBlocks()[msrColumnEvalutorInfo.getColumnIndex()]) {
        blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo.getColumnIndex()] =
            blockDataHolder.getLeafDataBlock()
                .getNodeMsrDataWrapper(msrColumnEvalutorInfo.getColumnIndex(),
                    blockDataHolder.getFileHolder()).getValues()[msrColumnEvalutorInfo
                .getColumnIndex()];
      }
    }
    //CHECKSTYLE:ON

    int numberOfRows = blockDataHolder.getLeafDataBlock().getnKeys();
    BitSet set = new BitSet(numberOfRows);
    RowIntf row = new RowImpl();

    //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_007
    for (int index = 0; index < numberOfRows; index++) {
      createRow(blockDataHolder, row, index);
      try {
        Boolean rslt = exp.evaluate(row).getBoolean();
        if (null != rslt && rslt) {
          set.set(index);
        }
      } catch (FilterUnsupportedException e) {
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e, e.getMessage());
      }
    }
    //CHECKSTYLE:ON

    return set;
  }

  private void createRow(BlockDataHolder blockDataHolder, RowIntf row, int index) {
    Object[] record = new Object[dimColEvaluatorInfoList.size() + msrColEvalutorInfoList.size()];
    String memberString = null;
    for (DimColumnEvaluatorInfo dimColumnEvaluatorInfo : dimColEvaluatorInfoList) {
      if (dimColumnEvaluatorInfo.getDims().getDataType() != Type.ARRAY
          && dimColumnEvaluatorInfo.getDims().getDataType() != Type.STRUCT) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] = dimColumnEvaluatorInfo.getDefaultValue();
        }
        if (dimColumnEvaluatorInfo.getDims().isNoDictionaryDim()) {
          ColumnarKeyStoreDataHolder columnarKeyStoreDataHolder =
              blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo.getColumnIndex()];
          if (null != columnarKeyStoreDataHolder.getNoDictionaryValBasedKeyBlockData()) {
            Member member =
                readMemberBasedOnNoDictionaryVal(dimColumnEvaluatorInfo, columnarKeyStoreDataHolder,
                    index);
            if (null != member) {
              memberString = member.toString();
              if (memberString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                memberString = null;
              }
            }
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeConverter
                .getDataBasedOnDataType(memberString,
                    dimColumnEvaluatorInfo.getDims().getDataType());
          } else {
            continue;
          }
        } else {
          Member member = QueryExecutorUtility
              .getMemberBySurrogateKey(dimColumnEvaluatorInfo.getDims(),
                  blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo.getColumnIndex()]
                      .getSurrogateKey(index), dimColumnEvaluatorInfo.getSlices(),
                  dimColumnEvaluatorInfo.getCurrentSliceIndex());

          if (null != member) {
            memberString = member.toString();
            if (memberString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
              memberString = null;
            }
          }
          record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeConverter
              .getDataBasedOnDataType(memberString, dimColumnEvaluatorInfo.getDims().getDataType());
        }
      } else {
        try {
          GenericQueryType complexType = dimColumnEvaluatorInfo.getComplexTypesWithBlockStartIndex()
              .get(dimColumnEvaluatorInfo.getColumnIndex());
          ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
          complexType
              .parseBlocksAndReturnComplexColumnByteArray(blockDataHolder.getColumnarKeyStore(),
                  index, dataOutputStream);
          record[dimColumnEvaluatorInfo.getRowIndex()] = complexType
              .getDataBasedOnDataTypeFromSurrogates(dimColumnEvaluatorInfo.getSlices(),
                  ByteBuffer.wrap(byteStream.toByteArray()),
                  dimColumnEvaluatorInfo.getDimensions());
          byteStream.close();
        } catch (IOException e) {
          LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e, e.getMessage());
        }

      }
    }

    SqlStatement.Type msrType;

    row.setValues(record);
  }

  /**
   * Reading the blocks for direct surrogates.
   *
   * @param dimColumnEvaluatorInfo
   * @param columnarKeyStoreDataHolder
   * @param index
   * @return
   */
  private Member readMemberBasedOnNoDictionaryVal(DimColumnEvaluatorInfo dimColumnEvaluatorInfo,
      ColumnarKeyStoreDataHolder columnarKeyStoreDataHolder, int index) {
    byte[] noDictionaryVals;
    if (null != columnarKeyStoreDataHolder.getColumnarKeyStoreMetadata().getColumnReverseIndex()) {
      // Getting the data for direct surrogates.
      noDictionaryVals = columnarKeyStoreDataHolder.getNoDictionaryValBasedKeyBlockData().get(
          columnarKeyStoreDataHolder.getColumnarKeyStoreMetadata().getColumnReverseIndex()[index]);
    } else {
      noDictionaryVals =
          columnarKeyStoreDataHolder.getNoDictionaryValBasedKeyBlockData().get(index);
    }
    Member member = new Member(noDictionaryVals);
    return member;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }

}
