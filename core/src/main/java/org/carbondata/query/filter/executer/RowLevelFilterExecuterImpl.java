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
package org.carbondata.query.filter.executer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.MeasureAggregatorFactory;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.evaluators.DimColumnResolvedFilterInfo;
import org.carbondata.query.evaluators.MeasureColumnResolvedFilterInfo;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

public class RowLevelFilterExecuterImpl implements FilterExecuter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowLevelFilterExecuterImpl.class.getName());
  private List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList;
  private List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList;
  private Expression exp;
  private AbsoluteTableIdentifier tableIdentifier;

  public RowLevelFilterExecuterImpl(List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier) {
    this.dimColEvaluatorInfoList = dimColEvaluatorInfoList;
    this.msrColEvalutorInfoList = msrColEvalutorInfoList;
    this.exp = exp;
    this.tableIdentifier = tableIdentifier;
  }

  @Override public BitSet applyFilter(BlocksChunkHolder blockChunkHolder) {
    for (DimColumnResolvedFilterInfo dimColumnEvaluatorInfo : dimColEvaluatorInfoList) {
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (null == blockChunkHolder.getDimensionDataChunk()[dimColumnEvaluatorInfo
            .getColumnIndex()]) {
          blockChunkHolder.getDimensionDataChunk()[dimColumnEvaluatorInfo.getColumnIndex()] =
              blockChunkHolder.getDataBlock().getDimensionChunk(blockChunkHolder.getFileReader(),
                  dimColumnEvaluatorInfo.getColumnIndex());
        }
      } else {
        GenericQueryType complexType = dimColumnEvaluatorInfo.getComplexTypesWithBlockStartIndex()
            .get(dimColumnEvaluatorInfo.getColumnIndex());
        complexType.fillRequiredBlockData(blockChunkHolder);
      }
    }

    // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
    for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
      if (msrColumnEvalutorInfo.isMeasureExistsInCurrentSlice() && null == blockChunkHolder
          .getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]) {
        blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()] =
            blockChunkHolder.getDataBlock().getMeasureChunk(blockChunkHolder.getFileReader(),
                msrColumnEvalutorInfo.getColumnIndex());
      }
    }
    // CHECKSTYLE:ON

    int numberOfRows = blockChunkHolder.getDataBlock().nodeSize();
    BitSet set = new BitSet(numberOfRows);
    RowIntf row = new RowImpl();

    // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_007
    for (int index = 0; index < numberOfRows; index++) {
      try {
        createRow(blockChunkHolder, row, index);
      } catch (QueryExecutionException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      try {
        Boolean rslt = exp.evaluate(row).getBoolean();
        if (null != rslt && rslt) {
          set.set(index);
        }
      } catch (FilterUnsupportedException e) {
        LOGGER.error(e, e.getMessage());
      }
    }
    // CHECKSTYLE:ON

    return set;
  }

  /**
   * Method will read the members of particular dimension block and create
   * a row instance for further processing of the filters
   *
   * @param blockChunkHolder
   * @param row
   * @param index
   * @throws QueryExecutionException
   */
  private void createRow(BlocksChunkHolder blockChunkHolder, RowIntf row, int index)
      throws QueryExecutionException {
    Object[] record = new Object[dimColEvaluatorInfoList.size() + msrColEvalutorInfoList.size()];
    String memberString = null;
    for (DimColumnResolvedFilterInfo dimColumnEvaluatorInfo : dimColEvaluatorInfoList) {
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] = dimColumnEvaluatorInfo.getDefaultValue();
        }
        if (!dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)
            && blockChunkHolder.getDimensionDataChunk()[dimColumnEvaluatorInfo
            .getColumnIndex()] instanceof VariableLengthDimensionDataChunk) {

          VariableLengthDimensionDataChunk dimensionColumnDataChunk =
              (VariableLengthDimensionDataChunk) blockChunkHolder
                  .getDimensionDataChunk()[dimColumnEvaluatorInfo.getColumnIndex()];
          if (null != dimensionColumnDataChunk.getCompleteDataChunk()) {
            memberString =
                readMemberBasedOnNoDictionaryVal(dimColumnEvaluatorInfo, dimensionColumnDataChunk,
                    index);
            if (null != memberString) {
              if (memberString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                memberString = null;
              }
            }
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
                .getDataBasedOnDataType(memberString,
                    dimColumnEvaluatorInfo.getDimension().getDataType());
          } else {
            continue;
          }
        } else {
          Dictionary forwardDictionary = null;
          try {
            forwardDictionary = FilterUtil
                .getForwardDictionaryCache(tableIdentifier, dimColumnEvaluatorInfo.getDimension());
          } catch (QueryExecutionException e) {
            throw new QueryExecutionException(e);
          }
          byte[] rawData =
              blockChunkHolder.getDimensionDataChunk()[dimColumnEvaluatorInfo.getColumnIndex()]
                  .getChunkData(index);
          ByteBuffer byteBuffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
          int dictionaryValue = CarbonUtil.getSurrogateKey(rawData, byteBuffer);
          memberString = forwardDictionary.getDictionaryValueForKey(dictionaryValue);
          if (null != memberString) {
            if (memberString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
              memberString = null;
            }
          }
          record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
              .getDataBasedOnDataType(memberString,
                  dimColumnEvaluatorInfo.getDimension().getDataType());
        }
      } else {
        // TODO Handling of complex type

      }
    }

    DataType msrType;

    for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
      switch (msrColumnEvalutorInfo.getType()) {
        case LONG:
          msrType = DataType.LONG;
          break;
        case DECIMAL:
          msrType = DataType.DECIMAL;
          break;
        default:
          msrType = DataType.DOUBLE;
      }
      // if measure doesnt exist then set the default value.
      if (!msrColumnEvalutorInfo.isMeasureExistsInCurrentSlice()) {
        record[msrColumnEvalutorInfo.getRowIndex()] = msrColumnEvalutorInfo.getDefaultValue();
      } else {
        if (msrColumnEvalutorInfo.isCustomMeasureValue()) {
          MeasureAggregator aggregator = MeasureAggregatorFactory
              .getAggregator(msrColumnEvalutorInfo.getAggregator(),
                  msrColumnEvalutorInfo.getType());
          aggregator.merge(
              blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]
                  .getMeasureDataHolder().getReadableByteArrayValueByIndex(index));
          switch (msrType) {
            case LONG:
              record[msrColumnEvalutorInfo.getRowIndex()] = aggregator.getLongValue();
              break;
            case DECIMAL:
              record[msrColumnEvalutorInfo.getRowIndex()] = aggregator.getBigDecimalValue();
              break;
            default:
              record[msrColumnEvalutorInfo.getRowIndex()] = aggregator.getDoubleValue();
          }
        } else {
          Object msrValue;
          switch (msrType) {
            case LONG:
              msrValue =
                  blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]
                      .getMeasureDataHolder().getReadableLongValueByIndex(index);
              break;
            case DECIMAL:
              msrValue =
                  blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]
                      .getMeasureDataHolder().getReadableBigDecimalValueByIndex(index);
              break;
            default:
              msrValue =
                  blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]
                      .getMeasureDataHolder().getReadableDoubleValueByIndex(index);
          }
          record[msrColumnEvalutorInfo.getRowIndex()] = msrValue;

        }
      }
    }
    row.setValues(record);
  }

  /**
   * Reading the blocks for no dictionary data, in no dictionary case
   * directly the filter data will read, no need to scan the dictionary
   * or read the dictionary value.
   *
   * @param dimColumnEvaluatorInfo
   * @param dimensionColumnDataChunk
   * @param index
   * @return
   */
  private String readMemberBasedOnNoDictionaryVal(
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      VariableLengthDimensionDataChunk dimensionColumnDataChunk, int index) {
    byte[] noDictionaryVals;
    if (null != dimensionColumnDataChunk.getAttributes().getInvertedIndexesReverse()) {
      // Getting the data for direct surrogates.
      noDictionaryVals = dimensionColumnDataChunk.getCompleteDataChunk()
          .get(dimensionColumnDataChunk.getAttributes().getInvertedIndexesReverse()[index]);
    } else {
      noDictionaryVals = dimensionColumnDataChunk.getCompleteDataChunk().get(index);
    }
    return new String(noDictionaryVals);
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }
}
