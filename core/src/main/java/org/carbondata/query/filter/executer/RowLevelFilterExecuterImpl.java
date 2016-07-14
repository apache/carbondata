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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.MeasureAggregatorFactory;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.exception.FilterIllegalMemberException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

public class RowLevelFilterExecuterImpl implements FilterExecuter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowLevelFilterExecuterImpl.class.getName());
  protected List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList;
  protected List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList;
  protected Expression exp;
  protected AbsoluteTableIdentifier tableIdentifier;
  protected SegmentProperties segmentProperties;
  /**
   * it has index at which given dimension is stored in file
   */
  private int[] blocksIndex;

  private Map<Integer, GenericQueryType> complexDimensionInfoMap;

  public RowLevelFilterExecuterImpl(List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap) {
    this.dimColEvaluatorInfoList = dimColEvaluatorInfoList;
    this.segmentProperties = segmentProperties;
    this.blocksIndex = new int[dimColEvaluatorInfoList.size()];
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      this.blocksIndex[i] = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimColEvaluatorInfoList.get(i).getColumnIndex());
    }
    if (null == msrColEvalutorInfoList) {
      this.msrColEvalutorInfoList = new ArrayList<MeasureColumnResolvedFilterInfo>(20);
    } else {
      this.msrColEvalutorInfoList = msrColEvalutorInfoList;
    }
    this.exp = exp;
    this.tableIdentifier = tableIdentifier;
    this.complexDimensionInfoMap = complexDimensionInfoMap;
  }

  @Override public BitSet applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException {
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (null == blockChunkHolder.getDimensionDataChunk()[blocksIndex[i]]) {
          blockChunkHolder.getDimensionDataChunk()[blocksIndex[i]] = blockChunkHolder.getDataBlock()
              .getDimensionChunk(blockChunkHolder.getFileReader(), blocksIndex[i]);
        }
      } else {
        GenericQueryType complexType = complexDimensionInfoMap.get(blocksIndex[i]);
        complexType.fillRequiredBlockData(blockChunkHolder);
      }
    }

    // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
    if (null != msrColEvalutorInfoList) {
      for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
        if (msrColumnEvalutorInfo.isMeasureExistsInCurrentSlice() && null == blockChunkHolder
            .getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]) {
          blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()] =
              blockChunkHolder.getDataBlock().getMeasureChunk(blockChunkHolder.getFileReader(),
                  msrColumnEvalutorInfo.getColumnIndex());
        }
      }
    }
    // CHECKSTYLE:ON

    int numberOfRows = blockChunkHolder.getDataBlock().nodeSize();
    BitSet set = new BitSet(numberOfRows);
    RowIntf row = new RowImpl();
    boolean invalidRowsPresent = false;
    for (int index = 0; index < numberOfRows; index++) {
      try {
        createRow(blockChunkHolder, row, index);
      } catch (QueryExecutionException e) {
        FilterUtil.logError(e, invalidRowsPresent);
      }
      Boolean rslt = false;
      try {
        rslt = exp.evaluate(row).getBoolean();
      }
      // Any invalid member while evaluation shall be ignored, system will log the
      // error only once since all rows the evaluation happens so inorder to avoid
      // too much log inforation only once the log will be printed.
      catch (FilterIllegalMemberException e) {
        FilterUtil.logError(e, invalidRowsPresent);
      }
      if (null != rslt && rslt) {
        set.set(index);
      }
    }
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
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] = dimColumnEvaluatorInfo.getDefaultValue();
        }
        if (!dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)
            && blockChunkHolder
            .getDimensionDataChunk()[blocksIndex[i]] instanceof VariableLengthDimensionDataChunk) {

          VariableLengthDimensionDataChunk dimensionColumnDataChunk =
              (VariableLengthDimensionDataChunk) blockChunkHolder
                  .getDimensionDataChunk()[blocksIndex[i]];
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
          int dictionaryValue =
              readSurrogatesFromColumnBlock(blockChunkHolder, index, dimColumnEvaluatorInfo,
                  blocksIndex[i]);
          Dictionary forwardDictionary = null;
          if (dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)
              && !dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
            memberString =
                getFilterActualValueFromDictionaryValue(dimColumnEvaluatorInfo, dictionaryValue,
                    forwardDictionary);
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
                .getDataBasedOnDataType(memberString,
                    dimColumnEvaluatorInfo.getDimension().getDataType());
          } else if (dimColumnEvaluatorInfo.getDimension()
              .hasEncoding(Encoding.DIRECT_DICTIONARY)) {

            Object member = getFilterActualValueFromDirectDictionaryValue(dimColumnEvaluatorInfo,
                dictionaryValue);
            record[dimColumnEvaluatorInfo.getRowIndex()] = member;
          }
        }
      } else {
        try {
          GenericQueryType complexType = complexDimensionInfoMap.get(blocksIndex[i]);
          ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
          complexType
              .parseBlocksAndReturnComplexColumnByteArray(blockChunkHolder.getDimensionDataChunk(),
                  index, dataOutputStream);
          record[dimColumnEvaluatorInfo.getRowIndex()] = complexType
              .getDataBasedOnDataTypeFromSurrogates(ByteBuffer.wrap(byteStream.toByteArray()));
          byteStream.close();
        } catch (IOException e) {
          LOGGER.info(e.getMessage());
        }
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
          if (null != aggregator) {
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
          record[msrColumnEvalutorInfo.getRowIndex()] =
              blockChunkHolder.getMeasureDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]
                  .getNullValueIndexHolder().getBitSet().get(index) ? null : msrValue;

        }
      }
    }
    row.setValues(record);
  }

  /**
   * method will read the actual data from the direct dictionary generator
   * by passing direct dictionary value.
   *
   * @param dimColumnEvaluatorInfo
   * @param dictionaryValue
   * @return
   */
  private Object getFilterActualValueFromDirectDictionaryValue(
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int dictionaryValue) {
    Object memberString = null;
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(dimColumnEvaluatorInfo.getDimension().getDataType());
    if (null != directDictionaryGenerator) {
      memberString = directDictionaryGenerator.getValueFromSurrogate(dictionaryValue);
    }
    return memberString;
  }

  /**
   * Read the actual filter member by passing the dictionary value from
   * the forward dictionary cache which which holds column wise cache
   *
   * @param dimColumnEvaluaatorInfo
   * @param dictionaryValue
   * @param forwardDictionary
   * @return
   * @throws QueryExecutionException
   */
  private String getFilterActualValueFromDictionaryValue(
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int dictionaryValue,
      Dictionary forwardDictionary) throws QueryExecutionException {
    String memberString;
    try {
      forwardDictionary = FilterUtil
          .getForwardDictionaryCache(tableIdentifier, dimColumnEvaluatorInfo.getDimension());
    } catch (QueryExecutionException e) {
      throw new QueryExecutionException(e);
    }

    memberString = forwardDictionary.getDictionaryValueForKey(dictionaryValue);
    if (null != memberString) {
      if (memberString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        memberString = null;
      }
    }
    return memberString;
  }

  /**
   * read the filter member dictionary data from the block corresponding to
   * applied filter column
   *
   * @param blockChunkHolder
   * @param index
   * @param dimColumnEvaluatorInfo
   * @return
   */
  private int readSurrogatesFromColumnBlock(BlocksChunkHolder blockChunkHolder, int index,
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int blockIndex) {
    if (dimColumnEvaluatorInfo.getDimension().isColumnar()) {
      byte[] rawData = blockChunkHolder.getDimensionDataChunk()[blockIndex].getChunkData(index);
      ByteBuffer byteBuffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
      int dictionaryValue = CarbonUtil.getSurrogateKey(rawData, byteBuffer);
      return dictionaryValue;
    } else {
      return readSurrogatesFromColumnGroupBlock(blockChunkHolder, index, dimColumnEvaluatorInfo,
          blockIndex);
    }

  }

  /**
   * @param blockChunkHolder
   * @param index
   * @param dimColumnEvaluatorInfo
   * @return read surrogate of given row of given column group dimension
   */
  private int readSurrogatesFromColumnGroupBlock(BlocksChunkHolder blockChunkHolder, int index,
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int blockIndex) {
    try {
      KeyStructureInfo keyStructureInfo =
          QueryUtil.getKeyStructureInfo(segmentProperties, dimColumnEvaluatorInfo);
      byte[] colData = blockChunkHolder.getDimensionDataChunk()[blockIndex].getChunkData(index);
      long[] result = keyStructureInfo.getKeyGenerator().getKeyArray(colData);
      int colGroupId =
          QueryUtil.getColumnGroupId(segmentProperties, dimColumnEvaluatorInfo.getColumnIndex());
      int dictionaryValue = (int) result[segmentProperties
          .getColumnGroupMdKeyOrdinal(colGroupId, dimColumnEvaluatorInfo.getColumnIndex())];
      return dictionaryValue;
    } catch (KeyGenException e) {
      LOGGER.error(e);
    }
    return 0;
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
    return new String(noDictionaryVals, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }
}
