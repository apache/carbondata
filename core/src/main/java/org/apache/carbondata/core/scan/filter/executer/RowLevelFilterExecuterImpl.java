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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
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
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

public class RowLevelFilterExecuterImpl implements FilterExecuter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowLevelFilterExecuterImpl.class.getName());
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
   * it has index at which given measure is stored in file
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

  /**
   * date direct dictionary generator
   */
  private DirectDictionaryGenerator dateDictionaryGenerator;

  /**
   * timestamp direct dictionary generator
   */
  private DirectDictionaryGenerator timestampDictionaryGenerator;

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
    if (this.dimColEvaluatorInfoList.size() > 0) {
      this.isDimensionPresentInCurrentBlock = new boolean[dimColEvaluatorInfoList.size()];
      this.dimensionChunkIndex = new int[dimColEvaluatorInfoList.size()];
    } else {
      this.isDimensionPresentInCurrentBlock = new boolean[]{false};
      this.dimensionChunkIndex = new int[]{0};
    }
    if (null == msrColEvalutorInfoList) {
      this.msrColEvalutorInfoList = new ArrayList<MeasureColumnResolvedFilterInfo>(20);
    } else {
      this.msrColEvalutorInfoList = msrColEvalutorInfoList;
    }
    if (this.msrColEvalutorInfoList.size() > 0) {
      this.isMeasurePresentInCurrentBlock = new boolean[msrColEvalutorInfoList.size()];
      this.measureChunkIndex = new int[msrColEvalutorInfoList.size()];
    } else {
      this.isMeasurePresentInCurrentBlock = new boolean[]{false};
      this.measureChunkIndex = new int[] {0};
    }
    this.exp = exp;
    this.tableIdentifier = tableIdentifier;
    this.complexDimensionInfoMap = complexDimensionInfoMap;
    this.dateDictionaryGenerator =
        DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(DataTypes.DATE);
    this.timestampDictionaryGenerator =
        DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(DataTypes.TIMESTAMP);
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
        this.dimensionChunkIndex[i] = segmentProperties.getDimensionOrdinalToChunkMapping()
            .get(dimensionFromCurrentBlock.getOrdinal());
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
      CarbonMeasure measureFromCurrentBlock = segmentProperties.getMeasureFromCurrentBlock(
          msrColEvalutorInfoList.get(i).getCarbonColumn().getColumnId());
      if (null != measureFromCurrentBlock) {
        msrColEvalutorInfoList.get(i).setColumnIndex(measureFromCurrentBlock.getOrdinal());
        this.measureChunkIndex[i] = segmentProperties.getMeasuresOrdinalToChunkMapping()
            .get(measureFromCurrentBlock.getOrdinal());
        isMeasurePresentInCurrentBlock[i] = true;
      }
    }
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
    if (exp instanceof MatchExpression) {
      return rawBlockletColumnChunks.getBitSetGroup();
    }
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
        numberOfRows = new int[] { rawBlockletColumnChunks.getDataBlock().numRows() };
      }
    }
    if (msrColEvalutorInfoList.size() > 0) {
      if (isMeasurePresentInCurrentBlock[0]) {
        pageNumbers = rawBlockletColumnChunks.getMeasureRawColumnChunks()[measureChunkIndex[0]]
            .getPagesCount();
        numberOfRows = rawBlockletColumnChunks.getMeasureRawColumnChunks()[measureChunkIndex[0]]
            .getRowCount();
      } else {
        // specific for restructure case where default values need to be filled
        pageNumbers = rawBlockletColumnChunks.getDataBlock().numberOfPages();
        numberOfRows = new int[] { rawBlockletColumnChunks.getDataBlock().numRows() };
      }
    }
    BitSetGroup bitSetGroup = new BitSetGroup(pageNumbers);
    for (int i = 0; i < pageNumbers; i++) {
      BitSet set = new BitSet(numberOfRows[i]);
      RowIntf row = new RowImpl();
      BitSet prvBitset = null;
      // if bitset pipe line is enabled then use rowid from previous bitset
      // otherwise use older flow
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

  @Override public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    try {
      return exp.evaluate(value).getBoolean();
    } catch (FilterIllegalMemberException e) {
      throw new FilterUnsupportedException(e);
    }
  }

  /**
   * Method will read the members of particular dimension block and create
   * a row instance for further processing of the filters
   *
   * @param blockChunkHolder
   * @param row
   * @param index
   * @throws IOException
   */
  private void createRow(RawBlockletColumnChunks blockChunkHolder, RowIntf row, int pageIndex,
      int index) throws IOException {
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
      if (!dimColumnEvaluatorInfo.getDimension().getDataType().isComplexType()) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] =
              dimColumnEvaluatorInfo.getDimension().getDefaultValue();
        }
        DimensionColumnPage columnDataChunk =
            blockChunkHolder.getDimensionRawColumnChunks()[dimensionChunkIndex[i]]
                .decodeColumnPage(pageIndex);
        if (!dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)
            && columnDataChunk instanceof VariableLengthDimensionColumnPage) {

          VariableLengthDimensionColumnPage dimensionColumnDataChunk =
              (VariableLengthDimensionColumnPage) columnDataChunk;
          byte[] memberBytes = dimensionColumnDataChunk.getChunkData(index);
          if (null != memberBytes) {
            if (Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, memberBytes)) {
              memberBytes = null;
            } else if (memberBytes.length == 0) {
              memberBytes = null;
            }
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
                .getDataBasedOnDataTypeForNoDictionaryColumn(memberBytes,
                    dimColumnEvaluatorInfo.getDimension().getDataType());
          }
        } else {
          int dictionaryValue = readSurrogatesFromColumnChunk(blockChunkHolder, index, pageIndex,
              dimColumnEvaluatorInfo, dimensionChunkIndex[i]);
          if (dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)
              && !dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
            memberString =
                getFilterActualValueFromDictionaryValue(dimColumnEvaluatorInfo, dictionaryValue);
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
          GenericQueryType complexType = complexDimensionInfoMap.get(dimensionChunkIndex[i]);
          ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
          complexType.parseBlocksAndReturnComplexColumnByteArray(
              blockChunkHolder.getDimensionRawColumnChunks(), index, pageIndex, dataOutputStream);
          record[dimColumnEvaluatorInfo.getRowIndex()] = complexType
              .getDataBasedOnDataTypeFromSurrogates(ByteBuffer.wrap(byteStream.toByteArray()));
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
      if (dataType == DataTypes.BOOLEAN) {
        msrType = DataTypes.BOOLEAN;
      } else if (dataType == DataTypes.SHORT) {
        msrType = DataTypes.SHORT;
      } else if (dataType == DataTypes.INT) {
        msrType = DataTypes.INT;
      } else if (dataType == DataTypes.LONG) {
        msrType = DataTypes.LONG;
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
      ColumnPage columnPage =
          blockChunkHolder.getMeasureRawColumnChunks()[measureChunkIndex[0]]
              .decodeColumnPage(pageIndex);
      if (msrType == DataTypes.BOOLEAN) {
        msrValue = columnPage.getBoolean(index);
      } else if (msrType == DataTypes.SHORT) {
        msrValue = (short) columnPage.getLong(index);
      } else if (msrType == DataTypes.INT) {
        msrValue = (int) columnPage.getLong(index);
      } else if (msrType == DataTypes.LONG) {
        msrValue = columnPage.getLong(index);
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
    Object dimensionDefaultValue = null;
    CarbonDimension dimension = dimColumnEvaluatorInfo.getDimension();
    if (dimension.hasEncoding(Encoding.DICTIONARY) && !dimension
        .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      byte[] defaultValue = dimension.getDefaultValue();
      if (null != defaultValue) {
        dimensionDefaultValue =
            new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      }
    } else {
      dimensionDefaultValue = RestructureUtil.validateAndGetDefaultValue(dimension);
    }
    return dimensionDefaultValue;
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
    if (dimColumnEvaluatorInfo.getDimension().getDataType() == DataTypes.DATE) {
      return dateDictionaryGenerator.getValueFromSurrogate(dictionaryValue);
    } else if (dimColumnEvaluatorInfo.getDimension().getDataType() == DataTypes.TIMESTAMP) {
      return timestampDictionaryGenerator.getValueFromSurrogate(dictionaryValue);
    } else {
      throw new RuntimeException("Invalid data type for dierct dictionary");
    }
  }

  /**
   * Read the actual filter member by passing the dictionary value from
   * the forward dictionary cache which which holds column wise cache
   *
   * @param dimColumnEvaluatorInfo
   * @param dictionaryValue
   * @return
   * @throws IOException
   */
  private String getFilterActualValueFromDictionaryValue(
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int dictionaryValue) throws IOException {
    String memberString;
    Dictionary forwardDictionary = FilterUtil
        .getForwardDictionaryCache(tableIdentifier, dimColumnEvaluatorInfo.getDimension());

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
  private int readSurrogatesFromColumnChunk(RawBlockletColumnChunks blockChunkHolder, int index,
      int page, DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int chunkIndex) {
    DimensionColumnPage dataChunk =
        blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex].decodeColumnPage(page);
    if (dimColumnEvaluatorInfo.getDimension().isColumnar()) {
      byte[] rawData = dataChunk.getChunkData(index);
      ByteBuffer byteBuffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
      return CarbonUtil.getSurrogateKey(rawData, byteBuffer);
    } else {
      return readSurrogatesFromColumnGroupBlock(dataChunk, index, dimColumnEvaluatorInfo);
    }

  }

  /**
   * @param index
   * @param dimColumnEvaluatorInfo
   * @return read surrogate of given row of given column group dimension
   */
  private int readSurrogatesFromColumnGroupBlock(DimensionColumnPage chunk, int index,
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) {
    try {
      KeyStructureInfo keyStructureInfo =
          QueryUtil.getKeyStructureInfo(segmentProperties, dimColumnEvaluatorInfo);
      byte[] colData = chunk.getChunkData(index);
      long[] result = keyStructureInfo.getKeyGenerator().getKeyArray(colData);
      int colGroupId =
          QueryUtil.getColumnGroupId(segmentProperties, dimensionChunkIndex[0]);
      return (int) result[segmentProperties
          .getColumnGroupMdKeyOrdinal(colGroupId, dimensionChunkIndex[0])];
    } catch (KeyGenException e) {
      LOGGER.error(e);
    }
    return 0;
  }


  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
      if (!dimColumnEvaluatorInfo.getDimension().getDataType().isComplexType()) {
        if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[dimensionChunkIndex[i]])
        {
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[dimensionChunkIndex[i]] =
              rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                  rawBlockletColumnChunks.getFileReader(), dimensionChunkIndex[i]);
        }
      } else {
        GenericQueryType complexType = complexDimensionInfoMap.get(dimensionChunkIndex[i]);
        complexType.fillRequiredBlockData(rawBlockletColumnChunks);
      }
    }

    if (null != msrColEvalutorInfoList) {
      for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
        if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[measureChunkIndex[0]]) {
          rawBlockletColumnChunks.getMeasureRawColumnChunks()[measureChunkIndex[0]] =
              rawBlockletColumnChunks.getDataBlock()
                  .readMeasureChunk(rawBlockletColumnChunks.getFileReader(), measureChunkIndex[0]);
        }
      }
    }
  }
}
