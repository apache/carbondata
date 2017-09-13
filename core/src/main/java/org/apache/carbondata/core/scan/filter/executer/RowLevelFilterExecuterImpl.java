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
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

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
  protected int[] dimensionBlocksIndex;

  /**
   * it has index at which given measure is stored in file
   */
  protected int[] measureBlocksIndex;

  private Map<Integer, GenericQueryType> complexDimensionInfoMap;

  /**
   * flag to check whether the filter dimension is present in current block list of dimensions.
   * Applicable for restructure scenarios
   */
  protected boolean[] isDimensionPresentInCurrentBlock;

  /**
   * flag to check whether the filter measure is present in current block list of measures.
   * Applicable for restructure scenarios
   */
  protected boolean[] isMeasurePresentInCurrentBlock;

  /**
   * is dimension column data is natural sorted
   */
  protected boolean isNaturalSorted;

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
      this.dimensionBlocksIndex = new int[dimColEvaluatorInfoList.size()];
    } else {
      this.isDimensionPresentInCurrentBlock = new boolean[]{false};
      this.dimensionBlocksIndex = new int[]{0};
    }
    if (null == msrColEvalutorInfoList) {
      this.msrColEvalutorInfoList = new ArrayList<MeasureColumnResolvedFilterInfo>(20);
    } else {
      this.msrColEvalutorInfoList = msrColEvalutorInfoList;
    }
    if (this.msrColEvalutorInfoList.size() > 0) {
      this.isMeasurePresentInCurrentBlock = new boolean[msrColEvalutorInfoList.size()];
      this.measureBlocksIndex = new int[msrColEvalutorInfoList.size()];
    } else {
      this.isMeasurePresentInCurrentBlock = new boolean[]{false};
      this.measureBlocksIndex = new int[] {0};
    }
    this.exp = exp;
    this.tableIdentifier = tableIdentifier;
    this.complexDimensionInfoMap = complexDimensionInfoMap;
    initDimensionBlockIndexes();
    initMeasureBlockIndexes();
  }

  /**
   * This method will initialize the dimension info for the current block to be
   * used for filtering the data
   */
  private void initDimensionBlockIndexes() {
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      // find the dimension in the current block dimensions list
      CarbonDimension dimensionFromCurrentBlock = segmentProperties
          .getDimensionFromCurrentBlock(dimColEvaluatorInfoList.get(i).getDimension());
      if (null != dimensionFromCurrentBlock) {
        dimColEvaluatorInfoList.get(i).setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
        this.dimensionBlocksIndex[i] = segmentProperties.getDimensionOrdinalToBlockMapping()
            .get(dimensionFromCurrentBlock.getOrdinal());
        isDimensionPresentInCurrentBlock[i] = true;
      }
    }
  }

  /**
   * This method will initialize the measure info for the current block to be
   * used for filtering the data
   */
  private void initMeasureBlockIndexes() {
    for (int i = 0; i < msrColEvalutorInfoList.size(); i++) {
      // find the measure in the current block measures list
      CarbonMeasure measureFromCurrentBlock = segmentProperties.getMeasureFromCurrentBlock(
          msrColEvalutorInfoList.get(i).getCarbonColumn().getColumnId());
      if (null != measureFromCurrentBlock) {
        msrColEvalutorInfoList.get(i).setColumnIndex(measureFromCurrentBlock.getOrdinal());
        this.measureBlocksIndex[i] = segmentProperties.getMeasuresOrdinalToBlockMapping()
            .get(measureFromCurrentBlock.getOrdinal());
        isMeasurePresentInCurrentBlock[i] = true;
      }
    }
  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    readBlocks(blockChunkHolder);
    // CHECKSTYLE:ON

    int[] numberOfRows = null;
    int pageNumbers = 0;

    if (dimColEvaluatorInfoList.size() > 0) {
      if (isDimensionPresentInCurrentBlock[0]) {
        pageNumbers =
            blockChunkHolder.getDimensionRawDataChunk()[dimensionBlocksIndex[0]].getPagesCount();
        numberOfRows =
            blockChunkHolder.getDimensionRawDataChunk()[dimensionBlocksIndex[0]].getRowCount();
      } else {
        // specific for restructure case where default values need to be filled
        pageNumbers = blockChunkHolder.getDataBlock().numberOfPages();
        numberOfRows = new int[] { blockChunkHolder.getDataBlock().nodeSize() };
      }
    }
    if (msrColEvalutorInfoList.size() > 0) {
      if (isMeasurePresentInCurrentBlock[0]) {
        pageNumbers =
            blockChunkHolder.getMeasureRawDataChunk()[measureBlocksIndex[0]].getPagesCount();
        numberOfRows =
            blockChunkHolder.getMeasureRawDataChunk()[measureBlocksIndex[0]].getRowCount();
      } else {
        // specific for restructure case where default values need to be filled
        pageNumbers = blockChunkHolder.getDataBlock().numberOfPages();
        numberOfRows = new int[] { blockChunkHolder.getDataBlock().nodeSize() };
      }
    }
    BitSetGroup bitSetGroup = new BitSetGroup(pageNumbers);
    for (int i = 0; i < pageNumbers; i++) {
      BitSet set = new BitSet(numberOfRows[i]);
      RowIntf row = new RowImpl();
      for (int index = 0; index < numberOfRows[i]; index++) {
        createRow(blockChunkHolder, row ,i, index);
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
      bitSetGroup.setBitSet(set, i);
    }
    return bitSetGroup;
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
  private void createRow(BlocksChunkHolder blockChunkHolder, RowIntf row, int pageIndex, int index)
      throws IOException {
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
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] =
              dimColumnEvaluatorInfo.getDimension().getDefaultValue();
        }
        DimensionColumnDataChunk columnDataChunk =
            blockChunkHolder.getDimensionRawDataChunk()[dimensionBlocksIndex[i]]
                .convertToDimColDataChunk(pageIndex);
        if (!dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)) {
          byte[] memberBytes = columnDataChunk.getChunkData(index);
          if (null != memberBytes) {
            if (Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, memberBytes)) {
              memberBytes = null;
            }
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
                .getDataBasedOnDataTypeForNoDictionaryColumn(memberBytes,
                    dimColumnEvaluatorInfo.getDimension().getDataType());
          }
        } else {
          int dictionaryValue = readSurrogatesFromColumnBlock(blockChunkHolder, index, pageIndex,
              dimColumnEvaluatorInfo, dimensionBlocksIndex[i]);
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
          GenericQueryType complexType = complexDimensionInfoMap.get(dimensionBlocksIndex[i]);
          ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
          complexType.parseBlocksAndReturnComplexColumnByteArray(
              blockChunkHolder.getDimensionRawDataChunk(), index, pageIndex, dataOutputStream);
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
      switch (msrColumnEvalutorInfo.getType()) {
        case SHORT:
          msrType = DataType.SHORT;
          break;
        case INT:
          msrType = DataType.INT;
          break;
        case LONG:
          msrType = DataType.LONG;
          break;
        case DECIMAL:
          msrType = DataType.DECIMAL;
          break;
        default:
          msrType = DataType.DOUBLE;
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
          blockChunkHolder.getMeasureRawDataChunk()[measureBlocksIndex[0]]
              .convertToColumnPage(pageIndex);
      switch (msrType) {
        case SHORT:
          msrValue = (short) columnPage.getLong(index);
          break;
        case INT:
          msrValue = (int) columnPage.getLong(index);
          break;
        case LONG:
          msrValue = columnPage.getLong(index);
          break;
        case DECIMAL:
          BigDecimal bigDecimalValue = columnPage.getDecimal(index);
          if (null != bigDecimalValue &&
              msrColumnEvalutorInfo.getCarbonColumn().getColumnSchema().getScale() >
                  bigDecimalValue.scale()) {
            bigDecimalValue =
                bigDecimalValue.setScale(
                    msrColumnEvalutorInfo.getCarbonColumn().getColumnSchema().getScale(),
                    RoundingMode.HALF_UP);
          }
          msrValue = bigDecimalValue;
          break;
        default:
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
  private int readSurrogatesFromColumnBlock(BlocksChunkHolder blockChunkHolder, int index, int page,
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo, int blockIndex) {
    DimensionColumnDataChunk dataChunk =
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex].convertToDimColDataChunk(page);
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
  private int readSurrogatesFromColumnGroupBlock(DimensionColumnDataChunk chunk, int index,
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) {
    try {
      KeyStructureInfo keyStructureInfo =
          QueryUtil.getKeyStructureInfo(segmentProperties, dimColumnEvaluatorInfo);
      byte[] colData = chunk.getChunkData(index);
      long[] result = keyStructureInfo.getKeyGenerator().getKeyArray(colData);
      int colGroupId =
          QueryUtil.getColumnGroupId(segmentProperties, dimensionBlocksIndex[0]);
      return (int) result[segmentProperties
          .getColumnGroupMdKeyOrdinal(colGroupId, dimensionBlocksIndex[0])];
    } catch (KeyGenException e) {
      LOGGER.error(e);
    }
    return 0;
  }


  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    for (int i = 0; i < dimColEvaluatorInfoList.size(); i++) {
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = dimColEvaluatorInfoList.get(i);
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (null == blockChunkHolder.getDimensionRawDataChunk()[dimensionBlocksIndex[i]]) {
          blockChunkHolder.getDimensionRawDataChunk()[dimensionBlocksIndex[i]] =
              blockChunkHolder.getDataBlock()
                  .getDimensionChunk(blockChunkHolder.getFileReader(), dimensionBlocksIndex[i]);
        }
      } else {
        GenericQueryType complexType = complexDimensionInfoMap.get(dimensionBlocksIndex[i]);
        complexType.fillRequiredBlockData(blockChunkHolder);
      }
    }

    if (null != msrColEvalutorInfoList) {
      for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
        if (null == blockChunkHolder.getMeasureRawDataChunk()[measureBlocksIndex[0]]) {
          blockChunkHolder.getMeasureRawDataChunk()[measureBlocksIndex[0]] =
              blockChunkHolder.getDataBlock()
                  .getMeasureChunk(blockChunkHolder.getFileReader(), measureBlocksIndex[0]);
        }
      }
    }
  }
}
