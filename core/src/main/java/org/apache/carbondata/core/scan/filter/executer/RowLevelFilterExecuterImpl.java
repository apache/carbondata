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
import java.nio.ByteBuffer;
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
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
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

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    readBlocks(blockChunkHolder);
    // CHECKSTYLE:ON

    int[] numberOfRows = null;
    int pageNumbers = 0;

    if (dimColEvaluatorInfoList.size() > 0) {
      pageNumbers = blockChunkHolder.getDimensionRawDataChunk()[blocksIndex[0]].getPagesCount();
      numberOfRows = blockChunkHolder.getDimensionRawDataChunk()[blocksIndex[0]].getRowCount();
    }
    if (msrColEvalutorInfoList.size() > 0) {
      int columnIndex = msrColEvalutorInfoList.get(0).getColumnIndex();
      pageNumbers = blockChunkHolder.getMeasureRawDataChunk()[columnIndex].getPagesCount();
      numberOfRows = blockChunkHolder.getMeasureRawDataChunk()[columnIndex].getRowCount();
    }
    BitSetGroup bitSetGroup = new BitSetGroup(pageNumbers);
    for (int i = 0; i < pageNumbers; i++) {
      BitSet set = new BitSet(numberOfRows[i]);
      RowIntf row = new RowImpl();
      boolean invalidRowsPresent = false;
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
          FilterUtil.logError(e, invalidRowsPresent);
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
      if (dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.ARRAY
          && dimColumnEvaluatorInfo.getDimension().getDataType() != DataType.STRUCT) {
        if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
          record[dimColumnEvaluatorInfo.getRowIndex()] = dimColumnEvaluatorInfo.getDefaultValue();
        }
        DimensionColumnDataChunk columnDataChunk =
            blockChunkHolder.getDimensionRawDataChunk()[blocksIndex[i]]
                .convertToDimColDataChunk(pageIndex);
        if (!dimColumnEvaluatorInfo.getDimension().hasEncoding(Encoding.DICTIONARY)
            && columnDataChunk instanceof VariableLengthDimensionDataChunk) {

          VariableLengthDimensionDataChunk dimensionColumnDataChunk =
              (VariableLengthDimensionDataChunk) columnDataChunk;
          byte[] memberBytes = dimensionColumnDataChunk.getChunkData(index);
          if (null != memberBytes) {
            if (Arrays.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, memberBytes)) {
              memberBytes = null;
            }
            record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeUtil
                .getDataBasedOnDataType(memberBytes,
                    dimColumnEvaluatorInfo.getDimension().getDataType());
          } else {
            continue;
          }
        } else {
          int dictionaryValue = readSurrogatesFromColumnBlock(blockChunkHolder, index, pageIndex,
              dimColumnEvaluatorInfo, blocksIndex[i]);
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
          GenericQueryType complexType = complexDimensionInfoMap.get(blocksIndex[i]);
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

    for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
      switch (msrColumnEvalutorInfo.getType()) {
        case INT:
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
      Object msrValue;
      MeasureColumnDataChunk measureColumnDataChunk =
          blockChunkHolder.getMeasureRawDataChunk()[msrColumnEvalutorInfo.getColumnIndex()]
              .convertToMeasureColDataChunk(pageIndex);
      switch (msrType) {
        case INT:
        case LONG:
          msrValue =
              measureColumnDataChunk.getMeasureDataHolder().getReadableLongValueByIndex(index);
          break;
        case DECIMAL:
          msrValue = measureColumnDataChunk.getMeasureDataHolder()
              .getReadableBigDecimalValueByIndex(index);
          break;
        default:
          msrValue =
              measureColumnDataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index);
      }
      record[msrColumnEvalutorInfo.getRowIndex()] =
          measureColumnDataChunk.getNullValueIndexHolder().getBitSet().get(index) ? null : msrValue;
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
      int dictionaryValue = CarbonUtil.getSurrogateKey(rawData, byteBuffer);
      return dictionaryValue;
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
          QueryUtil.getColumnGroupId(segmentProperties, dimColumnEvaluatorInfo.getColumnIndex());
      int dictionaryValue = (int) result[segmentProperties
          .getColumnGroupMdKeyOrdinal(colGroupId, dimColumnEvaluatorInfo.getColumnIndex())];
      return dictionaryValue;
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
        if (null == blockChunkHolder.getDimensionRawDataChunk()[blocksIndex[i]]) {
          blockChunkHolder.getDimensionRawDataChunk()[blocksIndex[i]] =
              blockChunkHolder.getDataBlock()
                  .getDimensionChunk(blockChunkHolder.getFileReader(), blocksIndex[i]);
        }
      } else {
        GenericQueryType complexType = complexDimensionInfoMap.get(blocksIndex[i]);
        complexType.fillRequiredBlockData(blockChunkHolder);
      }
    }

    if (null != msrColEvalutorInfoList) {
      for (MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
        if (null == blockChunkHolder.getMeasureRawDataChunk()[msrColumnEvalutorInfo
            .getColumnIndex()]) {
          blockChunkHolder.getMeasureRawDataChunk()[msrColumnEvalutorInfo.getColumnIndex()] =
              blockChunkHolder.getDataBlock().getMeasureChunk(blockChunkHolder.getFileReader(),
                  msrColumnEvalutorInfo.getColumnIndex());
        }
      }
    }
  }
}
