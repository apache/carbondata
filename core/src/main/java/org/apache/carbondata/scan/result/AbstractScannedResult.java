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
package org.apache.carbondata.scan.result;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.carbondata.common.iudprocessor.cache.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.scan.result.vector.ColumnVectorInfo;

/**
 * Scanned result class which will store and provide the result on request
 */
public abstract class AbstractScannedResult {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractScannedResult.class.getName());
  /**
   * current row number
   */
  protected int currentRow = -1;
  /**
   * row mapping indexes
   */
  protected int[] rowMapping;
  /**
   * key size of the fixed length column
   */
  private int fixedLengthKeySize;
  /**
   * total number of rows
   */
  private int totalNumberOfRows;
  /**
   * to keep track of number of rows process
   */
  protected int rowCounter;
  /**
   * dimension column data chunk
   */
  protected DimensionColumnDataChunk[] dataChunks;
  /**
   * measure column data chunk
   */
  protected MeasureColumnDataChunk[] measureDataChunks;
  /**
   * dictionary column block index in file
   */
  protected int[] dictionaryColumnBlockIndexes;

  /**
   * no dictionary column block index in file
   */
  protected int[] noDictionaryColumnBlockIndexes;

  /**
   * column group to is key structure info
   * which will be used to get the key from the complete
   * column group key
   * For example if only one dimension of the column group is selected
   * then from complete column group key it will be used to mask the key and
   * get the particular column key
   */
  protected Map<Integer, KeyStructureInfo> columnGroupKeyStructureInfo;

  /**
   *
   */
  private Map<Integer, GenericQueryType> complexParentIndexToQueryMap;

  private int totalDimensionsSize;

  /**
   * blockedId which will be blockId + blocklet number in the block
   */
  private String blockletId;

  private long rowId;

  /**
   * parent block indexes
   */
  private int[] complexParentBlockIndexes;

  protected BlockletLevelDeleteDeltaDataCache blockletDeleteDeltaCache;

  public AbstractScannedResult(BlockExecutionInfo blockExecutionInfo) {
    this.fixedLengthKeySize = blockExecutionInfo.getFixedLengthKeySize();
    this.noDictionaryColumnBlockIndexes = blockExecutionInfo.getNoDictionaryBlockIndexes();
    this.dictionaryColumnBlockIndexes = blockExecutionInfo.getDictionaryColumnBlockIndex();
    this.columnGroupKeyStructureInfo = blockExecutionInfo.getColumnGroupToKeyStructureInfo();
    this.complexParentIndexToQueryMap = blockExecutionInfo.getComlexDimensionInfoMap();
    this.complexParentBlockIndexes = blockExecutionInfo.getComplexColumnParentBlockIndexes();
    this.totalDimensionsSize = blockExecutionInfo.getQueryDimensions().length;
  }

  /**
   * Below method will be used to set the dimension chunks
   * which will be used to create a row
   *
   * @param dataChunks dimension chunks used in query
   */
  public void setDimensionChunks(DimensionColumnDataChunk[] dataChunks) {
    this.dataChunks = dataChunks;
  }

  /**
   * Below method will be used to set the measure column chunks
   *
   * @param measureDataChunks measure data chunks
   */
  public void setMeasureChunks(MeasureColumnDataChunk[] measureDataChunks) {
    this.measureDataChunks = measureDataChunks;
  }

  /**
   * Below method will be used to get the chunk based in measure ordinal
   *
   * @param ordinal measure ordinal
   * @return measure column chunk
   */
  public MeasureColumnDataChunk getMeasureChunk(int ordinal) {
    return measureDataChunks[ordinal];
  }

  /**
   * Below method will be used to get the key for all the dictionary dimensions
   * which is present in the query
   *
   * @param rowId row id selected after scanning
   * @return return the dictionary key
   */
  protected byte[] getDictionaryKeyArray(int rowId) {
    byte[] completeKey = new byte[fixedLengthKeySize];
    int offset = 0;
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
      offset += dataChunks[dictionaryColumnBlockIndexes[i]]
          .fillChunkData(completeKey, offset, rowId,
              columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));
    }
    rowCounter++;
    return completeKey;
  }

  /**
   * Below method will be used to get the key for all the dictionary dimensions
   * in integer array format which is present in the query
   *
   * @param rowId row id selected after scanning
   * @return return the dictionary key
   */
  protected int[] getDictionaryKeyIntegerArray(int rowId) {
    int[] completeKey = new int[totalDimensionsSize];
    int column = 0;
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
      column = dataChunks[dictionaryColumnBlockIndexes[i]]
          .fillConvertedChunkData(rowId, column, completeKey,
              columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));
    }
    rowCounter++;
    return completeKey;
  }

  /**
   * Fill the column data of dictionary to vector
   */
  public void fillColumnarDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
      column = dataChunks[dictionaryColumnBlockIndexes[i]]
          .fillConvertedChunkData(vectorInfo, column,
              columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));
    }
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarNoDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
      column = dataChunks[noDictionaryColumnBlockIndexes[i]]
          .fillConvertedChunkData(vectorInfo, column,
              columnGroupKeyStructureInfo.get(noDictionaryColumnBlockIndexes[i]));
    }
  }

  /**
   * Fill the measure column data to vector
   */
  public void fillColumnarMeasureBatch(ColumnVectorInfo[] vectorInfo, int[] measuresOrdinal) {
    for (int i = 0; i < measuresOrdinal.length; i++) {
      vectorInfo[i].measureVectorFiller
          .fillMeasureVector(measureDataChunks[measuresOrdinal[i]], vectorInfo[i]);
    }
  }

  public void fillColumnarComplexBatch(ColumnVectorInfo[] vectorInfos) {
    for (int i = 0; i < vectorInfos.length; i++) {
      int offset = vectorInfos[i].offset;
      int len = offset + vectorInfos[i].size;
      int vectorOffset = vectorInfos[i].vectorOffset;
      CarbonColumnVector vector = vectorInfos[i].vector;
      for (int j = offset; j < len; j++) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(byteStream);
        try {
          vectorInfos[i].genericQueryType.parseBlocksAndReturnComplexColumnByteArray(dataChunks,
              rowMapping == null ? j : rowMapping[j], dataOutput);
          Object data = vectorInfos[i].genericQueryType
              .getDataBasedOnDataTypeFromSurrogates(ByteBuffer.wrap(byteStream.toByteArray()));
          vector.putObject(vectorOffset++, data);
        } catch (IOException e) {
          LOGGER.error(e);
        } finally {
          CarbonUtil.closeStreams(dataOutput);
          CarbonUtil.closeStreams(byteStream);
        }
      }
    }
  }

  /**
   * Just increment the counter incase of query only on measures.
   */
  public void incrementCounter() {
    rowCounter ++;
    currentRow ++;
  }

  /**
   * increment the counter.
   */
  public void setRowCounter(int rowCounter) {
    this.rowCounter = rowCounter;
  }

  /**
   * Below method will be used to get the dimension data based on dimension
   * ordinal and index
   *
   * @param dimOrdinal dimension ordinal present in the query
   * @param rowId      row index
   * @return dimension data based on row id
   */
  protected byte[] getDimensionData(int dimOrdinal, int rowId) {
    return dataChunks[dimOrdinal].getChunkData(rowId);
  }

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   *
   * @param rowId row number
   * @return no dictionary keys for all no dictionary dimension
   */
  protected byte[][] getNoDictionaryKeyArray(int rowId) {
    byte[][] noDictionaryColumnsKeys = new byte[noDictionaryColumnBlockIndexes.length][];
    int position = 0;
    for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
      noDictionaryColumnsKeys[position++] =
          dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(rowId);
    }
    return noDictionaryColumnsKeys;
  }

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   *
   * @param rowId row number
   * @return no dictionary keys for all no dictionary dimension
   */
  protected String[] getNoDictionaryKeyStringArray(int rowId) {
    String[] noDictionaryColumnsKeys = new String[noDictionaryColumnBlockIndexes.length];
    int position = 0;
    for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
      noDictionaryColumnsKeys[position++] =
          new String(dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(rowId));
    }
    return noDictionaryColumnsKeys;
  }

  /**
   * @return blockletId
   */
  public String getBlockletId() {
    return blockletId;
  }

  /**
   * @param blockletId
   */
  public void setBlockletId(String blockletId) {
    this.blockletId = CarbonTablePath.getShortBlockId(blockletId);
  }

  /**
   * @return blockletId
   */
  public long getRowId() {
    return rowId;
  }

  /**
   * @param blockletId
   */
  public void setRowId(long rowId) {
    this.rowId = rowId;
  }


  /**
   * Below method will be used to get the complex type keys array based
   * on row id for all the complex type dimension selected in query
   *
   * @param rowId row number
   * @return complex type key array for all the complex dimension selected in query
   */
  protected byte[][] getComplexTypeKeyArray(int rowId) {
    byte[][] complexTypeData = new byte[complexParentBlockIndexes.length][];
    for (int i = 0; i < complexTypeData.length; i++) {
      GenericQueryType genericQueryType =
          complexParentIndexToQueryMap.get(complexParentBlockIndexes[i]);
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream dataOutput = new DataOutputStream(byteStream);
      try {
        genericQueryType.parseBlocksAndReturnComplexColumnByteArray(dataChunks, rowId, dataOutput);
        complexTypeData[i] = byteStream.toByteArray();
      } catch (IOException e) {
        LOGGER.error(e);
      } finally {
        CarbonUtil.closeStreams(dataOutput);
        CarbonUtil.closeStreams(byteStream);
      }
    }
    return complexTypeData;
  }

  /**
   * @return return the total number of row after scanning
   */
  public int numberOfOutputRows() {
    return this.totalNumberOfRows;
  }

  /**
   * to check whether any more row is present in the result
   *
   * @return
   */
  public boolean hasNext() {
    return rowCounter < this.totalNumberOfRows;
  }

  /**
   * As this class will be a flyweight object so
   * for one block all the blocklet scanning will use same result object
   * in that case we need to reset the counter to zero so
   * for new result it will give the result from zero
   */
  public void reset() {
    rowCounter = 0;
    currentRow = -1;
  }

  /**
   * @param totalNumberOfRows set total of number rows valid after scanning
   */
  public void setNumberOfRows(int totalNumberOfRows) {
    this.totalNumberOfRows = totalNumberOfRows;
  }

  /**
   * After applying filter it will return the  bit set with the valid row indexes
   * so below method will be used to set the row indexes
   *
   * @param indexes
   */
  public void setIndexes(int[] indexes) {
    this.rowMapping = indexes;
  }

  /**
   * Below method will be used to check whether measure value is null or not
   *
   * @param ordinal  measure ordinal
   * @param rowIndex row number to be checked
   * @return whether it is null or not
   */
  protected boolean isNullMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getNullValueIndexHolder().getBitSet().get(rowIndex);
  }

  /**
   * Below method will be used to get the measure value of
   * long type
   *
   * @param ordinal  measure ordinal
   * @param rowIndex row number of the measure value
   * @return measure value of long type
   */
  protected long getLongMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getMeasureDataHolder().getReadableLongValueByIndex(rowIndex);
  }

  /**
   * Below method will be used to get the measure value of double type
   *
   * @param ordinal  measure ordinal
   * @param rowIndex row number
   * @return measure value of double type
   */
  protected double getDoubleMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getMeasureDataHolder()
        .getReadableDoubleValueByIndex(rowIndex);
  }

  /**
   * Below method will be used to get the measure type of big decimal data type
   *
   * @param ordinal  ordinal of the of the measure
   * @param rowIndex row number
   * @return measure of big decimal type
   */
  protected BigDecimal getBigDecimalMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getMeasureDataHolder()
        .getReadableBigDecimalValueByIndex(rowIndex);
  }

  public int getRowCounter() {
    return rowCounter;
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  public abstract int getCurrenrRowId();

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  public abstract byte[] getDictionaryKeyArray();

  /**
   * @return dictionary key array for all the dictionary dimension in integer array forat
   * selected in query
   */
  public abstract int[] getDictionaryKeyIntegerArray();

  /**
   * Return the dimension data based on dimension ordinal
   *
   * @param dimensionOrdinal dimension ordinal
   * @return dimension data
   */
  public abstract byte[] getDimensionKey(int dimensionOrdinal);

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  public abstract byte[][] getComplexTypeKeyArray();

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  public abstract byte[][] getNoDictionaryKeyArray();

  /**
   * Below method will be used to get the no dictionary key
   * array in string array format for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  public abstract String[] getNoDictionaryKeyStringArray();

  /**
   * Below method will be used to to check whether measure value
   * is null or for a measure
   *
   * @param ordinal measure ordinal
   * @return is null or not
   */
  public abstract boolean isNullMeasureValue(int ordinal);

  /**
   * Below method will be used to get the measure value for measure
   * of long data type
   *
   * @param ordinal measure ordinal
   * @return long value of measure
   */
  public abstract long getLongMeasureValue(int ordinal);

  /**
   * Below method will be used to get the value of measure of double
   * type
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  public abstract double getDoubleMeasureValue(int ordinal);

  /**
   * Below method will be used to get the data of big decimal type
   * of a measure
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  public abstract BigDecimal getBigDecimalMeasureValue(int ordinal);

  /**
   *
   * @return BlockletLevelDeleteDeltaDataCache.
   */
  public BlockletLevelDeleteDeltaDataCache getDeleteDeltaDataCache() {
    return blockletDeleteDeltaCache;
  }

  /**
   * @param blockletDeleteDeltaCache
   */
  public void setBlockletDeleteDeltaCache(
      BlockletLevelDeleteDeltaDataCache blockletDeleteDeltaCache) {
    this.blockletDeleteDeltaCache = blockletDeleteDeltaCache;
  }
}
