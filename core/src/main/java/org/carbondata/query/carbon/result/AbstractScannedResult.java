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
package org.carbondata.query.carbon.result;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.complex.querytypes.GenericQueryType;

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
  private int rowCounter;
  /**
   * dimension column data chunk
   */
  private DimensionColumnDataChunk[] dataChunks;
  /**
   * measure column data chunk
   */
  private MeasureColumnDataChunk[] measureDataChunks;
  /**
   * dictionary column block index in file
   */
  private int[] dictionaryColumnBlockIndexes;

  /**
   * no dictionary column block index in file
   */
  private int[] noDictionaryColumnBlockIndexes;

  /**
   * column group to is key structure info
   * which will be used to get the key from the complete
   * column group key
   * For example if only one dimension of the column group is selected
   * then from complete column group key it will be used to mask the key and
   * get the particular column key
   */
  private Map<Integer, KeyStructureInfo> columnGroupKeyStructureInfo;

  /**
   *
   */
  private Map<Integer, GenericQueryType> complexParentIndexToQueryMap;

  /**
   * parent block indexes
   */
  private int[] complexParentBlockIndexes;

  public AbstractScannedResult(BlockExecutionInfo blockExecutionInfo) {
    this.fixedLengthKeySize = blockExecutionInfo.getFixedLengthKeySize();
    this.noDictionaryColumnBlockIndexes = blockExecutionInfo.getNoDictionaryBlockIndexes();
    this.dictionaryColumnBlockIndexes = blockExecutionInfo.getDictionaryColumnBlockIndex();
    this.columnGroupKeyStructureInfo = blockExecutionInfo.getColumnGroupToKeyStructureInfo();
    this.complexParentIndexToQueryMap = blockExecutionInfo.getComlexDimensionInfoMap();
    this.complexParentBlockIndexes = blockExecutionInfo.getComplexColumnParentBlockIndexes();
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
   * Just increment the counter incase of query only on measures.
   */
  public void incrementCounter() {
    rowCounter ++;
    currentRow ++;
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
}
