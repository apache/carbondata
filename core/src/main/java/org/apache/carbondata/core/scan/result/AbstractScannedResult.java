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
package org.apache.carbondata.core.scan.result;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.DeleteDeltaVo;
import org.apache.carbondata.core.mutate.TupleIdEnum;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

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

  protected int pageCounter;
  /**
   * row mapping indexes
   */
  protected int[][] rowMapping;
  /**
   * key size of the fixed length column
   */
  private int fixedLengthKeySize;
  /**
   * total number of rows per page
   */
  private int[] numberOfRows;

  /**
   * Total number of rows.
   */
  private int totalNumberOfRows;
  /**
   * to keep track of number of rows process
   */
  protected int rowCounter;
  /**
   * dimension column data chunk
   */
  protected DimensionColumnDataChunk[][] dimensionDataChunks;

  /**
   * Raw dimension chunks;
   */
  protected DimensionRawColumnChunk[] dimRawColumnChunks;

  /**
   * Raw dimension chunks;
   */
  protected MeasureRawColumnChunk[] msrRawColumnChunks;
  /**
   * measure column data chunk
   */
  protected ColumnPage[][] measureDataChunks;
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

  /**
   * blockletid+pageumber to deleted reocrd map
   */
  private Map<String, DeleteDeltaVo> deletedRecordMap;

  /**
   * current page delete delta vo
   */
  private DeleteDeltaVo currentDeleteDeltaVo;

  /**
   * actual blocklet number
   */
  private String blockletNumber;

  public AbstractScannedResult(BlockExecutionInfo blockExecutionInfo) {
    this.fixedLengthKeySize = blockExecutionInfo.getFixedLengthKeySize();
    this.noDictionaryColumnBlockIndexes = blockExecutionInfo.getNoDictionaryBlockIndexes();
    this.dictionaryColumnBlockIndexes = blockExecutionInfo.getDictionaryColumnBlockIndex();
    this.columnGroupKeyStructureInfo = blockExecutionInfo.getColumnGroupToKeyStructureInfo();
    this.complexParentIndexToQueryMap = blockExecutionInfo.getComlexDimensionInfoMap();
    this.complexParentBlockIndexes = blockExecutionInfo.getComplexColumnParentBlockIndexes();
    this.totalDimensionsSize = blockExecutionInfo.getQueryDimensions().length;
    this.deletedRecordMap = blockExecutionInfo.getDeletedRecordsMap();
  }

  /**
   * Below method will be used to set the dimension chunks
   * which will be used to create a row
   *
   * @param dataChunks dimension chunks used in query
   */
  public void setDimensionChunks(DimensionColumnDataChunk[][] dataChunks) {
    this.dimensionDataChunks = dataChunks;
  }

  /**
   * Below method will be used to set the measure column chunks
   *
   * @param measureDataChunks measure data chunks
   */
  public void setMeasureChunks(ColumnPage[][] measureDataChunks) {
    this.measureDataChunks = measureDataChunks;
  }

  public void setDimRawColumnChunks(DimensionRawColumnChunk[] dimRawColumnChunks) {
    this.dimRawColumnChunks = dimRawColumnChunks;
  }

  public void setMsrRawColumnChunks(MeasureRawColumnChunk[] msrRawColumnChunks) {
    this.msrRawColumnChunks = msrRawColumnChunks;
  }

  /**
   * Below method will be used to get the chunk based in measure ordinal
   *
   * @param ordinal measure ordinal
   * @return measure column chunk
   */
  public ColumnPage getMeasureChunk(int ordinal) {
    return measureDataChunks[ordinal][pageCounter];
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
      offset += dimensionDataChunks[dictionaryColumnBlockIndexes[i]][pageCounter]
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
      column = dimensionDataChunks[dictionaryColumnBlockIndexes[i]][pageCounter]
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
      column = dimensionDataChunks[dictionaryColumnBlockIndexes[i]][pageCounter]
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
      column = dimensionDataChunks[noDictionaryColumnBlockIndexes[i]][pageCounter]
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
              .fillMeasureVector(measureDataChunks[measuresOrdinal[i]][pageCounter], vectorInfo[i]);
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
          vectorInfos[i].genericQueryType
                  .parseBlocksAndReturnComplexColumnByteArray(dimRawColumnChunks,
                          rowMapping == null ? j :
                                  rowMapping[pageCounter][j], pageCounter, dataOutput);
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
   * Fill the column data to vector
   */
  public void fillColumnarImplicitBatch(ColumnVectorInfo[] vectorInfo) {
    for (int i = 0; i < vectorInfo.length; i++) {
      ColumnVectorInfo columnVectorInfo = vectorInfo[i];
      CarbonColumnVector vector = columnVectorInfo.vector;
      int offset = columnVectorInfo.offset;
      int vectorOffset = columnVectorInfo.vectorOffset;
      int len = offset + columnVectorInfo.size;
      for (int j = offset; j < len; j++) {
        // Considering only String case now as we support only
        String data = getBlockletId();
        if (CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID
                .equals(columnVectorInfo.dimension.getColumnName())) {
          data = data + CarbonCommonConstants.FILE_SEPARATOR + pageCounter
                  + CarbonCommonConstants.FILE_SEPARATOR + (rowMapping == null ?
                  j :
                  rowMapping[pageCounter][j]);
        }
        vector.putBytes(vectorOffset++,
                data.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      }
    }
  }

  /**
   * Just increment the counter incase of query only on measures.
   */
  public void incrementCounter() {
    rowCounter++;
    currentRow++;
  }

  /**
   * Just increment the page counter and reset the remaining counters.
   */
  public void incrementPageCounter() {
    rowCounter = 0;
    currentRow = -1;
    pageCounter++;
    fillDataChunks();
    if (null != deletedRecordMap) {
      currentDeleteDeltaVo = deletedRecordMap.get(blockletNumber + "_" + pageCounter);
    }
  }

  /**
   * This case is used only in case of compaction, since it does not use filter flow.
   */
  public void fillDataChunks() {
    freeDataChunkMemory();
    if (pageCounter >= numberOfRows.length) {
      return;
    }
    for (int i = 0; i < dimensionDataChunks.length; i++) {
      if (dimensionDataChunks[i][pageCounter] == null && dimRawColumnChunks[i] != null) {
        dimensionDataChunks[i][pageCounter] =
                dimRawColumnChunks[i].convertToDimColDataChunkWithOutCache(pageCounter);
      }
    }

    for (int i = 0; i < measureDataChunks.length; i++) {
      if (measureDataChunks[i][pageCounter] == null && msrRawColumnChunks[i] != null) {
        measureDataChunks[i][pageCounter] =
                msrRawColumnChunks[i].convertToColumnPageWithOutCache(pageCounter);
      }
    }
  }

  // free the memory for the last page chunk
  private void freeDataChunkMemory() {
    for (int i = 0; i < dimensionDataChunks.length; i++) {
      if (pageCounter > 0 && dimensionDataChunks[i][pageCounter - 1] != null) {
        dimensionDataChunks[i][pageCounter - 1].freeMemory();
        dimensionDataChunks[i][pageCounter - 1] = null;
      }
    }
    for (int i = 0; i < measureDataChunks.length; i++) {
      if (pageCounter > 0 && measureDataChunks[i][pageCounter - 1] != null) {
        measureDataChunks[i][pageCounter - 1].freeMemory();
        measureDataChunks[i][pageCounter - 1] = null;
      }
    }
  }

  public int numberOfpages() {
    return numberOfRows.length;
  }

  /**
   * Get total rows in the current page
   *
   * @return
   */
  public int getCurrentPageRowCount() {
    return numberOfRows[pageCounter];
  }

  public int getCurrentPageCounter() {
    return pageCounter;
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
    return dimensionDataChunks[dimOrdinal][pageCounter].getChunkData(rowId);
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
              dimensionDataChunks[noDictionaryColumnBlockIndexes[i]][pageCounter]
                      .getChunkData(rowId);
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
      noDictionaryColumnsKeys[position++] = new String(
              dimensionDataChunks[noDictionaryColumnBlockIndexes[i]][pageCounter]
                      .getChunkData(rowId),
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
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
    blockletNumber = CarbonUpdateUtil.getRequiredFieldFromTID(blockletId, TupleIdEnum.BLOCKLET_ID);
    // if deleted recors map is present for this block
    // then get the first page deleted vo
    if (null != deletedRecordMap) {
      currentDeleteDeltaVo = deletedRecordMap.get(blockletNumber + '_' + pageCounter);
    }
  }

  /**
   * @return blockletId
   */
  public long getRowId() {
    return rowId;
  }

  /**
   * @param rowId
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
        genericQueryType
                .parseBlocksAndReturnComplexColumnByteArray(dimRawColumnChunks, rowId, pageCounter,
                        dataOutput);
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
  public GenericQueryType getChildProjectionQueryType(GenericQueryType parentQueryType ,
                                                      String childProjection) {
    int childLength = parentQueryType.getChildern().size();
    for (int i = 0; i < childLength; i++) {
      GenericQueryType childQueryType = parentQueryType.getChildern().get(i);
      if (childProjection.equalsIgnoreCase(childQueryType.getName())) {
        return childQueryType;
      } else if (childQueryType.getChildern() != null &&
              childQueryType.getChildern().size() > 0) {
        GenericQueryType found = getChildProjectionQueryType(childQueryType ,
                childProjection);
        if (found != null) {
          return found;
        }

      }
    }

    return null;
  }

  /**
   * Get byte array for projected dimensions
   * @param rowId
   * @param childProjection
   * @param parentblockIndex
   * @return byte[]
   */
  protected byte[] getComplexTypeKeyArrayForDimension(int rowId , String childProjection ,
                                                      int parentblockIndex) {
    GenericQueryType genericQueryType =
            complexParentIndexToQueryMap.get(parentblockIndex) ;

    GenericQueryType projectedQueryType = getChildProjectionQueryType(genericQueryType ,
            childProjection);
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream dataOutput = new DataOutputStream(byteStream);
    try {
      projectedQueryType
              .parseBlocksAndReturnComplexColumnByteArray(dimRawColumnChunks, rowId, pageCounter,
                      dataOutput);
    } catch (IOException e) {
      LOGGER.error(e);
    } finally {
      CarbonUtil.closeStreams(dataOutput);
      CarbonUtil.closeStreams(byteStream);
    }

    return byteStream.toByteArray();

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
    if (pageCounter < numberOfRows.length && rowCounter < this.numberOfRows[pageCounter]) {
      return true;
    } else if (pageCounter < numberOfRows.length) {
      pageCounter++;
      fillDataChunks();
      rowCounter = 0;
      currentRow = -1;
      if (null != deletedRecordMap) {
        currentDeleteDeltaVo = deletedRecordMap.get(blockletNumber + "_" + pageCounter);
      }
      return hasNext();
    }
    return false;
  }

  /**
   * Below method will be used to free the occupied memory
   */
  public void freeMemory() {
    // first free the dimension chunks
    if (null != dimensionDataChunks) {
      for (int i = 0; i < dimensionDataChunks.length; i++) {
        if (null != dimensionDataChunks[i]) {
          for (int j = 0; j < dimensionDataChunks[i].length; j++) {
            if (null != dimensionDataChunks[i][j]) {
              dimensionDataChunks[i][j].freeMemory();
            }
          }
        }
      }
    }
    // free the measure data chunks
    if (null != measureDataChunks) {
      for (int i = 0; i < measureDataChunks.length; i++) {
        if (null != measureDataChunks[i]) {
          for (int j = 0; j < measureDataChunks[i].length; j++) {
            if (null != measureDataChunks[i][j]) {
              measureDataChunks[i][j].freeMemory();
            }
          }
        }
      }
    }
    // free the raw chunks
    if (null != dimRawColumnChunks) {
      for (int i = 0; i < dimRawColumnChunks.length; i++) {
        if (null != dimRawColumnChunks[i]) {
          dimRawColumnChunks[i].freeMemory();
        }
      }
    }
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
    pageCounter = 0;
  }

  /**
   * @param numberOfRows set total of number rows valid after scanning
   */
  public void setNumberOfRows(int[] numberOfRows) {
    this.numberOfRows = numberOfRows;

    for (int count : numberOfRows) {
      totalNumberOfRows += count;
    }
  }

  /**
   * After applying filter it will return the  bit set with the valid row indexes
   * so below method will be used to set the row indexes
   *
   * @param indexes
   */
  public void setIndexes(int[][] indexes) {
    this.rowMapping = indexes;
  }

  public int getRowCounter() {
    return rowCounter;
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  public abstract int getCurrentRowId();

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
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  public abstract byte[][] getComplexTypeKeyArray();

  public abstract byte[] getComplexTypeKeyArrayForDimension(String projectedDimension ,
                                                            int parentblockIndex);

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
   * Mark the filtered rows in columnar batch. These rows will not be added to vector batches later.
   * @param columnarBatch
   * @param startRow
   * @param size
   * @param vectorOffset
   */
  public int markFilteredRows(CarbonColumnarBatch columnarBatch, int startRow, int size,
                              int vectorOffset) {
    int rowsFiltered = 0;
    if (currentDeleteDeltaVo != null) {
      int len = startRow + size;
      for (int i = startRow; i < len; i++) {
        int rowId = rowMapping != null ? rowMapping[pageCounter][i] : i;
        if (currentDeleteDeltaVo.containsRow(rowId)) {
          columnarBatch.markFiltered(vectorOffset);
          rowsFiltered++;
        }
        vectorOffset++;
      }
    }
    return rowsFiltered;
  }

  /**
   * Below method will be used to check row got deleted
   *
   * @param rowId
   * @return is present in deleted row
   */
  public boolean containsDeletedRow(int rowId) {
    if (null != currentDeleteDeltaVo) {
      return currentDeleteDeltaVo.containsRow(rowId);
    }
    return false;
  }
}
