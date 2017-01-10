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
package org.apache.carbondata.scan.executor.infos;

import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryMeasure;

/**
 * Below class will have all the properties which needed during query execution
 * for one block
 */
public class BlockExecutionInfo {

  /**
   * block on which query will be executed
   */
  private AbstractIndex blockIndex;

  /**
   * each segment key size can be different and in that case we need to update
   * the fixed key with latest segment key generator. so this property will
   * tell whether this is required or not if key size is same then it is not
   * required
   */
  private boolean isFixedKeyUpdateRequired;

  /**
   * below to store all the information required for aggregation during query
   * execution
   */
  private AggregatorInfo aggregatorInfo;

  /**
   * this will be used to get the first tentative block from which query
   * execution start, this will be useful in case of filter query to get the
   * start block based on filter values
   */
  private IndexKey startKey;

  /**
   * this will be used to get the last tentative block till which scanning
   * will be done, this will be useful in case of filter query to get the last
   * block based on filter values
   */
  private IndexKey endKey;

  private String blockId;

  /**
   * masked byte for block which will be used to unpack the fixed length key,
   * this will be used for updating the older block key with new block key
   * generator
   */
  private int[] maskedByteForBlock;

  /**
   * total number of dimension in block
   */
  private int totalNumberDimensionBlock;

  /**
   * total number of measure in block
   */
  private int totalNumberOfMeasureBlock;

  /**
   * will be used to read the dimension block from file
   */
  private int[][] allSelectedDimensionBlocksIndexes;

  /**
   * will be used to read the measure block from file
   */
  private int[][] allSelectedMeasureBlocksIndexes;

  /**
   * this will be used to update the older block fixed length keys with the
   * new block fixed length key
   */
  private KeyStructureInfo keyStructureInfo;

  /**
   * first block from which query execution will start
   */
  private DataRefNode firstDataBlock;

  /**
   * number of block to be scanned in the query
   */
  private long numberOfBlockToScan;

  /**
   * key size of the fixed length dimension column
   */
  private int fixedLengthKeySize;

  /**
   * dictionary column block indexes based on query
   */
  private int[] dictionaryColumnBlockIndex;
  /**
   * no dictionary column block indexes in based on the query order
   */
  private int[] noDictionaryBlockIndexes;

  /**
   * key generator used for generating the table block fixed length key
   */
  private KeyGenerator blockKeyGenerator;

  /**
   * each column value size
   */
  private int[] eachColumnValueSize;

  /**
   * column group block index in file to key structure info mapping
   */
  private Map<Integer, KeyStructureInfo> columnGroupToKeyStructureInfo;

  /**
   * mapping of dictionary dimension to its dictionary mapping which will be
   * used to get the actual data from dictionary for aggregation, sorting
   */
  private Map<String, Dictionary> columnIdToDcitionaryMapping;

  /**
   * filter tree to execute the filter
   */
  private FilterExecuter filterExecuterTree;

  /**
   * whether it needs only raw byte records with out aggregation.
   */
  private boolean isRawRecordDetailQuery;

  /**
   * start index of blocklets
   */
  private int startBlockletIndex;

  /**
   * number of blocklet to be scanned
   */
  private int numberOfBlockletToScan;

  /**
   * complexParentIndexToQueryMap
   */
  private Map<Integer, GenericQueryType> complexParentIndexToQueryMap;

  /**
   * complex dimension parent block indexes;
   */
  private int[] complexColumnParentBlockIndexes;

  /**
   * @return the tableBlock
   */
  public AbstractIndex getDataBlock() {
    return blockIndex;
  }

  /**
   * list of dimension selected for in query
   */
  private QueryDimension[] queryDimensions;

  /**
   * list of measure selected in query
   */
  private QueryMeasure[] queryMeasures;

  /**
   * whether it needs to read data in vector/columnar format.
   */
  private boolean vectorBatchCollector;

  /**
   * absolute table identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;

  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  public void setAbsoluteTableIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
  }

  /**
   * @param blockIndex the tableBlock to set
   */
  public void setDataBlock(AbstractIndex blockIndex) {
    this.blockIndex = blockIndex;
  }

  /**
   * @return the isFixedKeyUpdateRequired
   */
  public boolean isFixedKeyUpdateRequired() {
    return isFixedKeyUpdateRequired;
  }

  /**
   * @param isFixedKeyUpdateRequired the isFixedKeyUpdateRequired to set
   */
  public void setFixedKeyUpdateRequired(boolean isFixedKeyUpdateRequired) {
    this.isFixedKeyUpdateRequired = isFixedKeyUpdateRequired;
  }

  /**
   * @return the aggregatorInfos
   */
  public AggregatorInfo getAggregatorInfo() {
    return aggregatorInfo;
  }

  /**
   * @param aggregatorInfo the aggregatorInfos to set
   */
  public void setAggregatorInfo(AggregatorInfo aggregatorInfo) {
    this.aggregatorInfo = aggregatorInfo;
  }

  /**
   * @return the startKey
   */
  public IndexKey getStartKey() {
    return startKey;
  }

  /**
   * @param startKey the startKey to set
   */
  public void setStartKey(IndexKey startKey) {
    this.startKey = startKey;
  }

  /**
   * @return the endKey
   */
  public IndexKey getEndKey() {
    return endKey;
  }

  /**
   * @param endKey the endKey to set
   */
  public void setEndKey(IndexKey endKey) {
    this.endKey = endKey;
  }

  /**
   * @return the maskedByteForBlock
   */
  public int[] getMaskedByteForBlock() {
    return maskedByteForBlock;
  }



  /**
   * @param maskedByteForBlock the maskedByteForBlock to set
   */
  public void setMaskedByteForBlock(int[] maskedByteForBlock) {
    this.maskedByteForBlock = maskedByteForBlock;
  }

  /**
   * @return the totalNumberDimensionBlock
   */
  public int getTotalNumberDimensionBlock() {
    return totalNumberDimensionBlock;
  }

  /**
   * @param totalNumberDimensionBlock the totalNumberDimensionBlock to set
   */
  public void setTotalNumberDimensionBlock(int totalNumberDimensionBlock) {
    this.totalNumberDimensionBlock = totalNumberDimensionBlock;
  }

  /**
   * @return the totalNumberOfMeasureBlock
   */
  public int getTotalNumberOfMeasureBlock() {
    return totalNumberOfMeasureBlock;
  }

  /**
   * @param totalNumberOfMeasureBlock the totalNumberOfMeasureBlock to set
   */
  public void setTotalNumberOfMeasureBlock(int totalNumberOfMeasureBlock) {
    this.totalNumberOfMeasureBlock = totalNumberOfMeasureBlock;
  }

  /**
   * @return the allSelectedDimensionBlocksIndexes
   */
  public int[][] getAllSelectedDimensionBlocksIndexes() {
    return allSelectedDimensionBlocksIndexes;
  }

  /**
   * @param allSelectedDimensionBlocksIndexes the allSelectedDimensionBlocksIndexes to set
   */
  public void setAllSelectedDimensionBlocksIndexes(int[][] allSelectedDimensionBlocksIndexes) {
    this.allSelectedDimensionBlocksIndexes = allSelectedDimensionBlocksIndexes;
  }

  /**
   * @return the allSelectedMeasureBlocksIndexes
   */
  public int[][] getAllSelectedMeasureBlocksIndexes() {
    return allSelectedMeasureBlocksIndexes;
  }

  /**
   * @param allSelectedMeasureBlocksIndexes the allSelectedMeasureBlocksIndexes to set
   */
  public void setAllSelectedMeasureBlocksIndexes(int[][] allSelectedMeasureBlocksIndexes) {
    this.allSelectedMeasureBlocksIndexes = allSelectedMeasureBlocksIndexes;
  }

  /**
   * @return the restructureInfos
   */
  public KeyStructureInfo getKeyStructureInfo() {
    return keyStructureInfo;
  }

  /**
   * @param keyStructureInfo the restructureInfos to set
   */
  public void setKeyStructureInfo(KeyStructureInfo keyStructureInfo) {
    this.keyStructureInfo = keyStructureInfo;
  }

  /**
   * @return the firstDataBlock
   */
  public DataRefNode getFirstDataBlock() {
    return firstDataBlock;
  }

  /**
   * @param firstDataBlock the firstDataBlock to set
   */
  public void setFirstDataBlock(DataRefNode firstDataBlock) {
    this.firstDataBlock = firstDataBlock;
  }

  /**
   * @return the numberOfBlockToScan
   */
  public long getNumberOfBlockToScan() {
    return numberOfBlockToScan;
  }

  /**
   * @param numberOfBlockToScan the numberOfBlockToScan to set
   */
  public void setNumberOfBlockToScan(long numberOfBlockToScan) {
    this.numberOfBlockToScan = numberOfBlockToScan;
  }

  /**
   * @return the fixedLengthKeySize
   */
  public int getFixedLengthKeySize() {
    return fixedLengthKeySize;
  }

  /**
   * @param fixedLengthKeySize the fixedLengthKeySize to set
   */
  public void setFixedLengthKeySize(int fixedLengthKeySize) {
    this.fixedLengthKeySize = fixedLengthKeySize;
  }

  /**
   * @return the filterEvaluatorTree
   */
  public FilterExecuter getFilterExecuterTree() {
    return filterExecuterTree;
  }

  /**
   * @param filterExecuterTree the filterEvaluatorTree to set
   */
  public void setFilterExecuterTree(FilterExecuter filterExecuterTree) {
    this.filterExecuterTree = filterExecuterTree;
  }

  /**
   * @return the tableBlockKeyGenerator
   */
  public KeyGenerator getBlockKeyGenerator() {
    return blockKeyGenerator;
  }

  /**
   * @param tableBlockKeyGenerator the tableBlockKeyGenerator to set
   */
  public void setBlockKeyGenerator(KeyGenerator tableBlockKeyGenerator) {
    this.blockKeyGenerator = tableBlockKeyGenerator;
  }

  /**
   * @return the eachColumnValueSize
   */
  public int[] getEachColumnValueSize() {
    return eachColumnValueSize;
  }

  /**
   * @param eachColumnValueSize the eachColumnValueSize to set
   */
  public void setEachColumnValueSize(int[] eachColumnValueSize) {
    this.eachColumnValueSize = eachColumnValueSize;
  }

  /**
   * @return the dictionaryColumnBlockIndex
   */
  public int[] getDictionaryColumnBlockIndex() {
    return dictionaryColumnBlockIndex;
  }

  /**
   * @param dictionaryColumnBlockIndex the dictionaryColumnBlockIndex to set
   */
  public void setDictionaryColumnBlockIndex(int[] dictionaryColumnBlockIndex) {
    this.dictionaryColumnBlockIndex = dictionaryColumnBlockIndex;
  }

  /**
   * @return the noDictionaryBlockIndexes
   */
  public int[] getNoDictionaryBlockIndexes() {
    return noDictionaryBlockIndexes;
  }

  /**
   * @param noDictionaryBlockIndexes the noDictionaryBlockIndexes to set
   */
  public void setNoDictionaryBlockIndexes(int[] noDictionaryBlockIndexes) {
    this.noDictionaryBlockIndexes = noDictionaryBlockIndexes;
  }

  /**
   * @return the columnGroupToKeyStructureInfo
   */
  public Map<Integer, KeyStructureInfo> getColumnGroupToKeyStructureInfo() {
    return columnGroupToKeyStructureInfo;
  }

  /**
   * @param columnGroupToKeyStructureInfo the columnGroupToKeyStructureInfo to set
   */
  public void setColumnGroupToKeyStructureInfo(
      Map<Integer, KeyStructureInfo> columnGroupToKeyStructureInfo) {
    this.columnGroupToKeyStructureInfo = columnGroupToKeyStructureInfo;
  }

  /**
   * @param columnIdToDcitionaryMapping the columnIdToDcitionaryMapping to set
   */
  public void setColumnIdToDcitionaryMapping(Map<String, Dictionary> columnIdToDcitionaryMapping) {
    this.columnIdToDcitionaryMapping = columnIdToDcitionaryMapping;
  }

  public boolean isRawRecordDetailQuery() {
    return isRawRecordDetailQuery;
  }

  public void setRawRecordDetailQuery(boolean rawRecordDetailQuery) {
    isRawRecordDetailQuery = rawRecordDetailQuery;
  }

  /**
   * @return the complexParentIndexToQueryMap
   */
  public Map<Integer, GenericQueryType> getComlexDimensionInfoMap() {
    return complexParentIndexToQueryMap;
  }

  /**
   * @param complexDimensionInfoMap the complexParentIndexToQueryMap to set
   */
  public void setComplexDimensionInfoMap(Map<Integer, GenericQueryType> complexDimensionInfoMap) {
    this.complexParentIndexToQueryMap = complexDimensionInfoMap;
  }

  /**
   * @return the complexColumnParentBlockIndexes
   */
  public int[] getComplexColumnParentBlockIndexes() {
    return complexColumnParentBlockIndexes;
  }

  /**
   * @param complexColumnParentBlockIndexes the complexColumnParentBlockIndexes to set
   */
  public void setComplexColumnParentBlockIndexes(int[] complexColumnParentBlockIndexes) {
    this.complexColumnParentBlockIndexes = complexColumnParentBlockIndexes;
  }

  public QueryDimension[] getQueryDimensions() {
    return queryDimensions;
  }

  public void setQueryDimensions(QueryDimension[] queryDimensions) {
    this.queryDimensions = queryDimensions;
  }

  public QueryMeasure[] getQueryMeasures() {
    return queryMeasures;
  }

  public void setQueryMeasures(QueryMeasure[] queryMeasures) {
    this.queryMeasures = queryMeasures;
  }

  /**
   * The method to set the number of blocklets to be scanned
   *
   * @param numberOfBlockletToScan
   */
  public void setNumberOfBlockletToScan(int numberOfBlockletToScan) {
    this.numberOfBlockletToScan = numberOfBlockletToScan;
  }

  /**
   * get the no of blocklet  to be scanned
   *
   * @return
   */
  public int getNumberOfBlockletToScan() {
    return numberOfBlockletToScan;
  }

  /**
   * returns the blocklet index to be scanned
   *
   * @return
   */
  public int getStartBlockletIndex() {
    return startBlockletIndex;
  }

  /**
   * set the blocklet index to be scanned
   *
   * @param startBlockletIndex
   */
  public void setStartBlockletIndex(int startBlockletIndex) {
    this.startBlockletIndex = startBlockletIndex;
  }

  public boolean isVectorBatchCollector() {
    return vectorBatchCollector;
  }

  public void setVectorBatchCollector(boolean vectorBatchCollector) {
    this.vectorBatchCollector = vectorBatchCollector;
  }

  public String getBlockId() {
    return blockId;
  }

  public void setBlockId(String blockId) {
    this.blockId = blockId;
  }
}
