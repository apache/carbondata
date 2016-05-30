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
package org.carbondata.query.carbon.executor.infos;

import java.util.List;
import java.util.Map;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.merger.ScannedResultMerger;
import org.carbondata.query.carbon.model.CustomAggregateExpression;
import org.carbondata.query.filter.executer.FilterExecuter;

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
   * in case of detail+order by query when number of output record is same we
   * need to store data in the disk, so for this check will be used to whether
   * we can write in the disk or not
   */
  private boolean isFileBasedQuery;

  /**
   * id of the query. this will be used to create directory while writing the
   * data file in case of detail+order by query
   */
  private String queryId;

  /**
   * this to handle limit query in case of detail query we are pushing down
   * the limit to executor level so based on the number of limit we can
   * process only that many records
   */
  private int limit;

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

  /**
   * holder of custom aggregation details which will be used to aggregate the
   * custome function UDAF
   */
  private List<CustomAggregateExpression> customAggregateExpressions;

  /**
   * masked byte for block which will be used to unpack the fixed length key,
   * this will be used for updating the older block key with new block key
   * generator
   */
  private int[] maskedByteForBlock;

  /**
   * flag to check whether query is detail query or aggregation query
   */
  private boolean isDetailQuery;

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
  private int[] allSelectedDimensionBlocksIndexes;

  /**
   * will be used to read the measure block from file
   */
  private int[] allSelectedMeasureBlocksIndexes;

  /**
   * this will be used to update the older block fixed length keys with the
   * new block fixed length key
   */
  private KeyStructureInfo keyStructureInfo;

  /**
   * below will be used to sort the data based
   */
  private SortInfo sortInfo;

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
   * to process the scanned result
   */
  private ScannedResultMerger scannedResultProcessor;

  /**
   * key generator used for generating the table block fixed length key
   */
  private KeyGenerator blockKeyGenerator;
  /**
   * dimension aggregator list which will be used to aggregate the dimension
   * data
   */
  private List<DimensionDataAggregator> dimensionAggregator;

  /**
   * each column value size
   */
  private int[] eachColumnValueSize;

  /**
   * partition number
   */
  private String partitionId;

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
   * fileType
   */
  private FileType fileType;

  /**
   * whether it needs only raw byte records with out aggregation.
   */
  private boolean isRawRecordDetailQuery;

  /**
   * @return the tableBlock
   */
  public AbstractIndex getDataBlock() {
    return blockIndex;
  }

  /**
   * @param tableBlock the tableBlock to set
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
   * @return the isFileBasedQuery
   */
  public boolean isFileBasedQuery() {
    return isFileBasedQuery;
  }

  /**
   * @param isFileBasedQuery the isFileBasedQuery to set
   */
  public void setFileBasedQuery(boolean isFileBasedQuery) {
    this.isFileBasedQuery = isFileBasedQuery;
  }

  /**
   * @return the queryId
   */
  public String getQueryId() {
    return queryId;
  }

  /**
   * @param queryId the queryId to set
   */
  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  /**
   * @return the limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit the limit to set
   */
  public void setLimit(int limit) {
    this.limit = limit;
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
   * @return the customAggregateExpression
   */
  public List<CustomAggregateExpression> getCustomAggregateExpressions() {
    return customAggregateExpressions;
  }

  /**
   * @param customAggregateExpression the customAggregateExpression to set
   */
  public void setCustomAggregateExpressions(
      List<CustomAggregateExpression> customAggregateExpressions) {
    this.customAggregateExpressions = customAggregateExpressions;
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
   * @return the isDetailQuery
   */
  public boolean isDetailQuery() {
    return isDetailQuery;
  }

  /**
   * @param isDetailQuery the isDetailQuery to set
   */
  public void setDetailQuery(boolean isDetailQuery) {
    this.isDetailQuery = isDetailQuery;
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
  public int[] getAllSelectedDimensionBlocksIndexes() {
    return allSelectedDimensionBlocksIndexes;
  }

  /**
   * @param allSelectedDimensionBlocksIndexes the allSelectedDimensionBlocksIndexes to set
   */
  public void setAllSelectedDimensionBlocksIndexes(int[] allSelectedDimensionBlocksIndexes) {
    this.allSelectedDimensionBlocksIndexes = allSelectedDimensionBlocksIndexes;
  }

  /**
   * @return the allSelectedMeasureBlocksIndexes
   */
  public int[] getAllSelectedMeasureBlocksIndexes() {
    return allSelectedMeasureBlocksIndexes;
  }

  /**
   * @param allSelectedMeasureBlocksIndexes the allSelectedMeasureBlocksIndexes to set
   */
  public void setAllSelectedMeasureBlocksIndexes(int[] allSelectedMeasureBlocksIndexes) {
    this.allSelectedMeasureBlocksIndexes = allSelectedMeasureBlocksIndexes;
  }

  /**
   * @return the restructureInfos
   */
  public KeyStructureInfo getKeyStructureInfo() {
    return keyStructureInfo;
  }

  /**
   * @param restructureInfos the restructureInfos to set
   */
  public void setKeyStructureInfo(KeyStructureInfo keyStructureInfo) {
    this.keyStructureInfo = keyStructureInfo;
  }

  /**
   * @return the sortInfos
   */
  public SortInfo getSortInfo() {
    return sortInfo;
  }

  /**
   * @param sortInfos the sortInfos to set
   */
  public void setSortInfo(SortInfo sortInfo) {
    this.sortInfo = sortInfo;
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
   * @return the scannedResultProcessor
   */
  public ScannedResultMerger getScannedResultProcessor() {
    return scannedResultProcessor;
  }

  /**
   * @param scannedResultProcessor the scannedResultProcessor to set
   */
  public void setScannedResultProcessor(ScannedResultMerger scannedResultProcessor) {
    this.scannedResultProcessor = scannedResultProcessor;
  }

  /**
   * @return the filterEvaluatorTree
   */
  public FilterExecuter getFilterExecuterTree() {
    return filterExecuterTree;
  }

  /**
   * @param filterEvaluatorTree the filterEvaluatorTree to set
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
   * @return the dimensionAggregator
   */
  public List<DimensionDataAggregator> getDimensionAggregator() {
    return dimensionAggregator;
  }

  /**
   * @param dimensionAggregator the dimensionAggregator to set
   */
  public void setDimensionAggregator(List<DimensionDataAggregator> dimensionAggregator) {
    this.dimensionAggregator = dimensionAggregator;
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
   * @return the partitionId
   */
  public String getPartitionId() {
    return partitionId;
  }

  /**
   * @param partitionId the partitionId to set
   */
  public void setPartitionId(String partitionId) {
    this.partitionId = partitionId;
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
   * @return the columnIdToDcitionaryMapping
   */
  public Map<String, Dictionary> getColumnIdToDcitionaryMapping() {
    return columnIdToDcitionaryMapping;
  }

  /**
   * @param columnIdToDcitionaryMapping the columnIdToDcitionaryMapping to set
   */
  public void setColumnIdToDcitionaryMapping(Map<String, Dictionary> columnIdToDcitionaryMapping) {
    this.columnIdToDcitionaryMapping = columnIdToDcitionaryMapping;
  }

  /**
   * @return the fileType
   */
  public FileType getFileType() {
    return fileType;
  }

  /**
   * @param fileType the fileType to set
   */
  public void setFileType(FileType fileType) {
    this.fileType = fileType;
  }

  public boolean isRawRecordDetailQuery() {
    return isRawRecordDetailQuery;
  }

  public void setRawRecordDetailQuery(boolean rawRecordDetailQuery) {
    isRawRecordDetailQuery = rawRecordDetailQuery;
  }
}
