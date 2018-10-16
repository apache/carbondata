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
package org.apache.carbondata.core.scan.executor.infos;

import java.util.Map;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.mutate.DeleteDeltaVo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

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
   * below to store all the information required for measures during query
   * execution
   */
  private MeasureInfo measureInfo;

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
   * total number of dimension in block
   */
  private int totalNumberDimensionToRead;

  /**
   * total number of measure in block
   */
  private int totalNumberOfMeasureToRead;

  /**
   * will be used to read the dimension block from file
   */
  private int[][] allSelectedDimensionColumnIndexRange;

  /**
   * will be used to read the measure block from file
   */
  private int[][] allSelectedMeasureIndexRange;

  /**
   * list of dimension present in the projection
   */
  private int[] projectionListDimensionIndexes;

  /**
   * list of measure present in the projection
   */
  private int[] projectionListMeasureIndexes;

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
  private int[] dictionaryColumnChunkIndex;
  /**
   * no dictionary column block indexes in based on the query order
   */
  private int[] noDictionaryColumnChunkIndexes;

  /**
   * each column value size
   */
  private int[] eachColumnValueSize;

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
   * list of dimension present in the current block. This will be
   * different in case of restructured block
   */
  private ProjectionDimension[] projectionDimensions;

  /**
   * list of dimension selected for in query
   */
  private ProjectionDimension[] actualQueryDimensions;

  /**
   * list of dimension present in the current block. This will be
   * different in case of restructured block
   */
  private ProjectionMeasure[] projectionMeasures;

  /**
   * list of measure selected in query
   */
  private ProjectionMeasure[] actualQueryMeasures;

  /**
   * variable to maintain dimension existence and default value info
   */
  private DimensionInfo dimensionInfo;

  /**
   * whether it needs to read data in vector/columnar format.
   */
  private boolean vectorBatchCollector;

  /**
   * flag to specify that whether the current block is with latest schema or old schema
   */
  private boolean isRestructuredBlock;

  /**
   * delete delta file path
   */
  private String[] deleteDeltaFilePath;

  /**
   * whether to prefetch the blocklet data while scanning
   */
  private boolean prefetchBlocklet = true;

  private Map<String, DeleteDeltaVo> deletedRecordsMap;

  /**
   * whether it require to output the row id
   */
  private boolean requiredRowId;

  /**
   * model for collecting query stats
   */
  private QueryStatisticsModel queryStatisticsModel;

  /**
   * It fills the vector directly from decoded column page with out any staging and conversions
   */
  private boolean isDirectVectorFill;

  /**
   * @param blockIndex the tableBlock to set
   */
  public void setDataBlock(AbstractIndex blockIndex) {
    this.blockIndex = blockIndex;
  }

  /**
   * @return the aggregatorInfos
   */
  public MeasureInfo getMeasureInfo() {
    return measureInfo;
  }

  /**
   * @param measureInfo the aggregatorInfos to set
   */
  public void setMeasureInfo(MeasureInfo measureInfo) {
    this.measureInfo = measureInfo;
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
   * @return the totalNumberDimensionToRead
   */
  public int getTotalNumberDimensionToRead() {
    return totalNumberDimensionToRead;
  }

  /**
   * @param totalNumberDimensionToRead the totalNumberDimensionToRead to set
   */
  public void setTotalNumberDimensionToRead(int totalNumberDimensionToRead) {
    this.totalNumberDimensionToRead = totalNumberDimensionToRead;
  }

  /**
   * @return the totalNumberOfMeasureToRead
   */
  public int getTotalNumberOfMeasureToRead() {
    return totalNumberOfMeasureToRead;
  }

  /**
   * @param totalNumberOfMeasureToRead the totalNumberOfMeasureToRead to set
   */
  public void setTotalNumberOfMeasureToRead(int totalNumberOfMeasureToRead) {
    this.totalNumberOfMeasureToRead = totalNumberOfMeasureToRead;
  }

  /**
   * @return the allSelectedDimensionColumnIndexRange
   */
  public int[][] getAllSelectedDimensionColumnIndexRange() {
    return allSelectedDimensionColumnIndexRange;
  }

  /**
   * @param allSelectedDimensionColumnIndexRange the allSelectedDimensionColumnIndexRange to set
   */
  public void setAllSelectedDimensionColumnIndexRange(int[][] allSelectedDimensionColumnIndexRange)
  {
    this.allSelectedDimensionColumnIndexRange = allSelectedDimensionColumnIndexRange;
  }

  /**
   * @return the allSelectedMeasureIndexRange
   */
  public int[][] getAllSelectedMeasureIndexRange() {
    return allSelectedMeasureIndexRange;
  }

  /**
   * @param allSelectedMeasureIndexRange the allSelectedMeasureIndexRange to set
   */
  public void setAllSelectedMeasureIndexRange(int[][] allSelectedMeasureIndexRange) {
    this.allSelectedMeasureIndexRange = allSelectedMeasureIndexRange;
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
   * @return the dictionaryColumnChunkIndex
   */
  public int[] getDictionaryColumnChunkIndex() {
    return dictionaryColumnChunkIndex;
  }

  /**
   * @param dictionaryColumnChunkIndex the dictionaryColumnChunkIndex to set
   */
  public void setDictionaryColumnChunkIndex(int[] dictionaryColumnChunkIndex) {
    this.dictionaryColumnChunkIndex = dictionaryColumnChunkIndex;
  }

  /**
   * @return the noDictionaryColumnChunkIndexes
   */
  public int[] getNoDictionaryColumnChunkIndexes() {
    return noDictionaryColumnChunkIndexes;
  }

  /**
   * @param noDictionaryColumnChunkIndexes the noDictionaryColumnChunkIndexes to set
   */
  public void setNoDictionaryColumnChunkIndexes(int[] noDictionaryColumnChunkIndexes) {
    this.noDictionaryColumnChunkIndexes = noDictionaryColumnChunkIndexes;
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

  public ProjectionDimension[] getProjectionDimensions() {
    return projectionDimensions;
  }

  public void setProjectionDimensions(ProjectionDimension[] projectionDimensions) {
    this.projectionDimensions = projectionDimensions;
  }

  public ProjectionMeasure[] getProjectionMeasures() {
    return projectionMeasures;
  }

  public void setProjectionMeasures(ProjectionMeasure[] projectionMeasures) {
    this.projectionMeasures = projectionMeasures;
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

  // Return file name and path, like Part0/Segment_0/part-0-0_batchno0-0-1517155583332.carbondata
  public String getBlockIdString() {
    return blockId;
  }

  public void setBlockId(String blockId) {
    this.blockId = blockId;
  }

  public boolean isRestructuredBlock() {
    return isRestructuredBlock;
  }

  public void setRestructuredBlock(boolean restructuredBlock) {
    isRestructuredBlock = restructuredBlock;
  }

  public DimensionInfo getDimensionInfo() {
    return dimensionInfo;
  }

  public void setDimensionInfo(DimensionInfo dimensionInfo) {
    this.dimensionInfo = dimensionInfo;
  }

  public ProjectionDimension[] getActualQueryDimensions() {
    return actualQueryDimensions;
  }

  public void setActualQueryDimensions(ProjectionDimension[] actualQueryDimensions) {
    this.actualQueryDimensions = actualQueryDimensions;
  }

  public ProjectionMeasure[] getActualQueryMeasures() {
    return actualQueryMeasures;
  }

  public void setActualQueryMeasures(ProjectionMeasure[] actualQueryMeasures) {
    this.actualQueryMeasures = actualQueryMeasures;
  }

  public int[] getProjectionListDimensionIndexes() {
    return projectionListDimensionIndexes;
  }

  public void setProjectionListDimensionIndexes(int[] projectionListDimensionIndexes) {
    this.projectionListDimensionIndexes = projectionListDimensionIndexes;
  }

  public int[] getProjectionListMeasureIndexes() {
    return projectionListMeasureIndexes;
  }

  public void setProjectionListMeasureIndexes(int[] projectionListMeasureIndexes) {
    this.projectionListMeasureIndexes = projectionListMeasureIndexes;
  }

  /**
   * @return delete delta files
   */
  public String[] getDeleteDeltaFilePath() {
    return deleteDeltaFilePath;
  }

  /**
   * set the delete delta files
   * @param deleteDeltaFilePath
   */
  public void setDeleteDeltaFilePath(String[] deleteDeltaFilePath) {
    this.deleteDeltaFilePath = deleteDeltaFilePath;
  }

  /**
   * @return deleted record map
   */
  public Map<String, DeleteDeltaVo> getDeletedRecordsMap() {
    return deletedRecordsMap;
  }

  /**
   * @param deletedRecordsMap
   */
  public void setDeletedRecordsMap(Map<String, DeleteDeltaVo> deletedRecordsMap) {
    this.deletedRecordsMap = deletedRecordsMap;
  }

  public boolean isPrefetchBlocklet() {
    return prefetchBlocklet;
  }

  public void setPrefetchBlocklet(boolean prefetchBlocklet) {
    this.prefetchBlocklet = prefetchBlocklet;
  }

  public boolean isRequiredRowId() {
    return requiredRowId;
  }

  public void setRequiredRowId(boolean requiredRowId) {
    this.requiredRowId = requiredRowId;
  }

  public QueryStatisticsModel getQueryStatisticsModel() {
    return queryStatisticsModel;
  }

  public void setQueryStatisticsModel(QueryStatisticsModel queryStatisticsModel) {
    this.queryStatisticsModel = queryStatisticsModel;
  }

  public boolean isDirectVectorFill() {
    return isDirectVectorFill && !isRestructuredBlock;
  }

  public void setDirectVectorFill(boolean directVectorFill) {
    isDirectVectorFill = directVectorFill;
  }
}
