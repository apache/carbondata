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
package org.apache.carbondata.core.scan.result.iterator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataRefNode;
import org.apache.carbondata.core.mutate.DeleteDeltaVo;
import org.apache.carbondata.core.reader.CarbonDeleteFilesDataReader;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DeleteDeltaInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.processor.DataBlockIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public abstract class AbstractDetailQueryResultIterator<E> extends CarbonIterator<E> {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDetailQueryResultIterator.class.getName());

  private static final Map<DeleteDeltaInfo, Object> deleteDeltaToLockObjectMap =
      new ConcurrentHashMap<>();

  private ExecutorService execService;
  /**
   * execution info of the block
   */
  private List<BlockExecutionInfo> blockExecutionInfos;

  /**
   * file reader which will be used to execute the query
   */
  protected FileReader fileReader;

  DataBlockIterator dataBlockIterator;

  /**
   * QueryStatisticsRecorder
   */
  private QueryStatisticsRecorder recorder;
  /**
   * number of cores which can be used
   */
  private int batchSize;
  /**
   * queryStatisticsModel to store query statistics object
   */
  private QueryStatisticsModel queryStatisticsModel;

  AbstractDetailQueryResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
      ExecutorService execService) {
    String batchSizeString =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE);
    if (null != batchSizeString) {
      try {
        batchSize = Integer.parseInt(batchSizeString);
      } catch (NumberFormatException ne) {
        LOGGER.error("Invalid inmemory records size. Using default value");
        batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
      }
    } else {
      batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
    }
    this.recorder = queryModel.getStatisticsRecorder();
    this.blockExecutionInfos = infos;
    this.fileReader = FileFactory.getFileHolder(
        FileFactory.getFileType(queryModel.getAbsoluteTableIdentifier().getTablePath()));
    this.fileReader.setReadPageByPage(queryModel.isReadPageByPage());
    this.execService = execService;
    intialiseInfos();
    initQueryStatiticsModel();
  }

  private void intialiseInfos() {
    for (BlockExecutionInfo blockInfo : blockExecutionInfos) {
      Map<String, DeleteDeltaVo> deletedRowsMap = null;
      DataRefNodeFinder finder = new BTreeDataRefNodeFinder(blockInfo.getEachColumnValueSize(),
          blockInfo.getDataBlock().getSegmentProperties().getNumberOfSortColumns(),
          blockInfo.getDataBlock().getSegmentProperties().getNumberOfNoDictSortColumns());
      // if delete delta file is present
      if (null != blockInfo.getDeleteDeltaFilePath() && 0 != blockInfo
          .getDeleteDeltaFilePath().length) {
        DeleteDeltaInfo deleteDeltaInfo = new DeleteDeltaInfo(blockInfo.getDeleteDeltaFilePath());
        // read and get the delete detail block details
        deletedRowsMap = getDeleteDeltaDetails(blockInfo.getDataBlock(), deleteDeltaInfo);
        // set the deleted row to block execution info
        blockInfo.setDeletedRecordsMap(deletedRowsMap);
      }
      DataRefNode dataRefNode = blockInfo.getDataBlock().getDataRefNode();
      if (dataRefNode instanceof BlockletDataRefNode) {
        BlockletDataRefNode node = (BlockletDataRefNode) dataRefNode;
        blockInfo.setFirstDataBlock(node);
        blockInfo.setNumberOfBlockToScan(node.numberOfNodes());
      } else {
        DataRefNode startDataBlock =
            finder.findFirstDataBlock(dataRefNode, blockInfo.getStartKey());
        while (startDataBlock.nodeIndex() < blockInfo.getStartBlockletIndex()) {
          startDataBlock = startDataBlock.getNextDataRefNode();
        }
        long numberOfBlockToScan = blockInfo.getNumberOfBlockletToScan();
        //if number of block is less than 0 then take end block.
        if (numberOfBlockToScan <= 0) {
          DataRefNode endDataBlock = finder.findLastDataBlock(dataRefNode, blockInfo.getEndKey());
          numberOfBlockToScan = endDataBlock.nodeIndex() - startDataBlock.nodeIndex() + 1;
        }
        blockInfo.setFirstDataBlock(startDataBlock);
        blockInfo.setNumberOfBlockToScan(numberOfBlockToScan);
      }
    }
  }

  /**
   * Below method will be used to get the delete delta rows for a block
   *
   * @param dataBlock       data block
   * @param deleteDeltaInfo delete delta info
   * @return blockid+pageid to deleted row mapping
   */
  private Map<String, DeleteDeltaVo> getDeleteDeltaDetails(AbstractIndex dataBlock,
      DeleteDeltaInfo deleteDeltaInfo) {
    // if datablock deleted delta timestamp is more then the current delete delta files timestamp
    // then return the current deleted rows
    if (dataBlock.getDeleteDeltaTimestamp() >= deleteDeltaInfo
        .getLatestDeleteDeltaFileTimestamp()) {
      return dataBlock.getDeletedRowsMap();
    }
    CarbonDeleteFilesDataReader carbonDeleteDeltaFileReader = null;
    // get the lock object so in case of concurrent query only one task will read the delete delta
    // files other tasks will wait
    Object lockObject = deleteDeltaToLockObjectMap.get(deleteDeltaInfo);
    // if lock object is null then add a lock object
    if (null == lockObject) {
      synchronized (deleteDeltaToLockObjectMap) {
        // double checking
        lockObject = deleteDeltaToLockObjectMap.get(deleteDeltaInfo);
        if (null == lockObject) {
          lockObject = new Object();
          deleteDeltaToLockObjectMap.put(deleteDeltaInfo, lockObject);
        }
      }
    }
    // double checking to check the deleted rows is already present or not
    if (dataBlock.getDeleteDeltaTimestamp() < deleteDeltaInfo.getLatestDeleteDeltaFileTimestamp()) {
      // if not then acquire the lock
      synchronized (lockObject) {
        // check the timestamp again
        if (dataBlock.getDeleteDeltaTimestamp() < deleteDeltaInfo
            .getLatestDeleteDeltaFileTimestamp()) {
          // read the delete delta files
          carbonDeleteDeltaFileReader = new CarbonDeleteFilesDataReader();
          Map<String, DeleteDeltaVo> deletedRowsMap = carbonDeleteDeltaFileReader
              .getDeletedRowsDataVo(deleteDeltaInfo.getDeleteDeltaFile());
          setDeltedDeltaBoToDataBlock(deleteDeltaInfo, deletedRowsMap, dataBlock);
          // remove the lock
          deleteDeltaToLockObjectMap.remove(deleteDeltaInfo);
          return deletedRowsMap;
        } else {
          return dataBlock.getDeletedRowsMap();
        }
      }
    } else {
      return dataBlock.getDeletedRowsMap();
    }
  }

  /**
   * Below method will be used to set deleted records map to data block
   * based on latest delta file timestamp
   *
   * @param deleteDeltaInfo
   * @param deletedRecordsMap
   * @param dataBlock
   */
  private void setDeltedDeltaBoToDataBlock(DeleteDeltaInfo deleteDeltaInfo,
      Map<String, DeleteDeltaVo> deletedRecordsMap, AbstractIndex dataBlock) {
    // check if timestamp of data block is less than the latest delete delta timestamp
    // then update the delete delta details and timestamp in data block
    if (dataBlock.getDeleteDeltaTimestamp() < deleteDeltaInfo.getLatestDeleteDeltaFileTimestamp()) {
      synchronized (dataBlock) {
        if (dataBlock.getDeleteDeltaTimestamp() < deleteDeltaInfo
            .getLatestDeleteDeltaFileTimestamp()) {
          dataBlock.setDeletedRowsMap(deletedRecordsMap);
          dataBlock.setDeleteDeltaTimestamp(deleteDeltaInfo.getLatestDeleteDeltaFileTimestamp());
        }
      }
    }
  }

  @Override
  public boolean hasNext() {
    if ((dataBlockIterator != null && dataBlockIterator.hasNext())) {
      return true;
    } else if (blockExecutionInfos.size() > 0) {
      return true;
    } else {
      return false;
    }
  }

  void updateDataBlockIterator() {
    if (dataBlockIterator == null || !dataBlockIterator.hasNext()) {
      dataBlockIterator = getDataBlockIterator();
      while (dataBlockIterator != null && !dataBlockIterator.hasNext()) {
        dataBlockIterator = getDataBlockIterator();
      }
    }
  }

  private DataBlockIterator getDataBlockIterator() {
    if (blockExecutionInfos.size() > 0) {
      BlockExecutionInfo executionInfo = blockExecutionInfos.get(0);
      blockExecutionInfos.remove(executionInfo);
      return new DataBlockIterator(executionInfo, fileReader, batchSize, queryStatisticsModel,
          execService);
    }
    return null;
  }

  private void initQueryStatiticsModel() {
    this.queryStatisticsModel = new QueryStatisticsModel();
    this.queryStatisticsModel.setRecorder(recorder);
    QueryStatistic queryStatisticTotalBlocklet = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, queryStatisticTotalBlocklet);
    queryStatisticsModel.getRecorder().recordStatistics(queryStatisticTotalBlocklet);

    QueryStatistic queryStatisticValidScanBlocklet = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, queryStatisticValidScanBlocklet);
    queryStatisticsModel.getRecorder().recordStatistics(queryStatisticValidScanBlocklet);

    QueryStatistic totalNumberOfPages = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.TOTAL_PAGE_SCANNED, totalNumberOfPages);
    queryStatisticsModel.getRecorder().recordStatistics(totalNumberOfPages);

    QueryStatistic validPages = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.VALID_PAGE_SCANNED, validPages);
    queryStatisticsModel.getRecorder().recordStatistics(validPages);

    QueryStatistic scannedPages = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.PAGE_SCANNED, scannedPages);
    queryStatisticsModel.getRecorder().recordStatistics(scannedPages);

    QueryStatistic scanTime = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.SCAN_BLOCKlET_TIME, scanTime);
    queryStatisticsModel.getRecorder().recordStatistics(scanTime);

    QueryStatistic readTime = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.READ_BLOCKlET_TIME, readTime);
    queryStatisticsModel.getRecorder().recordStatistics(readTime);
  }

  public void processNextBatch(CarbonColumnarBatch columnarBatch) {
    throw new UnsupportedOperationException("Please use VectorDetailQueryResultIterator");
  }

  @Override public void close() {
    if (null != dataBlockIterator) {
      dataBlockIterator.close();
    }
    try {
      fileReader.finish();
    } catch (IOException e) {
      LOGGER.error(e);
    }
  }

}
