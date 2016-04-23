package org.carbondata.query.carbon.executor.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BtreeDataRefNodeFinder;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.InternalQueryExecutor;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.processor.ScannedResultMerger;
import org.carbondata.query.carbon.processor.impl.SortedScannedResultMerger;
import org.carbondata.query.carbon.processor.impl.UnSortedScannedResultMerger;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.util.CarbonEngineLogEvent;

public abstract class AbstractInternalQueryExecutor implements InternalQueryExecutor {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractInternalQueryExecutor.class.getName());

  /**
   * Executor Service.
   */
  protected ExecutorService execService;

  /**
   * number of cores to be used
   */
  protected int numberOfCores;

  /**
   * Below method will be used to used to execute the detail query
   * and it will return iterator over result
   *
   * @param executionInfos block execution info which will have all the properties
   *                       required for query execution
   * @param sliceIndexes   slice indexes to be executed in this case it w
   * @return query result
   */
  @Override public CarbonIterator<Result> executeQuery(
      List<BlockExecutionInfo> tableBlockExecutionInfosList, int[] sliceIndex)
      throws QueryExecutionException {

    long startTime = System.currentTimeMillis();
    BlockExecutionInfo latestInfo =
        tableBlockExecutionInfosList.get(tableBlockExecutionInfosList.size() - 1);
    execService = Executors.newFixedThreadPool(numberOfCores);
    ScannedResultMerger scannedResultProcessor = null;
    if (latestInfo.getSortInfo().getSortDimensionIndex().length > 0) {
      scannedResultProcessor = new SortedScannedResultMerger(latestInfo, numberOfCores);
    } else {
      scannedResultProcessor = new UnSortedScannedResultMerger(latestInfo, numberOfCores);
    }
    try {
      for (BlockExecutionInfo blockInfo : tableBlockExecutionInfosList) {
        DataRefNodeFinder finder = new BtreeDataRefNodeFinder(blockInfo.getEachColumnValueSize());
        DataRefNode startDataBlock =
            finder.findFirstDataBlock(blockInfo.getFirstDataBlock(), blockInfo.getStartKey());
        DataRefNode endDataBlock =
            finder.findLastDataBlock(blockInfo.getFirstDataBlock(), blockInfo.getEndKey());
        long numberOfBlockToScan = endDataBlock.nodeNumber() - startDataBlock.nodeNumber() + 1;
        blockInfo.setFirstDataBlock(startDataBlock);
        blockInfo.setNumberOfBlockToScan(numberOfBlockToScan);
        blockInfo.setScannedResultProcessor(scannedResultProcessor);
        execService.submit(new QueryRunner(blockInfo));
      }
      execService.shutdown();
      execService.awaitTermination(2, TimeUnit.DAYS);
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Total time taken for scan " + (System.currentTimeMillis() - startTime));
      return scannedResultProcessor.getQueryResultIterator();
    } catch (QueryExecutionException e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e, e.getMessage());
      throw new QueryExecutionException(e);
    } catch (InterruptedException e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e, e.getMessage());
      throw new QueryExecutionException(e);
    } finally {
      execService = null;
      latestInfo = null;
    }
  }

}
