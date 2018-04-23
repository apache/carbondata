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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.processor.BlockScan;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;

public abstract class AbstractSearchModeResultIterator
        extends AbstractDetailQueryResultIterator<Object> {

  private FileFactory.FileType fileType;
  private List<Future<BlockScan>> taskSubmitList;
  protected BlockScan curBlockScan;
  private int nextBlockScanIndex = 0;

  public AbstractSearchModeResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
                                          ExecutorService execService) {
    super(infos, queryModel, execService);
    this.fileType = FileFactory.getFileType(queryModel.getAbsoluteTableIdentifier().getTablePath());
    scanAll();
  }

  private void scanAll() {
    taskSubmitList = new ArrayList<>(blockExecutionInfos.size());
    for (final BlockExecutionInfo info: blockExecutionInfos) {
      taskSubmitList.add(execService.submit(new Callable<BlockScan>() {

        @Override
        public BlockScan call() throws Exception {
          BlockScan blockScan = new BlockScan(info, FileFactory.getFileHolder(fileType),
              buildQueryStatiticsModel(recorder));
          blockScan.scan();
          return blockScan;
        }
      }));
    }
  }

  @Override
  public boolean hasNext() {
    try {
      while ((curBlockScan == null || !curBlockScan.hasNext()) &&
              nextBlockScanIndex < taskSubmitList.size()) {
        curBlockScan = taskSubmitList.get(nextBlockScanIndex++).get();
      }
      return curBlockScan != null && curBlockScan.hasNext();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private QueryStatisticsModel buildQueryStatiticsModel(QueryStatisticsRecorder recorder) {
    QueryStatisticsModel queryStatisticsModel = new QueryStatisticsModel();
    queryStatisticsModel.setRecorder(recorder);
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

    // dimension filling time
    QueryStatistic keyColumnFilingTime = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.KEY_COLUMN_FILLING_TIME, keyColumnFilingTime);
    queryStatisticsModel.getRecorder().recordStatistics(keyColumnFilingTime);
    // measure filling time
    QueryStatistic measureFilingTime = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.MEASURE_FILLING_TIME, measureFilingTime);
    queryStatisticsModel.getRecorder().recordStatistics(measureFilingTime);
    // page Io Time
    QueryStatistic pageUncompressTime = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.PAGE_UNCOMPRESS_TIME, pageUncompressTime);
    queryStatisticsModel.getRecorder().recordStatistics(pageUncompressTime);
    // result preparation time
    QueryStatistic resultPreparationTime = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.RESULT_PREP_TIME, resultPreparationTime);
    queryStatisticsModel.getRecorder().recordStatistics(resultPreparationTime);
    return queryStatisticsModel;
  }
}
