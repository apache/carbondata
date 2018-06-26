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
package org.apache.carbondata.core.scan.executor.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.SearchModeResultIterator;
import org.apache.carbondata.core.util.CarbonProperties;


public class SearchModeDetailQueryExecutor extends AbstractQueryExecutor<Object> {
  private static final LogService LOGGER =
          LogServiceFactory.getLogService(SearchModeDetailQueryExecutor.class.getName());
  private static ExecutorService executorService = null;

  public SearchModeDetailQueryExecutor() {
    if (executorService == null) {
      initThreadPool();
    }
  }

  private static synchronized void initThreadPool() {
    int defaultValue = Runtime.getRuntime().availableProcessors();
    int nThread;
    try {
      nThread = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_SEARCH_MODE_SCAN_THREAD,
              String.valueOf(defaultValue)));
    } catch (NumberFormatException e) {
      nThread = defaultValue;
      LOGGER.warn("The carbon.search.mode.thread is invalid. Using the default value " + nThread);
    }
    if (nThread > 0) {
      executorService = Executors.newFixedThreadPool(nThread);
    } else {
      executorService = Executors.newCachedThreadPool();
    }
  }

  public static synchronized void shutdownThreadPool() {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  @Override
  public CarbonIterator<Object> execute(QueryModel queryModel)
      throws QueryExecutionException, IOException {
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);

    this.queryIterator = new SearchModeResultIterator(
        blockExecutionInfoList,
        queryModel,
        executorService
    );
    return this.queryIterator;
  }

}
