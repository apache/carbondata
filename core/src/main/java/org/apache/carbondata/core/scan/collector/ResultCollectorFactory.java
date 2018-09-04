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
package org.apache.carbondata.core.scan.collector;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.scan.collector.impl.*;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * This class will provide the result collector instance based on the required type
 */
public class ResultCollectorFactory {

  /**
   * logger of result collector factory
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ResultCollectorFactory.class.getName());

  private static boolean isDirectFill = Boolean.parseBoolean(CarbonProperties.getInstance().getProperty("carbon.directfill", "true"));

  /**
   * This method will create result collector based on the given type
   *
   * @param blockExecutionInfo
   * @return
   */
  public static AbstractScannedResultCollector getScannedResultCollector(
      BlockExecutionInfo blockExecutionInfo) {
    AbstractScannedResultCollector scannerResultAggregator = null;
    if (blockExecutionInfo.isRawRecordDetailQuery()) {
      if (blockExecutionInfo.isRestructuredBlock()) {
        if (blockExecutionInfo.isRequiredRowId()) {
          LOGGER.info("RowId Restructure based raw ollector is used to scan and collect the data");
          scannerResultAggregator = new RowIdRestructureBasedRawResultCollector(blockExecutionInfo);
        } else {
          LOGGER.info("Restructure based raw collector is used to scan and collect the data");
          scannerResultAggregator = new RestructureBasedRawResultCollector(blockExecutionInfo);
        }
      } else {
        if (blockExecutionInfo.isRequiredRowId()) {
          LOGGER.info("RowId based raw collector is used to scan and collect the data");
          scannerResultAggregator = new RowIdRawBasedResultCollector(blockExecutionInfo);
        } else {
          LOGGER.info("Row based raw collector is used to scan and collect the data");
          scannerResultAggregator = new RawBasedResultCollector(blockExecutionInfo);
        }
      }
    } else if (blockExecutionInfo.isVectorBatchCollector()) {
      if (blockExecutionInfo.isRestructuredBlock()) {
        LOGGER.info("Restructure dictionary vector collector is used to scan and collect the data");
        scannerResultAggregator = new RestructureBasedVectorResultCollector(blockExecutionInfo);
      } else {
        if (isDirectFill) {
          LOGGER.info("Direct Vector based dictionary collector is used to scan and collect the data");
          scannerResultAggregator = new DirectDictionaryBasedVectorResultCollector(blockExecutionInfo);
        } else {
          LOGGER.info("Vector based dictionary collector is used to scan and collect the data");
          scannerResultAggregator = new DictionaryBasedVectorResultCollector(blockExecutionInfo);
        }
      }
    } else {
      if (blockExecutionInfo.isRestructuredBlock()) {
        LOGGER.info("Restructure based dictionary collector is used to scan and collect the data");
        scannerResultAggregator = new RestructureBasedDictionaryResultCollector(blockExecutionInfo);
      } else if (blockExecutionInfo.isRequiredRowId()) {
        LOGGER.info("RowId based dictionary collector is used to scan and collect the data");
        scannerResultAggregator = new RowIdBasedResultCollector(blockExecutionInfo);
      } else {
        LOGGER.info("Row based dictionary collector is used to scan and collect the data");
        scannerResultAggregator = new DictionaryBasedResultCollector(blockExecutionInfo);
      }
    }
    return scannerResultAggregator;
  }

  private ResultCollectorFactory() {
  }
}
