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

package org.carbondata.query.columnar.scanner;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.columnar.aggregator.ColumnarScannedResultAggregator;
import org.carbondata.query.columnar.datastoreblockprocessor.DataStoreBlockProcessor;
import org.carbondata.query.columnar.scanner.impl.BtreeLeafNodeIterator;
import org.carbondata.query.datastorage.storeinterface.DataStoreBlock;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.executer.impl.RestructureHolder;
import org.carbondata.query.executer.processor.ScannedResultProcessor;
import org.carbondata.query.schema.metadata.ColumnarStorageScannerInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;

public abstract class AbstractColumnarStorageScanner implements ColumnarStorageScanner {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractColumnarStorageScanner.class.getName());

  protected CarbonIterator<DataStoreBlock> leafIterator;

  protected DataStoreBlockProcessor blockProcessor;

  protected ScannedResultProcessor scannedResultProcessor;

  protected RestructureHolder restructurHolder;

  protected ColumnarScannedResultAggregator columnarAggaregator;

  protected BlockDataHolder blockDataHolder;

  private String queryId;

  public AbstractColumnarStorageScanner(ColumnarStorageScannerInfo columnarStorageScannerInfo) {
    leafIterator = new BtreeLeafNodeIterator(columnarStorageScannerInfo.getDatablock(),
        columnarStorageScannerInfo.getTotalNumberOfBlocksToScan());
    this.queryId = columnarStorageScannerInfo.getQueryId();

    this.blockProcessor = columnarStorageScannerInfo.getBlockProcessor();
    this.scannedResultProcessor = columnarStorageScannerInfo.getScannedResultProcessor();
    this.restructurHolder = columnarStorageScannerInfo.getRestructurHolder();
    this.blockDataHolder = new BlockDataHolder(columnarStorageScannerInfo.getDimColumnCount(),
        columnarStorageScannerInfo.getMsrColumnCount());
    this.blockDataHolder.setFileHolder(columnarStorageScannerInfo.getFileHolder());
  }

  protected void finish() {
    try {
      this.scannedResultProcessor.addScannedResult(columnarAggaregator.getResult(restructurHolder));
    } catch (Exception e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
    }
  }
}
