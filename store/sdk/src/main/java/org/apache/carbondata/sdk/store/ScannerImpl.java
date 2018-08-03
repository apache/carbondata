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

package org.apache.carbondata.sdk.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.sdk.store.service.DataService;
import org.apache.carbondata.sdk.store.service.PruneService;
import org.apache.carbondata.sdk.store.service.ServiceFactory;
import org.apache.carbondata.sdk.store.service.model.PruneRequest;
import org.apache.carbondata.sdk.store.service.model.PruneResponse;
import org.apache.carbondata.sdk.store.service.model.ScanRequest;
import org.apache.carbondata.sdk.store.service.model.ScanResponse;

import org.apache.hadoop.conf.Configuration;

class ScannerImpl implements Scanner {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ScannerImpl.class.getCanonicalName());

  private TableInfo tableInfo;
  private String pruneServiceHost;
  private int pruneServiePort;

  ScannerImpl(StoreConf conf, TableInfo tableInfo) throws IOException {
    this.tableInfo = tableInfo;
    this.pruneServiceHost = conf.masterHost();
    this.pruneServiePort = conf.pruneServicePort();
  }

  /**
   * Trigger a RPC to Carbon Master to do pruning
   * @param table table identifier
   * @param filterExpression expression of filter predicate given by user
   * @return list of ScanUnit
   * @throws CarbonException if any error occurs
   */
  @Override
  public List<ScanUnit> prune(TableIdentifier table, Expression filterExpression)
      throws CarbonException {
    try {
      Configuration configuration = new Configuration();
      CarbonInputFormat.setTableName(configuration, table.getTableName());
      CarbonInputFormat.setDatabaseName(configuration, table.getDatabaseName());
      CarbonInputFormat.setFilterPredicates(configuration, filterExpression);
      PruneRequest request = new PruneRequest(configuration);
      PruneService pruneService = ServiceFactory.createPruneService(
          pruneServiceHost, pruneServiePort);
      PruneResponse response = pruneService.prune(request);
      return response.getScanUnits();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  /**
   * Execute scan by firing RPC call to worker, return the result rows
   * @param input one scan unit
   * @param descriptor parameter for scan
   * @param option scan options
   * @return result batch
   * @throws CarbonException  if any error occurs
   */
  @Override
  public Iterator<? extends ResultBatch<CarbonRow>> scan(ScanUnit input, ScanDescriptor descriptor,
      SelectOption option) throws CarbonException {
    List<CarbonInputSplit> toBeScan = new ArrayList<>();
    if (input instanceof BlockScanUnit) {
      toBeScan.add(((BlockScanUnit) input).getInputSplit());
    } else {
      throw new CarbonException(input.getClass().getName() + " is not supported");
    }
    int queryId = new Random().nextInt();
    CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(toBeScan, input.preferredLocations());
    try {
      ScanRequest request = new ScanRequest(queryId, split, tableInfo, descriptor.getProjection(),
          descriptor.getFilter(), descriptor.getLimit());
      DataService dataService =
          DataServicePool.getOrCreateDataService(((BlockScanUnit) input).getSchedulable());
      ScanResponse response = dataService.scan(request);
      List<CarbonRow> rows = Arrays.stream(response.getRows())
          .map(CarbonRow::new)
          .collect(Collectors.toList());

      return Collections.singletonList(new RowMajorResultBatch(rows)).iterator();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

}
