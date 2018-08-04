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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.ResultBatch;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.devapi.Scanner;
import org.apache.carbondata.store.impl.service.DataService;
import org.apache.carbondata.store.impl.service.PruneService;
import org.apache.carbondata.store.impl.service.ServiceFactory;
import org.apache.carbondata.store.impl.service.model.PruneRequest;
import org.apache.carbondata.store.impl.service.model.PruneResponse;
import org.apache.carbondata.store.impl.service.model.ScanRequest;
import org.apache.carbondata.store.impl.service.model.ScanResponse;

/**
 * This Scanner executes pruning and scanning in remote Master and Worker
 */
public class RemoteScanner<T> implements Scanner<T> {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RemoteScanner.class.getCanonicalName());

  private TableInfo tableInfo;
  private String pruneServiceHost;
  private int pruneServiePort;
  private ScanDescriptor scanDescriptor;
  private Map<String, String> scanOption;
  private CarbonReadSupport<T> readSupport;

  RemoteScanner(StoreConf conf, CarbonTable carbonTable, ScanDescriptor scanDescriptor,
      Map<String, String> scanOption, CarbonReadSupport<T> readSupport) {
    this.tableInfo = carbonTable.getTableInfo();
    this.pruneServiceHost = conf.masterHost();
    this.pruneServiePort = conf.pruneServicePort();
    this.scanDescriptor = scanDescriptor;
    this.scanOption = scanOption;
    this.readSupport = readSupport;
  }

  @Override
  public List<ScanUnit> prune(TableIdentifier table, Expression filterExpression)
      throws CarbonException {
    try {
      PruneRequest request = new PruneRequest(table, filterExpression);
      PruneService pruneService = ServiceFactory.createPruneService(
          pruneServiceHost, pruneServiePort);
      PruneResponse response = pruneService.prune(request);
      return response.getScanUnits();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @Override
  public Iterator<? extends ResultBatch<T>> scan(ScanUnit input) throws CarbonException {
    List<CarbonInputSplit> toBeScan = new ArrayList<>();
    if (input instanceof BlockScanUnit) {
      toBeScan.add(((BlockScanUnit) input).getInputSplit());
    } else {
      throw new CarbonException(input.getClass().getName() + " is not supported");
    }
    int queryId = new Random().nextInt();
    CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(toBeScan, input.preferredLocations());
    try {
      ScanRequest request = new ScanRequest(queryId, split, tableInfo,
          scanDescriptor.getProjection(), scanDescriptor.getFilter(), scanDescriptor.getLimit());
      DataService dataService =
          DataServicePool.getOrCreateDataService(((BlockScanUnit) input).getSchedulable());
      ScanResponse response = dataService.scan(request);
      List<T> rows = Arrays.stream(response.getRows())
          .map(row -> readSupport.readRow(row))
          .collect(Collectors.toList());

      return Collections.singletonList(new RowMajorResultBatch<T>(rows)).iterator();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

}
