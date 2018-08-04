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

package org.apache.carbondata.store.impl.master;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.impl.BlockScanUnit;
import org.apache.carbondata.store.impl.IndexOperation;
import org.apache.carbondata.store.impl.MetaOperation;
import org.apache.carbondata.store.impl.Schedulable;
import org.apache.carbondata.store.impl.service.PruneService;
import org.apache.carbondata.store.impl.service.model.PruneRequest;
import org.apache.carbondata.store.impl.service.model.PruneResponse;

import org.apache.hadoop.ipc.ProtocolSignature;

public class PruneServiceImpl implements PruneService {

  private StoreConf storeConf;
  private Scheduler scheduler;

  PruneServiceImpl(StoreConf storeConf, Scheduler scheduler) {
    this.storeConf = storeConf;
    this.scheduler = scheduler;
  }

  @Override
  public PruneResponse prune(PruneRequest request) throws CarbonException {
    TableInfo tableInfo = MetaOperation.getTable(request.getTable(), storeConf);
    try {
      List<CarbonInputSplit> splits =
          IndexOperation.pruneBlock(tableInfo, request.getFilterExpression());
      List<ScanUnit> output = splits.stream()
          .map((Function<CarbonInputSplit, ScanUnit>) inputSplit -> {
            Schedulable worker;
            String[] locations;
            try {
              locations = inputSplit.getLocations();
              if (locations.length == 0) {
                worker = scheduler.pickNexWorker();
              } else {
                worker = scheduler.pickWorker(locations[0]);
              }
            } catch (IOException e) {
              // ignore it and pick next worker as no locality
              worker = scheduler.pickNexWorker();
            }
            return new BlockScanUnit(inputSplit, worker);
          }).collect(Collectors.toList());
      return new PruneResponse(output);
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException {
    return null;
  }
}
