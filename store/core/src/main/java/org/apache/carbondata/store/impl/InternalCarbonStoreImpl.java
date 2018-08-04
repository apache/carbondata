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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.sdk.store.DistributedCarbonStore;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.DataLoader;
import org.apache.carbondata.store.devapi.DataScanner;
import org.apache.carbondata.store.devapi.InternalCarbonStore;
import org.apache.carbondata.store.devapi.Pruner;
import org.apache.carbondata.store.devapi.ResultBatch;
import org.apache.carbondata.store.devapi.ScanOption;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.devapi.Scanner;

/**
 * This store does prune and scan either remotely by sending RPC to Master/Worker
 * or in local JVM, depends on parameter passed.
 */
@InterfaceAudience.Internal
public class InternalCarbonStoreImpl extends DistributedCarbonStore implements InternalCarbonStore {

  private Map<TableIdentifier, CarbonTable> tableCache = new ConcurrentHashMap<>();
  private StoreConf storeConf;

  public InternalCarbonStoreImpl(StoreConf storeConf) throws IOException {
    super(storeConf);
    this.storeConf = storeConf;
  }

  @Override
  public CarbonTable getCarbonTable(TableIdentifier tableIdentifier)
      throws CarbonException {
    Objects.requireNonNull(tableIdentifier);
    CarbonTable carbonTable = tableCache.getOrDefault(
        tableIdentifier,
        CarbonTable.buildFromTableInfo(storeService.getTable(tableIdentifier)));
    tableCache.putIfAbsent(tableIdentifier, carbonTable);
    return carbonTable;
  }

  @Override
  public DataLoader newLoader(LoadDescriptor load) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  /**
   * By default, it returns a Scanner that does prune and scan remotely
   */
  @Override
  public <T> Scanner<T> newScanner(TableIdentifier identifier, ScanDescriptor scanDescriptor,
      Map<String, String> scanOption, CarbonReadSupport<T> readSupport)
      throws CarbonException {
    Objects.requireNonNull(identifier);
    Objects.requireNonNull(scanDescriptor);
    if (scanOption == null) {
      return new RemoteScanner<>(storeConf, getCarbonTable(identifier), scanDescriptor, scanOption,
          readSupport);
    }
    TableInfo tableInfo = MetaOperation.getTable(identifier, storeConf);
    boolean isRemotePrune = ScanOption.isRemotePrune(scanOption);
    boolean isOpPushdown = ScanOption.isOperatorPushdown(scanOption);

    Pruner pruner;
    DataScanner<T> scanner;

    if (isRemotePrune) {
      pruner = new RemotePruner(storeConf.masterHost(), storeConf.pruneServicePort());
    } else {
      pruner = new LocalPruner(storeConf);
    }
    if (isOpPushdown) {
      scanner = new RemoteDataScanner<>(tableInfo, scanDescriptor, scanOption, readSupport);
    } else {
      scanner = new LocalDataScanner<>(storeConf, scanDescriptor, scanOption);
    }

    return new Scanner<T>() {
      @Override public List<ScanUnit> prune(TableIdentifier table, Expression filterExpression)
          throws CarbonException {
        return pruner.prune(table, filterExpression);
      }

      @Override public Iterator<? extends ResultBatch<T>> scan(ScanUnit input)
          throws CarbonException {
        return scanner.scan(input);
      }
    };
  }

}
