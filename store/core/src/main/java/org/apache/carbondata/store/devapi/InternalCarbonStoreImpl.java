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

package org.apache.carbondata.store.devapi;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.sdk.store.DistributedCarbonStore;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.impl.RowScanner;

public class InternalCarbonStoreImpl extends DistributedCarbonStore implements InternalCarbonStore {

  private Map<TableIdentifier, CarbonTable> tableCache = new ConcurrentHashMap<>();
  private StoreConf storeConf;

  InternalCarbonStoreImpl(StoreConf storeConf) throws IOException {
    super(storeConf);
    this.storeConf = storeConf;
  }

  @Override
  public CarbonTable getCarbonTable(TableIdentifier tableIdentifier)
      throws CarbonException {
    CarbonTable carbonTable = tableCache.getOrDefault(
        tableIdentifier, storeService.getTable(tableIdentifier));
    tableCache.putIfAbsent(tableIdentifier, carbonTable);
    return carbonTable;
  }

  @Override
  public Loader newLoader(LoadDescriptor load) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Scanner<T> newRowScanner(TableIdentifier identifier, CarbonReadSupport<T> readSupport)
      throws CarbonException {
    return new RowScanner<>(storeConf, getCarbonTable(identifier), readSupport);
  }

}
