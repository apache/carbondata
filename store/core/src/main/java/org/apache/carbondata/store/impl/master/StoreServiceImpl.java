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

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.sdk.store.service.StoreService;
import org.apache.carbondata.store.impl.LocalCarbonStore;
import org.apache.carbondata.store.impl.MetaOperation;

import org.apache.hadoop.ipc.ProtocolSignature;

public class StoreServiceImpl implements StoreService {

  // TODO: simple implementation, load and scan inside master
  private LocalCarbonStore localStore;

  private MetaOperation metaOperation;

  StoreServiceImpl(StoreConf storeConf) {
    localStore = new LocalCarbonStore(storeConf);
    metaOperation = new MetaOperation(storeConf);
  }

  @Override
  public void createTable(TableDescriptor descriptor) throws CarbonException {
    metaOperation.createTable(descriptor);
  }

  @Override
  public void dropTable(TableIdentifier identifier) throws CarbonException {
    metaOperation.dropTable(identifier);
  }

  @Override
  public TableInfo getTable(TableIdentifier identifier) throws CarbonException {
    return metaOperation.getTable(identifier);
  }

  @Override
  public List<TableDescriptor> listTable() throws CarbonException {
    return metaOperation.listTable();
  }

  @Override
  public TableDescriptor getDescriptor(TableIdentifier identifier) throws CarbonException {
    return metaOperation.getDescriptor(identifier);
  }

  @Override
  public void alterTable(TableIdentifier identifier, TableDescriptor newTable)
      throws CarbonException {
    metaOperation.alterTable(identifier, newTable);
  }

  @Override
  public void loadData(LoadDescriptor loadDescriptor) throws CarbonException {
    localStore.loadData(loadDescriptor);
  }

  @Override
  public List<CarbonRow> scan(ScanDescriptor scanDescriptor) throws CarbonException {
    return localStore.scan(scanDescriptor);
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
