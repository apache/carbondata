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
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.sdk.store.service.ServiceFactory;
import org.apache.carbondata.sdk.store.service.StoreService;

/**
 * A CarbonStore that leverage multiple servers via RPC calls (Master and Workers)
 */
@InterfaceAudience.User
public class DistributedCarbonStore implements CarbonStore {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DistributedCarbonStore.class.getCanonicalName());

  protected StoreService storeService;

  public DistributedCarbonStore(StoreConf conf) throws IOException {
    this.storeService =
        ServiceFactory.createStoreService(conf.masterHost(), conf.storeServicePort());
  }

  @Override
  public void createTable(TableDescriptor descriptor) throws CarbonException {
    try {
      storeService.createTable(descriptor);
    } catch (Exception e)  {
      System.out.println(e.getMessage());
    }
  }

  @Override
  public void dropTable(TableIdentifier table) throws CarbonException {
    try {
      storeService.dropTable(table);
    } catch (Exception e)  {
      System.out.println(e.getMessage());
    }
  }

  @Override
  public List<TableDescriptor> listTable() throws CarbonException {
    return storeService.listTable();
  }

  @Override
  public TableDescriptor getDescriptor(TableIdentifier table) throws CarbonException {
    // TableInfo tableInfo = storeService.getTable(table);
    // TODO: create TableDescriptor from table info
    return null;
  }

  @Override
  public void alterTable(TableIdentifier table, TableDescriptor newTable) throws CarbonException {
    storeService.alterTable(table, newTable);
  }

  @Override
  public void loadData(LoadDescriptor load) throws CarbonException {
    storeService.loadData(load);
  }

  @Override
  public void upsert(Iterator<KeyedRow> row, StructType schema) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(Iterator<PrimaryKey> keys) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CarbonRow> scan(ScanDescriptor select) throws CarbonException {
    try {
      return storeService.scan(select);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return null;
    }
  }

  @Override
  public Row lookup(PrimaryKey key) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Row> lookup(TableIdentifier tableIdentifier, String filterExpression)
      throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }
}
