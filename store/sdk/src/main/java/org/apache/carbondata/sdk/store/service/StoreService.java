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

package org.apache.carbondata.sdk.store.service;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;

import org.apache.hadoop.ipc.VersionedProtocol;

@InterfaceAudience.Internal
public interface StoreService extends VersionedProtocol {
  long versionID = 1L;

  void createTable(TableDescriptor descriptor) throws CarbonException;

  void dropTable(TableIdentifier table) throws CarbonException;

  TableInfo getTable(TableIdentifier table) throws CarbonException;

  List<TableDescriptor> listTable() throws CarbonException;

  TableDescriptor getDescriptor(TableIdentifier table) throws CarbonException;

  void alterTable(TableIdentifier table, TableDescriptor newTable) throws CarbonException;

  void loadData(LoadDescriptor loadDescriptor) throws CarbonException;

  List<CarbonRow> scan(ScanDescriptor scanDescriptor) throws CarbonException;

}
