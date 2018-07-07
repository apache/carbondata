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

package org.apache.carbondata.store.api;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.store.api.descriptor.TableDescriptor;
import org.apache.carbondata.store.api.descriptor.TableIdentifier;
import org.apache.carbondata.store.api.exception.StoreException;

/**
 * Public interface to manage table in CarbonStore
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface MetaStore {
  /**
   * Create a Table
   * @param table descriptor for create table operation
   * @throws IOException if network or disk IO error occurs
   */
  void createTable(TableDescriptor table) throws IOException, StoreException;

  /**
   * Drop a Table, and remove all data in it
   * @param table table identifier
   * @throws IOException if network or disk IO error occurs
   */
  void dropTable(TableIdentifier table) throws IOException;

  CarbonTable getTable(TableIdentifier table) throws IOException;
}
