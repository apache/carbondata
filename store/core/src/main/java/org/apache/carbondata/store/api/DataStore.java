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
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.store.api.descriptor.LoadDescriptor;
import org.apache.carbondata.store.api.descriptor.SelectDescriptor;
import org.apache.carbondata.store.api.exception.StoreException;

/**
 * Public interface to write and read data in CarbonStore
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface DataStore {

  /**
   * Load data into a Table
   * @param load descriptor for load operation
   * @throws IOException if network or disk IO error occurs
   */
  void loadData(LoadDescriptor load) throws IOException, StoreException;

  /**
   * Scan a Table and return matched rows
   * @param select descriptor for scan operation, including required column, filter, etc
   * @return matched rows
   * @throws IOException if network or disk IO error occurs
   */
  List<CarbonRow> select(SelectDescriptor select) throws IOException, StoreException;
}
