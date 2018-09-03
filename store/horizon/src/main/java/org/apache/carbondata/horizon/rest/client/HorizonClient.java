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

package org.apache.carbondata.horizon.rest.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.DropTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.sdk.store.exception.CarbonException;

/**
 * Client to send REST request to Horizon service
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface HorizonClient extends Closeable {

  /**
   * Create a Table
   * @param create descriptor for create table operation
   * @throws CarbonException if network or disk IO error occurs
   */
  void createTable(CreateTableRequest create) throws CarbonException;

  /**
   * Drop a Table, and remove all data in it
   * @param table table identifier
   * @throws IOException if network or disk IO error occurs
   */
  void dropTable(DropTableRequest drop) throws IOException;

  /**
   * Load data into a Table
   * @param load descriptor for load operation
   * @throws IOException if network or disk IO error occurs
   */
  void loadData(LoadRequest load) throws IOException, CarbonException;

  /**
   * Scan a Table and return matched rows
   * @param select descriptor for scan operation, including required column, filter, etc
   * @return matched rows
   * @throws IOException if network or disk IO error occurs
   */
  List<CarbonRow> select(SelectRequest select) throws IOException, CarbonException;

  /**
   * Executor a SQL statement
   * @param sqlString SQL statement
   * @return matched rows
   * @throws IOException if network or disk IO error occurs
   */
  List<CarbonRow> sql(String sqlString) throws IOException;

}
