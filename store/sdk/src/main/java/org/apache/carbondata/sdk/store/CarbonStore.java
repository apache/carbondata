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

import java.io.Closeable;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.SelectDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;

/**
 * Public Interface of CarbonStore
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface CarbonStore extends Closeable {

  ////////////////////////////////////////////////////////////////////
  /////                  Metadata Operation                      /////
  ////////////////////////////////////////////////////////////////////

  /**
   * Create a Table
   * @param descriptor descriptor for create table operation
   * @throws CarbonException if any error occurs
   */
  void createTable(TableDescriptor descriptor) throws CarbonException;

  /**
   * Drop a Table, and remove all data in it
   * @param table table identifier
   * @throws CarbonException if any error occurs
   */
  void dropTable(TableIdentifier table) throws CarbonException;

  /**
   * @return all table created
   * @throws CarbonException if any error occurs
   */
  List<TableDescriptor> listTable() throws CarbonException;

  /**
   * Return table descriptor by specified identifier
   * @param table table identifier
   * @return table descriptor
   * @throws CarbonException if any error occurs
   */
  TableDescriptor getDescriptor(TableIdentifier table) throws CarbonException;

  /**
   * Alter table operation
   * @param table table identifier
   * @param newTable new table descriptor to alter to
   * @throws CarbonException if any error occurs
   */
  void alterTable(TableIdentifier table, TableDescriptor newTable) throws CarbonException;


  ////////////////////////////////////////////////////////////////////
  /////                     Write Operation                      /////
  ////////////////////////////////////////////////////////////////////

  /**
   * Trigger a Load into the table specified by load descriptor
   * @param load descriptor for load operation
   * @throws CarbonException if any error occurs
   */
  void loadData(LoadDescriptor load) throws CarbonException;

  /**
   * Create a new Loader, which can be used to load data in distributed compute framework
   * @param load descriptor for load operation
   * @return a new Loader
   * @throws CarbonException if any error occurs
   */
  Loader newLoader(LoadDescriptor load) throws CarbonException;

  /**
   * Return true if this table has primary key defined when create table using
   * {@link #createTable(TableDescriptor)}
   *
   * For a such table, {@link #newMutator()} and {@link #newFetcher()} are supported
   *
   * @return true if this table has primary key.
   */
  default boolean isPrimaryKeyDefined() {
    return false;
  }

  /**
   * A mutator supports upsert and delete using primary key
   * @return a new mutator
   * @throws CarbonException if any error occurs
   */
  default Mutator newMutator() throws CarbonException {
    throw new UnsupportedOperationException();
  }


  ////////////////////////////////////////////////////////////////////
  /////                      Read Operation                      /////
  ////////////////////////////////////////////////////////////////////

  /**
   * Scan a Table and return matched rows
   * @param select descriptor for scan operation, including required column, filter, etc
   * @return matched rows
   * @throws CarbonException if any error occurs
   */
  List<CarbonRow> select(SelectDescriptor select) throws CarbonException;

  /**
   * @return a new Scanner
   * @throws CarbonException if any error occurs
   */
  Scanner newScanner() throws CarbonException;

  /**
   * @return a new Fetcher
   * @throws CarbonException if any error occurs
   */
  default Fetcher newFetcher() throws CarbonException {
    throw new UnsupportedOperationException();
  }
}
