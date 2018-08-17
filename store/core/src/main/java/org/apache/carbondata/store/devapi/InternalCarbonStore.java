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

import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.sdk.store.CarbonStore;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;

/**
 * Internal API for engine integration developers
 */
@InterfaceAudience.Developer("Integration")
@InterfaceStability.Unstable
public interface InternalCarbonStore extends CarbonStore {

  /**
   * Get CarbonTable object from the store
   *
   * @param tableIdentifier table identifier
   * @return CarbonTable object
   * @throws CarbonException if any error occurs
   */
  CarbonTable getCarbonTable(TableIdentifier tableIdentifier) throws CarbonException;

  /**
   * Return a new Loader that can be used to load data in distributed compute framework
   * @param load descriptor for load operation
   * @return a new Loader
   * @throws CarbonException if any error occurs
   */
  DataLoader newLoader(LoadDescriptor load) throws CarbonException;

  /**
   * Return a new Scanner that can be used in for parallel scan
   *
   * @param tableIdentifier table to scan
   * @param scanDescriptor parameter for scan, like projection column and filter expression
   * @param scanOption options for scan, use {@link ScanOption} for the map key
   * @param readSupportClass read support class to convert the row to output object
   * @param <T> the target object type contain in {@link ResultBatch}
   * @return a new Scanner
   * @throws CarbonException if any error occurs
   */
  <T> Scanner<T> newScanner(
      TableIdentifier tableIdentifier,
      ScanDescriptor scanDescriptor,
      Map<String, String> scanOption,
      Class<? extends CarbonReadSupport<T>> readSupportClass) throws CarbonException;

}
