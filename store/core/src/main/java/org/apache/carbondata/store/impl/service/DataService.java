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

package org.apache.carbondata.store.impl.service;

import java.io.Closeable;

import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.impl.service.model.BaseResponse;
import org.apache.carbondata.store.impl.service.model.LoadDataRequest;
import org.apache.carbondata.store.impl.service.model.ScanRequest;
import org.apache.carbondata.store.impl.service.model.ScanResponse;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface DataService extends VersionedProtocol, Closeable {

  long versionID = 1L;

  /**
   * Load data into a Table
   * @param load descriptor for load operation
   * @throws CarbonException if any error occurs
   */
  BaseResponse loadData(LoadDataRequest load) throws CarbonException;

  /**
   * Scan a Table and return matched rows
   * @param scan descriptor for scan operation, including required column, filter, etc
   * @return matched rows
   * @throws CarbonException if any error occurs
   */
  ScanResponse scan(ScanRequest scan) throws CarbonException;

}
