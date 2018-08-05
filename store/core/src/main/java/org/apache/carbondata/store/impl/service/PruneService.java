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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.impl.service.model.PruneRequest;
import org.apache.carbondata.store.impl.service.model.PruneResponse;

import org.apache.hadoop.ipc.VersionedProtocol;

@InterfaceAudience.Internal
public interface PruneService extends VersionedProtocol {
  long versionID = 1L;

  /**
   * Leveraging index and segment information to skip blocks,
   * return a list of block eligible for scanning
   *
   * @param request prune request containing table identifier
   *                and filter expression
   * @return prune result containing blocks to scan
   * @throws CarbonException if any error occurs
   */
  PruneResponse prune(PruneRequest request) throws CarbonException;
}
