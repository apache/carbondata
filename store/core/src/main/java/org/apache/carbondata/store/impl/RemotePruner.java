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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.Pruner;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.impl.service.PruneService;
import org.apache.carbondata.store.impl.service.ServiceFactory;
import org.apache.carbondata.store.impl.service.model.PruneRequest;
import org.apache.carbondata.store.impl.service.model.PruneResponse;

public class RemotePruner implements Pruner {

  private String pruneServiceHost;
  private int pruneServiePort;

  RemotePruner(String pruneServiceHost, int pruneServiePort) {
    this.pruneServiceHost = pruneServiceHost;
    this.pruneServiePort = pruneServiePort;
  }

  @Override
  public List<ScanUnit> prune(TableIdentifier table, Expression filterExpression)
      throws CarbonException {
    try {
      PruneRequest request = new PruneRequest(table, filterExpression);
      PruneService pruneService = ServiceFactory.createPruneService(
          pruneServiceHost, pruneServiePort);
      PruneResponse response = pruneService.prune(request);
      return response.getScanUnits();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }
}
