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

package org.apache.carbondata.presto;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;

public class CarbondataConnector implements Connector {

  private static final Logger log = Logger.get(CarbondataConnector.class);

  private final LifeCycleManager lifeCycleManager;
  private final ConnectorMetadata metadata;
  private final ConnectorSplitManager splitManager;
  private final ClassLoader classLoader;
  private final ConnectorPageSourceProvider pageSourceProvider;

  public CarbondataConnector(LifeCycleManager lifeCycleManager, ConnectorMetadata metadata,
      ConnectorSplitManager splitManager,
      ClassLoader classLoader, ConnectorPageSourceProvider pageSourceProvider) {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.classLoader = requireNonNull(classLoader, "classLoader is null");
    this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
  }

  @Override public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
      boolean readOnly) {
    checkConnectorSupports(READ_COMMITTED, isolationLevel);
    return new CarbondataTransactionHandle();
  }

  @Override public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
    return metadata;
  }

  @Override public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }


  @Override
  public ConnectorPageSourceProvider getPageSourceProvider()
  {
    return pageSourceProvider;
  }

  @Override public final void shutdown() {
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      log.error(e, "Error shutting down connector");
    }
  }
}
