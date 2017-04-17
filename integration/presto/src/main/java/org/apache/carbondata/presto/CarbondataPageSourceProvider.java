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

import java.util.List;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Provider Class for Carbondata Page Source class.
 */
public class CarbondataPageSourceProvider implements ConnectorPageSourceProvider {

  private CarbondataRecordSetProvider carbondataRecordSetProvider;

  @Inject
  public CarbondataPageSourceProvider(CarbondataRecordSetProvider carbondataRecordSetProvider)
  {
    this.carbondataRecordSetProvider = requireNonNull(carbondataRecordSetProvider, "recordSetProvider is null");
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
    return new CarbondataPageSource(carbondataRecordSetProvider.getRecordSet(transactionHandle, session, split, columns));
  }
}
