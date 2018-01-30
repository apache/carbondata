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

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.service.impl.PathFactory;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;
import static org.apache.carbondata.presto.Types.checkType;

public class CarbondataRecordSetProvider implements ConnectorRecordSetProvider {

  private final String connectorId;
  private final CarbonTableReader carbonTableReader;

  @Inject
  public CarbondataRecordSetProvider(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = reader;
  }

  @Override public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {

    CarbondataSplit carbondataSplit =
        checkType(split, CarbondataSplit.class, "split is not class CarbondataSplit");
    checkArgument(carbondataSplit.getConnectorId().equals(connectorId),
        "split is not for this connector");

    CarbonProjection carbonProjection = new CarbonProjection();
    // Convert all columns handles
    ImmutableList.Builder<CarbondataColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add(checkType(handle, CarbondataColumnHandle.class, "handle"));
      carbonProjection.addColumn(((CarbondataColumnHandle) handle).getColumnName());
    }

    CarbonTableCacheModel tableCacheModel =
        carbonTableReader.getCarbonCache(carbondataSplit.getSchemaTableName());
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.carbonTable, "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.tableInfo, "tableCacheModel.tableInfo should not be null");

    // Build Query Model
    CarbonTable targetTable = tableCacheModel.carbonTable;

    QueryModel queryModel ;
    try {
      Configuration conf = new Configuration();
      conf.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
      String carbonTablePath = PathFactory.getInstance()
          .getCarbonTablePath(targetTable.getAbsoluteTableIdentifier(), null).getPath();

      conf.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
      queryModel = CarbonInputFormat.createQueryModel(conf);
      queryModel.setVectorReader(true);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get the Query Model ", e);
    }
    return new CarbondataRecordSet(carbondataSplit, handles.build(),
        queryModel);
  }

}
