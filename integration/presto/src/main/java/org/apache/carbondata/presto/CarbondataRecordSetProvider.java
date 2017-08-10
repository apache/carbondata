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
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;
import static org.apache.carbondata.presto.Types.checkType;

class CarbondataRecordSetProvider implements ConnectorRecordSetProvider {

  private final String connectorId;
  private final CarbonTableReader carbonTableReader;

  @Inject
  public CarbondataRecordSetProvider(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = reader;
  }

  @Override public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
    requireNonNull(split, "split is null");
    requireNonNull(columns, "columns is null");

    CarbondataSplit carbondataSplit =
        checkType(split, CarbondataSplit.class, "split is not class CarbondataSplit");
    checkArgument(carbondataSplit.getConnectorId().equals(connectorId),
        "split is not for this connector");

    StringBuffer targetCols = new StringBuffer();
    // Convert all columns handles
    ImmutableList.Builder<CarbondataColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add(checkType(handle, CarbondataColumnHandle.class, "handle"));
      targetCols.append(((CarbondataColumnHandle) handle).getColumnName());
      targetCols.append(",");
    }

    String projectedColumns;
    // Build column projection(check the column order)
    if (targetCols.length() > 0) {
      projectedColumns = targetCols.substring(0, targetCols.length() - 1);
    } else {
      projectedColumns = null;
    }
    CarbonTableCacheModel tableCacheModel =
        carbonTableReader.getCarbonCache(carbondataSplit.getSchemaTableName());
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.carbonTable, "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.tableInfo, "tableCacheModel.tableInfo should not be null");

    // Build Query Model
    CarbonTable targetTable = tableCacheModel.carbonTable;
    CarbonQueryPlan queryPlan =
        CarbonInputFormatUtil.createQueryPlan(targetTable, projectedColumns);

    QueryModel queryModel = QueryModel
        .createModel(targetTable.getAbsoluteTableIdentifier(), queryPlan, targetTable,
            new DataTypeConverterImpl());

    // Push down filter
    fillFilter2QueryModel(queryModel, targetTable);

    // Return new record set
    return new CarbondataRecordSet(targetTable, session, carbondataSplit, handles.build(),
        queryModel);
  }

  /**
   * Build filter for QueryModel
   *
   * @param queryModel
   * @param carbonTable
   */
  private void fillFilter2QueryModel(QueryModel queryModel, CarbonTable carbonTable) {

    Expression finalFilters =
        PrestoFilterUtil.getFilters(carbonTable.getFactTableName().hashCode());

    CarbonInputFormatUtil.processFilterExpression(finalFilters, carbonTable);
    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    queryModel.setFilterExpressionResolverTree(CarbonInputFormatUtil
        .resolveFilter(finalFilters, queryModel.getAbsoluteTableIdentifier(), tableProvider));
  }
}