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

package org.apache.carbondata.datamap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapProvider;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateTableHelper;
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand;
import scala.Some;

@InterfaceAudience.Internal
public class PreAggregateDataMapProvider extends DataMapProvider {
  protected PreAggregateTableHelper helper;
  protected CarbonDropTableCommand dropTableCommand;
  protected SparkSession sparkSession;
  private String dbName;
  private String tableName;

  PreAggregateDataMapProvider(CarbonTable table, DataMapSchema schema,
      SparkSession sparkSession) {
    super(table, schema);
    this.sparkSession = sparkSession;
    this.dbName = table.getDatabaseName();
    this.tableName = table.getTableName() + '_' + schema.getDataMapName();
  }

  @Override
  public void initMeta(String ctasSqlStatement) throws MalformedDataMapCommandException {
    DataMapSchema dataMapSchema = getDataMapSchema();
    validateDmProperty(dataMapSchema);
    helper = new PreAggregateTableHelper(
        getMainTable(), dataMapSchema.getDataMapName(), dataMapSchema.getProviderName(),
        dataMapSchema.getProperties(), ctasSqlStatement, null, false);
    helper.initMeta(sparkSession);
  }

  private void validateDmProperty(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    if (!dataMapSchema.getProperties().isEmpty()) {
      Map<String, String> properties = new HashMap<>(dataMapSchema.getProperties());
      properties.remove(DataMapProperty.DEFERRED_REBUILD);
      properties.remove(DataMapProperty.PATH);
      properties.remove(DataMapProperty.PARTITIONING);
      properties.remove(CarbonCommonConstants.LONG_STRING_COLUMNS);
      if (properties.size() > 0) {
        throw new MalformedDataMapCommandException(
                "Only 'path', 'partitioning' and 'long_string_columns' dmproperties " +
                "are allowed for this datamap");
      }
    }
  }

  @Override
  public void cleanMeta() {
    dropTableCommand = new CarbonDropTableCommand(
        true,
        new Some<>(dbName),
        tableName,
        true);
    dropTableCommand.processMetadata(sparkSession);
  }

  @Override
  public void cleanData() {
    if (dropTableCommand != null) {
      dropTableCommand.processData(sparkSession);
    }
  }

  @Override
  public boolean rebuild() {
    if (helper != null) {
      helper.initData(sparkSession);
    }
    return true;
  }

  @Override
  public DataMapFactory getDataMapFactory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportRebuild() {
    return false;
  }

  @Override public boolean rebuildInternal(String newLoadName, Map<String, List<String>> segmentMap,
      CarbonTable carbonTable) {
    return false;
  }
}
