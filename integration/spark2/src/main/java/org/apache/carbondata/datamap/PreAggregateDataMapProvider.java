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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateTableHelper;
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand;
import scala.Some;

@InterfaceAudience.Internal
public class PreAggregateDataMapProvider implements DataMapProvider {
  protected PreAggregateTableHelper helper;
  protected CarbonDropTableCommand dropTableCommand;

  @Override
  public void initMeta(CarbonTable mainTable, DataMapSchema dataMapSchema, String ctasSqlStatement,
      SparkSession sparkSession) throws MalformedDataMapCommandException {
    validateDmProperty(dataMapSchema);
    helper = new PreAggregateTableHelper(
        mainTable, dataMapSchema.getDataMapName(), dataMapSchema.getProviderName(),
        dataMapSchema.getProperties(), ctasSqlStatement, null, false);
    helper.initMeta(sparkSession);
  }

  private void validateDmProperty(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    if (!dataMapSchema.getProperties().isEmpty()) {
      if (dataMapSchema.getProperties().size() > 1 ||
          !dataMapSchema.getProperties().containsKey(DataMapProperty.PATH)) {
        throw new MalformedDataMapCommandException(
            "Only 'path' dmproperty is allowed for this datamap");
      }
    }
  }

  @Override
  public void initData(CarbonTable mainTable, SparkSession sparkSession) {
    // Nothing is needed to do by default
  }

  @Override
  public void freeMeta(CarbonTable mainTable, DataMapSchema dataMapSchema,
      SparkSession sparkSession) {
    dropTableCommand = new CarbonDropTableCommand(
        true,
        new Some<>(dataMapSchema.getRelationIdentifier().getDatabaseName()),
        dataMapSchema.getRelationIdentifier().getTableName(),
        true);
    dropTableCommand.processMetadata(sparkSession);
  }

  @Override
  public void freeData(CarbonTable mainTable, DataMapSchema dataMapSchema,
      SparkSession sparkSession) {
    if (dropTableCommand != null) {
      dropTableCommand.processData(sparkSession);
    }
  }

  @Override
  public void rebuild(CarbonTable mainTable, SparkSession sparkSession) {
    if (helper != null) {
      helper.initData(sparkSession);
    }
  }

  @Override
  public void incrementalBuild(CarbonTable mainTable, String[] segmentIds,
      SparkSession sparkSession) {
    throw new UnsupportedOperationException();
  }
}
