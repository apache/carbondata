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

import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateTableHelper;
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil;
import scala.Some;
import scala.Tuple2;

@InterfaceAudience.Internal
public class TimeseriesDataMapProvider extends PreAggregateDataMapProvider {

  TimeseriesDataMapProvider(CarbonTable mainTable, DataMapSchema dataMapSchema,
      SparkSession sparkSession) {
    super(mainTable, dataMapSchema, sparkSession);
  }

  @Override
  public void initMeta(String ctasSqlStatement) {
    DataMapSchema dataMapSchema = getDataMapSchema();
    CarbonTable mainTable = getMainTable();
    Map<String, String> dmProperties = dataMapSchema.getProperties();
    String dmProviderName = dataMapSchema.getProviderName();
    TimeSeriesUtil.validateTimeSeriesGranularity(dmProperties, dmProviderName);
    Tuple2<String, String> details =
        TimeSeriesUtil.getTimeSeriesGranularityDetails(dmProperties, dmProviderName);
    dmProperties.remove(details._1());
    helper = new PreAggregateTableHelper(
        mainTable, dataMapSchema.getDataMapName(), dataMapSchema.getProviderName(),
        dmProperties, ctasSqlStatement, new Some(details._1()), false);
    helper.initMeta(sparkSession);
  }

}
