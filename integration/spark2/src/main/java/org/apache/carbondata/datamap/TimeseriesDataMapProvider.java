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

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.preaaggregate.CarbonCreatePreAggregateTableCommand;
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil;
import scala.Tuple2;

public class TimeseriesDataMapProvider extends PreAggregateDataMapProvider {

  @Override
  public void initMeta(CarbonTable mainTable, DataMapSchema dataMapSchema, String ctasSqlStatement,
      SparkSession sparkSession) {
    Map<String, String> dmProperties = dataMapSchema.getProperties();
    String dmProviderName = dataMapSchema.getClassName();
    TimeSeriesUtil.validateTimeSeriesGranularity(dmProperties, dmProviderName);
    Tuple2<String, String> details =
        TimeSeriesUtil.getTimeSeriesGranularityDetails(dmProperties, dmProviderName);
    dmProperties.remove(details._1());
    createCommand = new CarbonCreatePreAggregateTableCommand(
        mainTable, dataMapSchema.getDataMapName(), dataMapSchema.getClassName(),
        dmProperties, ctasSqlStatement, details._1());
    createCommand.processMetadata(sparkSession);
  }

}
