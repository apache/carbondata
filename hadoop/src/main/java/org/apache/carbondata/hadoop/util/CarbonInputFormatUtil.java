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

package org.apache.carbondata.hadoop.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonCommonConstantsInternal;
import org.apache.carbondata.core.datamap.DataMapJob;
import org.apache.carbondata.core.datamap.DataMapUtil;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

/**
 * Utility class
 */
public class CarbonInputFormatUtil {

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonProperties.class.getName());

  public static <V> CarbonTableInputFormat<V> createCarbonInputFormat(
      AbsoluteTableIdentifier identifier,
      Job job) throws IOException {
    CarbonTableInputFormat<V> carbonInputFormat = new CarbonTableInputFormat<>();
    CarbonTableInputFormat.setDatabaseName(
        job.getConfiguration(), identifier.getCarbonTableIdentifier().getDatabaseName());
    CarbonTableInputFormat.setTableName(
        job.getConfiguration(), identifier.getCarbonTableIdentifier().getTableName());
    FileInputFormat.addInputPath(job, new Path(identifier.getTablePath()));
    setDataMapJobIfConfigured(job.getConfiguration());
    return carbonInputFormat;
  }

  public static <V> CarbonTableInputFormat<V> createCarbonTableInputFormat(
      AbsoluteTableIdentifier identifier, List<String> partitionId, Job job) throws IOException {
    CarbonTableInputFormat<V> carbonTableInputFormat = new CarbonTableInputFormat<>();
    CarbonTableInputFormat.setPartitionIdList(
        job.getConfiguration(), partitionId);
    CarbonTableInputFormat.setDatabaseName(
        job.getConfiguration(), identifier.getCarbonTableIdentifier().getDatabaseName());
    CarbonTableInputFormat.setTableName(
        job.getConfiguration(), identifier.getCarbonTableIdentifier().getTableName());
    FileInputFormat.addInputPath(job, new Path(identifier.getTablePath()));
    setDataMapJobIfConfigured(job.getConfiguration());
    return carbonTableInputFormat;
  }

  public static <V> CarbonTableInputFormat<V> createCarbonTableInputFormat(
      Job job,
      CarbonTable carbonTable,
      String[] projectionColumns,
      Expression filterExpression,
      List<PartitionSpec> partitionNames,
      DataMapJob dataMapJob) throws IOException, InvalidConfigurationException {
    Configuration conf = job.getConfiguration();
    CarbonInputFormat.setTableInfo(conf, carbonTable.getTableInfo());
    CarbonInputFormat.setDatabaseName(conf, carbonTable.getTableInfo().getDatabaseName());
    CarbonInputFormat.setTableName(conf, carbonTable.getTableInfo().getFactTable().getTableName());
    if (partitionNames != null) {
      CarbonInputFormat.setPartitionsToPrune(conf, partitionNames);
    }
    CarbonInputFormat
        .setTransactionalTable(conf, carbonTable.getTableInfo().isTransactionalTable());
    CarbonProjection columnProjection = new CarbonProjection(projectionColumns);
    return createInputFormat(conf, carbonTable.getAbsoluteTableIdentifier(),
        filterExpression, columnProjection, dataMapJob);
  }

  private static <V> CarbonTableInputFormat<V> createInputFormat(
      Configuration conf,
      AbsoluteTableIdentifier identifier,
      Expression filterExpression,
      CarbonProjection columnProjection,
      DataMapJob dataMapJob) throws InvalidConfigurationException, IOException {
    CarbonTableInputFormat<V> format = new CarbonTableInputFormat<>();
    CarbonInputFormat.setTablePath(
        conf,
        identifier.appendWithLocalPrefix(identifier.getTablePath()));
    CarbonInputFormat.setQuerySegment(conf, identifier);
    CarbonInputFormat.setFilterPredicates(conf, filterExpression);
    CarbonInputFormat.setColumnProjection(conf, columnProjection);
    if (dataMapJob != null) {
      DataMapUtil.setDataMapJob(conf, dataMapJob);
    } else {
      setDataMapJobIfConfigured(conf);
    }
    // when validate segments is disabled in thread local update it to CarbonTableInputFormat
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    if (carbonSessionInfo != null) {
      String tableUniqueKey = identifier.getDatabaseName() + "." + identifier.getTableName();
      String validateInputSegmentsKey = CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
          tableUniqueKey;
      CarbonInputFormat.setValidateSegmentsToAccess(
          conf,
          Boolean.valueOf(carbonSessionInfo.getThreadParams().getProperty(
              validateInputSegmentsKey, "true")));
      String queryOnPreAggStreamingKey = CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING +
          tableUniqueKey;
      boolean queryOnPreAggStreaming = Boolean.valueOf(carbonSessionInfo.getThreadParams()
          .getProperty(queryOnPreAggStreamingKey, "false"));
      String inputSegmentsKey = CarbonCommonConstants.CARBON_INPUT_SEGMENTS + tableUniqueKey;
      CarbonInputFormat.setValidateSegmentsToAccess(conf,
          Boolean.valueOf(carbonSessionInfo.getThreadParams()
              .getProperty(validateInputSegmentsKey, "true")));
      CarbonInputFormat.setQuerySegment(
          conf,
          carbonSessionInfo.getThreadParams().getProperty(
              inputSegmentsKey,
              CarbonProperties.getInstance().getProperty(inputSegmentsKey, "*")));
      if (queryOnPreAggStreaming) {
        CarbonInputFormat.setAccessStreamingSegments(conf, true);
        carbonSessionInfo.getThreadParams().removeProperty(queryOnPreAggStreamingKey);
        carbonSessionInfo.getThreadParams().removeProperty(inputSegmentsKey);
        carbonSessionInfo.getThreadParams().removeProperty(validateInputSegmentsKey);
      }
    }
    return format;
  }

  /**
   * This method set DataMapJob if configured
   *
   * @param conf
   * @throws IOException
   */
  public static void setDataMapJobIfConfigured(Configuration conf) throws IOException {
    String className = "org.apache.carbondata.indexserver.EmbeddedDataMapJob";
    DataMapUtil.setDataMapJob(conf, DataMapUtil.createDataMapJob(className));
  }

  public static String createJobTrackerID(java.util.Date date) {
    return new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(date);
  }

  public static JobID getJobId(java.util.Date date, int batch) {
    String jobtrackerID = createJobTrackerID(date);
    return new JobID(jobtrackerID, batch);
  }

}
