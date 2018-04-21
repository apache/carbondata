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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonCommonConstantsInternal;
import org.apache.carbondata.core.datastore.impl.FileFactory;
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
import org.apache.carbondata.hadoop.api.DataMapJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Utility class
 */
public class CarbonInputFormatUtil {

  public static <V> CarbonTableInputFormat<V> createCarbonInputFormat(
      AbsoluteTableIdentifier identifier,
      Job job) throws IOException {
    CarbonTableInputFormat<V> carbonInputFormat = new CarbonTableInputFormat<>();
    CarbonTableInputFormat.setDatabaseName(
        job.getConfiguration(), identifier.getCarbonTableIdentifier().getDatabaseName());
    CarbonTableInputFormat.setTableName(
        job.getConfiguration(), identifier.getCarbonTableIdentifier().getTableName());
    FileInputFormat.addInputPath(job, new Path(identifier.getTablePath()));
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
    CarbonInputFormat.setUnmanagedTable(conf, carbonTable.getTableInfo().isUnManagedTable());
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
    if (dataMapJob != null &&
        Boolean.valueOf(CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
            CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT))) {
      CarbonInputFormat.setDataMapJob(conf, dataMapJob);
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

  public static String createJobTrackerID(java.util.Date date) {
    return new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(date);
  }

  public static JobID getJobId(java.util.Date date, int batch) {
    String jobtrackerID = createJobTrackerID(date);
    return new JobID(jobtrackerID, batch);
  }

  public static void setS3Configurations(Configuration hadoopConf) {
    FileFactory.getConfiguration()
        .set("fs.s3a.access.key", hadoopConf.get("fs.s3a.access.key", ""));
    FileFactory.getConfiguration()
        .set("fs.s3a.secret.key", hadoopConf.get("fs.s3a.secret.key", ""));
    FileFactory.getConfiguration()
        .set("fs.s3a.endpoint", hadoopConf.get("fs.s3a.endpoint", ""));
    FileFactory.getConfiguration().set(CarbonCommonConstants.S3_ACCESS_KEY,
        hadoopConf.get(CarbonCommonConstants.S3_ACCESS_KEY, ""));
    FileFactory.getConfiguration().set(CarbonCommonConstants.S3_SECRET_KEY,
        hadoopConf.get(CarbonCommonConstants.S3_SECRET_KEY, ""));
    FileFactory.getConfiguration().set(CarbonCommonConstants.S3N_ACCESS_KEY,
        hadoopConf.get(CarbonCommonConstants.S3N_ACCESS_KEY, ""));
    FileFactory.getConfiguration().set(CarbonCommonConstants.S3N_SECRET_KEY,
        hadoopConf.get(CarbonCommonConstants.S3N_SECRET_KEY, ""));
  }
}
