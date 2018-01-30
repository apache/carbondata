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
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.intf.FilterOptimizer;
import org.apache.carbondata.core.scan.filter.optimizer.RangeFilterOptmizer;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;

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

  public static void processFilterExpression(Expression filterExpression, CarbonTable carbonTable,
      boolean[] isFilterDimensions, boolean[] isFilterMeasures) {
    QueryModel.processFilterExpression(carbonTable, filterExpression, isFilterDimensions,
        isFilterMeasures);

    if (null != filterExpression) {
      // Optimize Filter Expression and fit RANGE filters is conditions apply.
      FilterOptimizer rangeFilterOptimizer =
          new RangeFilterOptmizer(filterExpression);
      rangeFilterOptimizer.optimizeFilter();
    }
  }

  /**
   * Resolve the filter expression.
   *
   * @param filterExpression
   * @param absoluteTableIdentifier
   * @return
   */
  public static FilterResolverIntf resolveFilter(Expression filterExpression,
      AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider) {
    try {
      FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
      //get resolved filter
      return filterExpressionProcessor
          .getFilterResolver(filterExpression, absoluteTableIdentifier, tableProvider);
    } catch (Exception e) {
      throw new RuntimeException("Error while resolving filter expression", e);
    }
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
