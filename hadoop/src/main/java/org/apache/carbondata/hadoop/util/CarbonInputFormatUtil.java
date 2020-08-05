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
import java.util.Date;
import java.util.Locale;

import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
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
    setIndexJobIfConfigured(job.getConfiguration());
    return carbonInputFormat;
  }

  /**
   * This method set IndexJob if configured
   */
  public static void setIndexJobIfConfigured(Configuration conf) throws IOException {
    String className = "org.apache.carbondata.indexserver.EmbeddedIndexJob";
    IndexUtil.setIndexJob(conf, IndexUtil.createIndexJob(className));
  }

  public static String createJobTrackerID() {
    return new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date());
  }

  public static JobID getJobId(int batch) {
    String jobTrackerID = createJobTrackerID();
    return new JobID(jobTrackerID, batch);
  }
}
