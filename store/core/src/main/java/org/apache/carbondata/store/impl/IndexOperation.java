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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class IndexOperation {

  /**
   * Prune data by leveraging Carbon's Index
   */
  public static List<CarbonInputSplit> pruneBlock(TableInfo tableInfo, Expression filter)
      throws IOException {
    Objects.requireNonNull(tableInfo);
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableInputFormat format;
    try {
      // We just want to do pruning, so passing empty projection columns
      format = CarbonInputFormatUtil.createCarbonTableInputFormat(
          job, table, new String[0], filter, null, null, true);
    } catch (InvalidConfigurationException e) {
      throw new IOException(e.getMessage());
    }

    // We will do FG pruning in reader side, so don't do it here
    CarbonInputFormat.setFgDataMapPruning(job.getConfiguration(), false);
    return format.getSplits(job);
  }
}
