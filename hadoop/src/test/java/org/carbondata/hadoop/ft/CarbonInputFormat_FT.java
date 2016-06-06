/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.hadoop.ft;

import java.util.Arrays;
import java.util.List;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.hadoop.CarbonInputFormat;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.LiteralExpression;
import org.carbondata.query.expression.conditional.EqualToExpression;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonInputFormat_FT extends TestCase {

  @Before public void setUp() throws Exception {
    //create a table with column c1 string type
    //Insert data with column c1 has "a", "b", "c"
  }

  @Test public void testGetSplits() throws Exception {
    CarbonInputFormat carbonInputFormat = new CarbonInputFormat();
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableIdentifier tableIdentifier = new CarbonTableIdentifier("db", "table1");
    FileInputFormat.addInputPath(job, new Path("/opt/carbonstore/"));
    carbonInputFormat.setTableToAccess(job.getConfiguration(), tableIdentifier);
    job.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, "1,2");
    List splits = carbonInputFormat.getSplits(job);

    Assert.assertTrue(splits != null);
    Assert.assertTrue(!splits.isEmpty());
  }

  @Test public void testGetFilteredSplits() throws Exception {
    CarbonInputFormat carbonInputFormat = new CarbonInputFormat();
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableIdentifier tableIdentifier = new CarbonTableIdentifier("db", "table1");
    FileInputFormat.addInputPath(job, new Path("/opt/carbonstore/"));
    carbonInputFormat.setTableToAccess(job.getConfiguration(), tableIdentifier);
    job.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, "1,2");
    Expression expression = new EqualToExpression(new ColumnExpression("c1", DataType.StringType),
        new LiteralExpression("a", DataType.StringType));
    CarbonInputFormat.setFilterPredicates(job.getConfiguration(), expression);
    List splits = carbonInputFormat.getSplits(job);

    Assert.assertTrue(splits != null);
    Assert.assertTrue(!splits.isEmpty());
  }
}
