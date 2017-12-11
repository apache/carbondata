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

package org.apache.carbondata.hadoop.ft;

import java.io.File;
import java.io.FileFilter;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.carbondata.core.api.CarbonProperties;
import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.test.util.StoreCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InputFilesTest extends TestCase {
  @Before
  public void setUp() throws Exception {
    CarbonProperties.getInstance().addProperty("carbon.badRecords.location", "/tmp/carbon/badrecords");
    StoreCreator.createCarbonStore();
    // waiting 3s to finish table create and data loading
    Thread.sleep(3000L);
  }

  @Test
  public void testGetSplits() throws Exception {
    CarbonInputFormat carbonInputFormat = new CarbonInputFormat();
    JobConf jobConf = new JobConf(new Configuration());
    Job job = Job.getInstance(jobConf);
    job.getConfiguration().set("query.id", UUID.randomUUID().toString());
    String tblPath = StoreCreator.getAbsoluteTableIdentifier().getTablePath();
    FileInputFormat.addInputPath(job, new Path(tblPath));
    job.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, "0");
    // list files to get the carbondata file
    File segmentDir = new File(tblPath + File.separator + "Fact" + File.separator +
        "Part0" + File.separator + "Segment_0");
    if (segmentDir.exists() && segmentDir.isDirectory()) {
      File[] files = segmentDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.getName().endsWith("carbondata");
        }
      });
      if (files != null && files.length > 0) {
        job.getConfiguration().set(CarbonInputFormat.INPUT_FILES, files[0].getName());
      }
    }
    List splits = carbonInputFormat.getSplits(job);

    Assert.assertTrue(splits != null && splits.size() == 1);
  }
}
