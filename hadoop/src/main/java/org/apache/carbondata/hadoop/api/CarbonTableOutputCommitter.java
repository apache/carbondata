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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;

import org.apache.carbondata.hadoop.internal.segment.Segment;
import org.apache.carbondata.hadoop.internal.segment.SegmentManager;

import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobContext;

public class CarbonTableOutputCommitter extends FileOutputCommitter {
  private SegmentManager segmentManager;
  private Segment newSegment;

  @Override
  public void setupJob(JobContext context) throws IOException {
    // steps:
    // call segment manager to open new segment for loading
    // load data synchronously
    // close the segment and make it available for reading

    newSegment = segmentManager.openNewSegment();
    super.setupJob(context);
  }

  @Override
  public void abortJob(JobContext context, int runState) throws IOException {
    segmentManager.closeSegment(newSegment);
    super.abortJob(context, runState);
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    newSegment.setupForRead(context);
    segmentManager.commitSegment(newSegment);
    super.commitJob(context);
  }
}
