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

package org.apache.carbondata.hadoop.internal.segment.impl.zk;

import java.io.IOException;

import org.apache.carbondata.hadoop.internal.segment.Segment;
import org.apache.carbondata.hadoop.internal.segment.SegmentManager;

/**
 * This class leverage Zookeeper and HDFS file to manage segments.
 * Zookeeper is used for locking, and HDFS is for storing the state of segments.
 */
public class ZkSegmentManager implements SegmentManager {
  @Override
  public Segment[] getAllValidSegments() {
    return new Segment[0];
  }

  @Override
  public Segment openNewSegment() throws IOException {
    return null;
  }

  @Override
  public void commitSegment(Segment segment) throws IOException {

  }

  @Override
  public void closeSegment(Segment segment) throws IOException {

  }

  @Override
  public void deleteSegment(Segment segment) throws IOException {

  }
}
