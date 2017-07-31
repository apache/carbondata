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

package org.apache.carbondata.hadoop.internal.segment;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Within a carbon table, each data load becomes one Segment,
 * which stores all data files belong to this load in the segment folder.
 */
public abstract class Segment {

  protected String id;

  /**
   * Path of the segment folder
   */
  private String path;

  public Segment(String id, String path) {
    this.id = id;
    this.path = path;
  }

  public String getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  /**
   * get all files, implementation may use the input filter and index to prune files
   * @param job job context
   * @param filterResolver filter
   * @return all files
   */
  public abstract List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver)
      throws IOException;

  /**
   * This is called after Segment is loaded and before segment is committed,
   * implementation should load indices if required
   * @param job job context
   */
  public abstract void setupForRead(JobContext job) throws IOException;

}
