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

package org.apache.carbondata.hadoop.internal.segment.impl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.hadoop.internal.index.Block;
import org.apache.carbondata.hadoop.internal.index.Index;
import org.apache.carbondata.hadoop.internal.index.IndexLoader;
import org.apache.carbondata.hadoop.internal.segment.Segment;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * This segment is backed by index, thus getSplits can use the index to do file pruning.
 */
public class IndexedSegment extends Segment {

  private IndexLoader loader;

  public IndexedSegment(String name, String path, IndexLoader loader) {
    super(name, path);
    this.loader = loader;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver)
      throws IOException {
    // do as following
    // 1. create the index or get from cache by the filter name in the configuration
    // 2. filter by index to get the filtered block
    // 3. create input split from filtered block

    List<InputSplit> output = new LinkedList<>();
    Index index = loader.load(this);
    List<Block> blocks = index.filter(job, filterResolver);
    for (Block block: blocks) {
      output.add(makeInputSplit(block));
    }
    return output;
  }

  @Override
  public void setupForRead(JobContext job) {

  }

  private InputSplit makeInputSplit(Block block) {
    // TODO: get all required parameter from block
    return null;
  }
}
