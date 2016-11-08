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
package org.apache.carbondata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.hadoop.internal.index.Block;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Carbon input split to allow distributed read of CarbonInputFormat.
 */
public class CarbonInputSplit extends FileSplit implements Serializable, Writable, Block {

  private static final long serialVersionUID = 3520344046772190207L;
  private String segmentId;
  /**
   * Number of BlockLets in a block
   */
  private int numberOfBlocklets = 0;

  public CarbonInputSplit() {
    super(null, 0, 0, new String[0]);
  }

  public CarbonInputSplit(String segmentId, Path path, long start, long length,
      String[] locations) {
    super(path, start, length, locations);
    this.segmentId = segmentId;
  }

  public CarbonInputSplit(String segmentId, Path path, long start, long length,
      String[] locations, int numberOfBlocklets) {
    this(segmentId, path, start, length, locations);
    this.numberOfBlocklets = numberOfBlocklets;
  }

  public static CarbonInputSplit from(String segmentId, FileSplit split) throws IOException {
    return new CarbonInputSplit(segmentId, split.getPath(), split.getStart(), split.getLength(),
        split.getLocations());
  }

  public String getSegmentId() {
    return segmentId;
  }

  @Override public void readFields(DataInput in) throws IOException {

    super.readFields(in);
    this.segmentId = in.readUTF();

  }

  @Override public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(segmentId);
  }

  /**
   * returns the number of blocklets
   * @return
   */
  public int getNumberOfBlocklets() {
    return numberOfBlocklets;
  }

  @Override
  public String getBlockPath() {
    return getPath().getName();
  }

  @Override
  public List<Long> getMatchedBlocklets() {
    return null;
  }

  @Override
  public boolean fullScan() {
    return true;
  }
}
