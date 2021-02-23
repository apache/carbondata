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

package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.index.Segment;

import org.apache.hadoop.io.Writable;

public class SegmentWrapperContainer implements Writable {

  private SegmentWrapper[] segmentWrappers;

  public SegmentWrapperContainer(SegmentWrapper[] segmentWrappers) {
    this.segmentWrappers = segmentWrappers;
  }

  public List<Segment> getSegments() {
    List<Segment> segments = new ArrayList<>();
    for (SegmentWrapper segmentWrapper: segmentWrappers) {
      segments.addAll(segmentWrapper.getSegments());
    }
    return segments;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(segmentWrappers.length);
    for (SegmentWrapper segmentWrapper: segmentWrappers) {
      segmentWrapper.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int numOfWrappers = dataInput.readInt();
    segmentWrappers = new SegmentWrapper[numOfWrappers];
    for (int i = 0; i < numOfWrappers; i++) {
      SegmentWrapper segmentWrapper = new SegmentWrapper();
      segmentWrapper.readFields(dataInput);
      segmentWrappers[i] = segmentWrapper;
    }
  }
}