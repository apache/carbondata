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

package org.carbondata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Handles input splits for raw data
 */
public class CarbonRawDataInputSplit extends InputSplit implements Writable {

  long length;
  String[] locations;

  public CarbonRawDataInputSplit(long length, String[] locations) {
    this.length = length;
    this.locations = locations;
  }

  public static CarbonRawDataInputSplit from(FileSplit split) throws IOException {
    return new CarbonRawDataInputSplit(split.getLength(), split.getLocations());
  }

  @Override public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override public String[] getLocations() throws IOException, InterruptedException {
    return locations;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeLong(locations.length);
  }

  @Override public void readFields(DataInput in) throws IOException {

  }
}
