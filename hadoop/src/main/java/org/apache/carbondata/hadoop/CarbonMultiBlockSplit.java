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

package org.apache.carbondata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.statusmanager.FileFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This class wraps multiple blocks belong to a same node to one split.
 * So the scanning task will scan multiple blocks. This is an optimization for concurrent query.
 */
public class CarbonMultiBlockSplit extends InputSplit implements Writable {

  /*
   * Splits (HDFS Blocks) for task to scan.
   */
  private List<CarbonInputSplit> splitList;

  /*
   * The locations of all wrapped splits
   */
  private String[] locations;

  private FileFormat fileFormat = FileFormat.carbondata;

  public CarbonMultiBlockSplit() {
    splitList = null;
    locations = null;
  }

  public CarbonMultiBlockSplit(AbsoluteTableIdentifier identifier, List<CarbonInputSplit> splitList,
      String[] locations) throws IOException {
    this.splitList = splitList;
    this.locations = locations;
  }

  public CarbonMultiBlockSplit(AbsoluteTableIdentifier identifier, List<CarbonInputSplit> splitList,
      String[] locations, FileFormat fileFormat) throws IOException {
    this.splitList = splitList;
    this.locations = locations;
    this.fileFormat = fileFormat;
  }

  /**
   * Return all splits for scan
   * @return split list for scan
   */
  public List<CarbonInputSplit> getAllSplits() {
    return splitList;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    long total = 0;
    for (InputSplit split: splitList) {
      total += split.getLength();
    }
    return total;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return locations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write number of splits and then write all splits
    out.writeInt(splitList.size());
    for (CarbonInputSplit split: splitList) {
      split.write(out);
    }
    out.writeInt(locations.length);
    for (int i = 0; i < locations.length; i++) {
      out.writeUTF(locations[i]);
    }
    out.writeInt(fileFormat.ordinal());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read all splits
    int numSplit = in.readInt();
    splitList = new ArrayList<>(numSplit);
    for (int i = 0; i < numSplit; i++) {
      CarbonInputSplit split = new CarbonInputSplit();
      split.readFields(in);
      splitList.add(split);
    }
    int len = in.readInt();
    locations = new String[len];
    for (int i = 0; i < len; i++) {
      locations[i] = in.readUTF();
    }
    fileFormat = FileFormat.getByOrdinal(in.readInt());
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }
}
