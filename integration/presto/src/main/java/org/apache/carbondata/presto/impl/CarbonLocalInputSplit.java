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

package org.apache.carbondata.presto.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CarbonLocalInputSplit {

  private static final long serialVersionUID = 3520344046772190207L;
  private String segmentId;
  private String path;
  private long start;
  private long length;
  private List<String> locations;
  private short version;
  /**
   * Number of BlockLets in a block
   */
  private int numberOfBlocklets = 0;

  @JsonProperty public short getVersion() {
    return version;
  }

  @JsonProperty public List<String> getLocations() {
    return locations;
  }

  @JsonProperty public long getLength() {
    return length;
  }

  @JsonProperty public long getStart() {
    return start;
  }

  @JsonProperty public String getPath() {
    return path;
  }

  @JsonProperty public String getSegmentId() {
    return segmentId;
  }

  @JsonProperty public int getNumberOfBlocklets() {
    return numberOfBlocklets;
  }

  @JsonCreator public CarbonLocalInputSplit(@JsonProperty("segmentId") String segmentId,
      @JsonProperty("path") String path, @JsonProperty("start") long start,
      @JsonProperty("length") long length, @JsonProperty("locations") List<String> locations,
      @JsonProperty("numberOfBlocklets") int numberOfBlocklets/*,
                                 @JsonProperty("tableBlockInfo") TableBlockInfo tableBlockInfo*/,
      @JsonProperty("version") short version) {
    this.path = path;
    this.start = start;
    this.length = length;
    this.segmentId = segmentId;
    this.locations = locations;
    this.numberOfBlocklets = numberOfBlocklets;
    //this.tableBlockInfo = tableBlockInfo;
    this.version = version;
  }
}
