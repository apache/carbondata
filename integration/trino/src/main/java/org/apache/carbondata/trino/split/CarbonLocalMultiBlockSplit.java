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

package org.apache.carbondata.trino.split;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;

/**
 * CarbonLocalInputSplit represents a block, it contains a set of blocklet.
 */
public class CarbonLocalMultiBlockSplit {

  private static final long serialVersionUID = 3520344046772190207L;

  /*
   * Splits (HDFS Blocks) for task to scan.
   */
  private final List<CarbonLocalInputSplit> splitList;

  /*
   * The locations of all wrapped splits
   */
  private final String[] locations;

  private FileFormat fileFormat = FileFormat.COLUMNAR_V3;

  @JsonCreator
  public CarbonLocalMultiBlockSplit(
      @JsonProperty("splitList") List<CarbonLocalInputSplit> splitList,
      @JsonProperty("locations") String[] locations) {
    this.splitList = splitList;
    this.locations = locations;
    if (!splitList.isEmpty()) {
      this.fileFormat = splitList.get(0).getFileFormat();
    }
  }

  public static CarbonMultiBlockSplit convertSplit(String multiSplitJson) {
    Gson gson = new Gson();
    CarbonLocalMultiBlockSplit carbonLocalMultiBlockSplit =
        gson.fromJson(multiSplitJson, CarbonLocalMultiBlockSplit.class);
    List<CarbonInputSplit> carbonInputSplitList =
        carbonLocalMultiBlockSplit.getSplitList().stream().map(CarbonLocalInputSplit::convertSplit)
            .collect(Collectors.toList());

    CarbonMultiBlockSplit carbonMultiBlockSplit =
        new CarbonMultiBlockSplit(carbonInputSplitList, carbonLocalMultiBlockSplit.getLocations());
    carbonMultiBlockSplit.setFileFormat(carbonLocalMultiBlockSplit.getFileFormat());

    return carbonMultiBlockSplit;
  }

  @JsonProperty
  public String[] getLocations() {
    return locations;
  }

  @JsonProperty
  public List<CarbonLocalInputSplit> getSplitList() {
    return splitList;
  }

  @JsonProperty
  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public String getJsonString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

}
