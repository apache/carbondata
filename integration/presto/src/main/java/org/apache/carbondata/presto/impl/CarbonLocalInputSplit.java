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

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;

import org.apache.hadoop.fs.Path;

/**
 * CarbonLocalInputSplit represents a block, it contains a set of blocklet.
 */
public class CarbonLocalInputSplit {

  private static final long serialVersionUID = 3520344046772190207L;
  private String segmentId;
  private String path;
  private long start; // the start offset of the block in a carbondata file.
  private long length; // the length of the block.
  private List<String> locations;// locations are the locations for different replicas.
  private short version;
  private String[] deleteDeltaFiles;
  private String blockletId;
  private String detailInfo;
  private int fileFormatOrdinal;
  private FileFormat fileFormat;

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

  @JsonProperty public String[] getDeleteDeltaFiles() {
    return deleteDeltaFiles;
  }

  @JsonProperty public String getDetailInfo() {
    return detailInfo;
  }

  @JsonProperty public String getBlockletId() {
    return blockletId;
  }

  @JsonProperty public int getFileFormatOrdinal() {
    return fileFormatOrdinal;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setDetailInfo(BlockletDetailInfo blockletDetailInfo) {
    Gson gson = new Gson();
    detailInfo = gson.toJson(blockletDetailInfo);

  }

  @JsonCreator public CarbonLocalInputSplit(@JsonProperty("segmentId") String segmentId,
      @JsonProperty("path") String path, @JsonProperty("start") long start,
      @JsonProperty("length") long length, @JsonProperty("locations") List<String> locations,
      @JsonProperty("numberOfBlocklets") int numberOfBlocklets/*,
                                 @JsonProperty("tableBlockInfo") TableBlockInfo tableBlockInfo*/,
      @JsonProperty("version") short version,
      @JsonProperty("deleteDeltaFiles") String[] deleteDeltaFiles,
      @JsonProperty("blockletId") String blockletId,
      @JsonProperty("detailInfo") String detailInfo,
      @JsonProperty("fileFormatOrdinal") int fileFormatOrdinal
  ) {
    this.path = path;
    this.start = start;
    this.length = length;
    this.segmentId = segmentId;
    this.locations = locations;
    this.numberOfBlocklets = numberOfBlocklets;
    //this.tableBlockInfo = tableBlockInfo;
    this.version = version;
    this.deleteDeltaFiles = deleteDeltaFiles;
    this.blockletId = blockletId;
    this.detailInfo = detailInfo;
    this.fileFormatOrdinal = fileFormatOrdinal;
    this.fileFormat = FileFormat.getByOrdinal(fileFormatOrdinal);
  }

  public static CarbonInputSplit convertSplit(CarbonLocalInputSplit carbonLocalInputSplit) {
    CarbonInputSplit inputSplit = new CarbonInputSplit(carbonLocalInputSplit.getSegmentId(),
        carbonLocalInputSplit.getBlockletId(), new Path(carbonLocalInputSplit.getPath()),
        carbonLocalInputSplit.getStart(), carbonLocalInputSplit.getLength(),
        carbonLocalInputSplit.getLocations()
            .toArray(new String[carbonLocalInputSplit.getLocations().size()]),
        carbonLocalInputSplit.getNumberOfBlocklets(),
        ColumnarFormatVersion.valueOf(carbonLocalInputSplit.getVersion()),
        carbonLocalInputSplit.getDeleteDeltaFiles());
    inputSplit.setFormat(carbonLocalInputSplit.getFileFormat());
    if (FileFormat.COLUMNAR_V3.ordinal() == inputSplit.getFileFormat().ordinal()) {
      Gson gson = new Gson();
      BlockletDetailInfo blockletDetailInfo =
          gson.fromJson(carbonLocalInputSplit.detailInfo, BlockletDetailInfo.class);
      if (null == blockletDetailInfo) {
        throw new RuntimeException("Could not read blocklet details");
      }
      try {
        blockletDetailInfo.readColumnSchema(blockletDetailInfo.getColumnSchemaBinary());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      inputSplit.setDetailInfo(blockletDetailInfo);
    }
    return inputSplit;
  }


}
