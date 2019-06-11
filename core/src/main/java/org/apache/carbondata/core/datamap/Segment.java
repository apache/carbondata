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
package org.apache.carbondata.core.datamap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;

/**
 * Represents one load of carbondata
 */
public class Segment implements Serializable, Writable {

  private static final long serialVersionUID = 7044555408162234064L;

  private String segmentNo;

  private String segmentFileName;

  /**
   * List of index shards which are already got filtered through CG index operation.
   */
  private Set<String> filteredIndexShardNames = new HashSet<>();

  /**
   * Points to the Read Committed Scope of the segment. This is a flavor of
   * transactional isolation level which only allows snapshot read of the
   * data and make non committed data invisible to the reader.
   */
  private transient ReadCommittedScope readCommittedScope;

  /**
   * keeps all the details about segments
   */
  private transient LoadMetadataDetails loadMetadataDetails;

  private String segmentString;

  private long indexSize = 0;

  public Segment() {

  }

  public Segment(String segmentNo) {
    this.segmentNo = segmentNo;
  }

  public Segment(String segmentNo, ReadCommittedScope readCommittedScope) {
    this.segmentNo = segmentNo;
    this.readCommittedScope = readCommittedScope;
    segmentString = segmentNo;
  }

  /**
   * ReadCommittedScope will be null. So getCommittedIndexFile will not work and will throw
   * a NullPointerException. In case getCommittedIndexFile is need to be accessed then
   * use the other constructor and pass proper ReadCommittedScope.
   * @param segmentNo
   * @param segmentFileName
   */
  public Segment(String segmentNo, String segmentFileName) {
    this.segmentNo = segmentNo;
    this.segmentFileName = segmentFileName;
    this.readCommittedScope = null;
    if (segmentFileName != null) {
      segmentString = segmentNo + "#" + segmentFileName;
    } else {
      segmentString = segmentNo;
    }
  }

  /**
   *
   * @param segmentNo
   * @param segmentFileName
   * @param readCommittedScope
   */
  public Segment(String segmentNo, String segmentFileName, ReadCommittedScope readCommittedScope) {
    this.segmentNo = segmentNo;
    this.segmentFileName = segmentFileName;
    this.readCommittedScope = readCommittedScope;
    if (segmentFileName != null) {
      segmentString = segmentNo + "#" + segmentFileName;
    } else {
      segmentString = segmentNo;
    }
  }

  /**
   * @param segmentNo
   * @param segmentFileName
   * @param readCommittedScope
   */
  public Segment(String segmentNo, String segmentFileName, ReadCommittedScope readCommittedScope,
      LoadMetadataDetails loadMetadataDetails) {
    this.segmentNo = segmentNo;
    this.segmentFileName = segmentFileName;
    this.readCommittedScope = readCommittedScope;
    this.loadMetadataDetails = loadMetadataDetails;
    if (loadMetadataDetails.getIndexSize() != null) {
      this.indexSize = Long.parseLong(loadMetadataDetails.getIndexSize());
    }
    if (segmentFileName != null) {
      segmentString = segmentNo + "#" + segmentFileName;
    } else {
      segmentString = segmentNo;
    }
  }

  /**
   *
   * @return map of Absolute path of index file as key and null as value -- without mergeIndex
   * map of AbsolutePath with fileName of MergeIndex parent file as key and mergeIndexFileName
   *                                                             as value -- with mergeIndex
   * @throws IOException
   */
  public Map<String, String> getCommittedIndexFile() throws IOException {
    return readCommittedScope.getCommittedIndexFile(this);
  }

  public SegmentRefreshInfo getSegmentRefreshInfo(UpdateVO updateVo)
      throws IOException {
    return readCommittedScope.getCommittedSegmentRefreshInfo(this, updateVo);
  }

  public String getSegmentNo() {
    return segmentNo;
  }

  public String getSegmentFileName() {
    return segmentFileName;
  }

  public void setReadCommittedScope(ReadCommittedScope readCommittedScope) {
    this.readCommittedScope = readCommittedScope;
  }

  public ReadCommittedScope getReadCommittedScope() {
    return readCommittedScope;
  }

  public static List<Segment> toSegmentList(String[] segmentIds,
      ReadCommittedScope readCommittedScope) {
    List<Segment> list = new ArrayList<>(segmentIds.length);
    for (String segmentId : segmentIds) {
      list.add(toSegment(segmentId, readCommittedScope));
    }
    return list;
  }

  public static List<Segment> toSegmentList(List<String> segmentIds,
      ReadCommittedScope readCommittedScope) {
    List<Segment> list = new ArrayList<>(segmentIds.size());
    for (String segmentId : segmentIds) {
      list.add(toSegment(segmentId, readCommittedScope));
    }
    return list;
  }

  /**
   * readCommittedScope provide Read Snapshot isolation.
   * @param segmentId
   * @param readCommittedScope
   * @return
   */
  public static Segment toSegment(String segmentId, ReadCommittedScope readCommittedScope) {
    // SegmentId can be combination of segmentNo and segmentFileName.
    String[] split = segmentId.split("#");
    if (split.length > 1) {
      return new Segment(split[0], split[1], readCommittedScope);
    } else if (split.length > 0) {
      return new Segment(split[0], null, readCommittedScope);
    }
    return new Segment(segmentId, null, readCommittedScope);
  }

  /**
   * Converts to segment object
   * @param segmentId
   * @return
   */
  public static Segment toSegment(String segmentId) {
    // SegmentId can be combination of segmentNo and segmentFileName.
    return toSegment(segmentId, null);
  }

  /**
   * Read the table status and get the segment corresponding to segmentNo
   * @param segmentNo
   * @param tablePath
   * @return
   */
  public static Segment getSegment(String segmentNo, String tablePath) {
    LoadMetadataDetails[] loadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(tablePath));
    return getSegment(segmentNo, loadMetadataDetails);
  }

  /**
   * Get the segment object corresponding to segmentNo
   * @param segmentNo
   * @param loadMetadataDetails
   * @return
   */
  public static Segment getSegment(String segmentNo, LoadMetadataDetails[] loadMetadataDetails) {
    for (LoadMetadataDetails details: loadMetadataDetails) {
      if (details.getLoadName().equals(segmentNo)) {
        return new Segment(details.getLoadName(), details.getSegmentFile());
      }
    }
    return null;
  }

  public Configuration getConfiguration() {
    return readCommittedScope.getConfiguration();
  }

  public Set<String> getFilteredIndexShardNames() {
    return filteredIndexShardNames;
  }

  public void setFilteredIndexShardName(String filteredIndexShardName) {
    this.filteredIndexShardNames.add(filteredIndexShardName);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Segment segment = (Segment) o;
    return Objects.equals(segmentNo, segment.segmentNo);
  }

  @Override public int hashCode() {
    return Objects.hash(segmentNo);
  }

  @Override public String toString() {
    return segmentString;
  }

  public LoadMetadataDetails getLoadMetadataDetails() {
    return loadMetadataDetails;
  }

  public long getIndexSize() {
    return indexSize;
  }

  public void setIndexSize(long indexSize) {
    this.indexSize = indexSize;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeUTF(segmentNo);
    boolean writeSegmentFileName = segmentFileName != null;
    out.writeBoolean(writeSegmentFileName);
    if (writeSegmentFileName) {
      out.writeUTF(segmentFileName);
    }
    out.writeInt(filteredIndexShardNames.size());
    for (String name: filteredIndexShardNames) {
      out.writeUTF(name);
    }
    if (segmentString == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(segmentString);
    }
    out.writeLong(indexSize);
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.segmentNo = in.readUTF();
    if (in.readBoolean()) {
      this.segmentFileName = in.readUTF();
    }
    filteredIndexShardNames = new HashSet<>();
    int indexShardNameSize = in.readInt();
    for (int i = 0; i < indexShardNameSize; i++) {
      filteredIndexShardNames.add(in.readUTF());
    }
    if (in.readBoolean()) {
      this.segmentString = in.readUTF();
    }
    this.indexSize = in.readLong();
  }
}
