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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentsHolder;

/**
 * Represents one load of carbondata
 */
public class Segment implements Serializable {

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
  private ReadCommittedScope readCommittedScope;

  /**
   * keeps all the details about segments
   */
  private SegmentDetailVO segmentDetailVO;

  public Segment(String segmentNo) {
    this.segmentNo = segmentNo;
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
  }

  /**
   * @param segmentNo
   * @param segmentFileName
   * @param readCommittedScope
   */
  public Segment(String segmentNo, String segmentFileName, ReadCommittedScope readCommittedScope,
      SegmentDetailVO segmentDetailVO) {
    this.segmentNo = segmentNo;
    this.segmentFileName = segmentFileName;
    this.readCommittedScope = readCommittedScope;
    this.segmentDetailVO = segmentDetailVO;
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
   * @param tableIdentifier
   * @return
   */
  public static Segment getSegment(String segmentNo, AbsoluteTableIdentifier tableIdentifier) {
    SegmentsHolder validSegments = new SegmentManager().getValidSegments(tableIdentifier);
    return getSegment(segmentNo, validSegments.getValidSegments());
  }

  /**
   * Get the segment object corresponding to segmentNo
   * @param segmentNo
   * @param segments
   * @return
   */
  public static Segment getSegment(String segmentNo, List<Segment> segments) {
    for (Segment segment: segments) {
      if (segment.getSegmentNo().equals(segmentNo)) {
        return segment;
      }
    }
    return null;
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
    if (segmentFileName != null) {
      return segmentNo + "#" + segmentFileName;
    } else {
      return segmentNo;
    }
  }

  public SegmentDetailVO getSegmentDetailVO() {
    return segmentDetailVO;
  }
}
