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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Represents one load of carbondata
 */
public class Segment implements Serializable {

  private static final long serialVersionUID = 7044555408162234064L;

  private String segmentNo;

  private String segmentFileName;

  public Segment(String segmentNo, String segmentFileName) {
    this.segmentNo = segmentNo;
    this.segmentFileName = segmentFileName;
  }

  public String getSegmentNo() {
    return segmentNo;
  }

  public String getSegmentFileName() {
    return segmentFileName;
  }

  public static List<Segment> toSegmentList(String[] segmentIds) {
    List<Segment> list = new ArrayList<>(segmentIds.length);
    for (String segmentId : segmentIds) {
      list.add(toSegment(segmentId));
    }
    return list;
  }

  public static List<Segment> toSegmentList(List<String> segmentIds) {
    List<Segment> list = new ArrayList<>(segmentIds.size());
    for (String segmentId : segmentIds) {
      list.add(toSegment(segmentId));
    }
    return list;
  }

  /**
   * SegmentId can be combination of segmentNo and segmentFileName
   * @param segmentId
   * @return
   */
  public static Segment toSegment(String segmentId) {
    String[] split = segmentId.split("#");
    if (split.length > 1) {
      return new Segment(split[0], split[1]);
    } else if (split.length > 0) {
      return new Segment(split[0], null);
    }
    return new Segment(segmentId, null);
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
    for (LoadMetadataDetails details: loadMetadataDetails) {
      if (details.getLoadName().equals(segmentNo)) {
        return new Segment(details.getLoadName(), details.getSegmentFile());
      }
    }
    return null;
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
}
