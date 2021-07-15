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

package org.apache.carbondata.acid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;

public class SegmentsHolder implements Serializable {

  //TODO: chnage serial ID for all newly added classes
  private static final long serialVersionUID = 3232635206406651621L;

  private List<SegmentDetailVO> allSegments;
  private List<SegmentDetailVO> listOfValidSegments;
  private List<SegmentDetailVO> listOfInvalidSegments;
  private List<SegmentDetailVO> listOfStreamSegments;
  private List<SegmentDetailVO> listOfInProgressSegments;
  private ReadCommittedScope committedScope;

  public SegmentsHolder(List<SegmentDetailVO> allSegments) {
    this.allSegments = allSegments;
    this.listOfValidSegments = new ArrayList<>();
    this.listOfInvalidSegments = new ArrayList<>();
    this.listOfStreamSegments = new ArrayList<>();
    this.listOfInProgressSegments = new ArrayList<>();
    process();
  }

  public SegmentsHolder(List<SegmentDetailVO> allSegments, ReadCommittedScope readCommittedScope) {
    this(allSegments);
    this.committedScope = readCommittedScope;
  }

  private void process() {
    //just directly iterate Array
    for (SegmentDetailVO segment : allSegments) {
      if (SegmentStatus.SUCCESS.toString().equals(segment.getStatus())
          || SegmentStatus.MARKED_FOR_UPDATE.toString().equals(segment.getStatus())
          || SegmentStatus.LOAD_PARTIAL_SUCCESS.toString().equals(segment.getStatus())
          || SegmentStatus.STREAMING.toString().equals(segment.getStatus())
          || SegmentStatus.STREAMING_FINISH.toString().equals(segment.getStatus())) {
        // TODO Should we check for merged segments? isn't they compacted already?
        if (SegmentStatus.STREAMING.toString().equals(segment.getStatus())
            || SegmentStatus.STREAMING_FINISH.toString().equals(segment.getStatus())) {
          listOfStreamSegments.add(segment);
          continue;
        }
        listOfValidSegments.add(segment);
      } else if ((SegmentStatus.LOAD_FAILURE.toString().equals(segment.getStatus())
          || SegmentStatus.COMPACTED.toString().equals(segment.getStatus())
          || SegmentStatus.MARKED_FOR_DELETE.toString().equals(segment.getStatus()))) {
        listOfInvalidSegments.add(segment);
      } else if (SegmentStatus.INSERT_IN_PROGRESS.toString().equals(segment.getStatus())
          || SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString().equals(segment.getStatus())) {
        listOfInProgressSegments.add(segment);
      }
    }
  }

  private List<Segment> getSegments(List<SegmentDetailVO> detailVOS,
      ReadCommittedScope readCommittedScope) {
    List<Segment> segments = new ArrayList<>();
    /*for (SegmentDetailVO detailVO : detailVOS) {
      segments.add(
          new Segment(detailVO.getSegmentId(), detailVO.getSegmentFileName(), readCommittedScope,
              detailVO));
    }*/
    return segments;
  }

  public void setReadCommittedScope(ReadCommittedScope committedScope) {
    this.committedScope = committedScope;
  }

  public List<Segment> getInvalidSegments() {
    return getSegments(listOfInvalidSegments, committedScope);
  }

  public List<Segment> getValidSegments() {
    return getSegments(listOfValidSegments, committedScope);
  }

  public List<SegmentDetailVO> getValidSegmentDetailVOs() {
    return listOfValidSegments;
  }

  public List<SegmentDetailVO> getInValidSegmentDetailVOs() {
    return listOfInvalidSegments;
  }

  public List<Segment> getStreamSegments() {
    return getSegments(listOfStreamSegments, committedScope);
  }

  public List<Segment> getListOfInProgressSegments() {
    return getSegments(listOfInProgressSegments, committedScope);
  }

  public List<SegmentDetailVO> getAllSegments() {
    return allSegments;
  }
}