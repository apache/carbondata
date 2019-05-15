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
package org.apache.carbondata.core.readcommitter;

import java.io.IOException;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;

/**
 * ReadCommittedScope for the managed carbon table
 */
@InterfaceAudience.Internal
@InterfaceStability.Stable
public class TableStatusReadCommittedScope implements ReadCommittedScope {

  private static final long serialVersionUID = 2324397174595872738L;
  private LoadMetadataDetails[] loadMetadataDetails;

  private AbsoluteTableIdentifier identifier;

  private transient Configuration configuration;

  public TableStatusReadCommittedScope(AbsoluteTableIdentifier identifier,
      Configuration configuration) throws IOException {
    this.identifier = identifier;
    this.configuration = configuration;
    takeCarbonIndexFileSnapShot();
  }

  public TableStatusReadCommittedScope(AbsoluteTableIdentifier identifier,
      LoadMetadataDetails[] loadMetadataDetails, Configuration configuration) {
    this.identifier = identifier;
    this.configuration = configuration;
    this.loadMetadataDetails = loadMetadataDetails;
  }

  @Override public LoadMetadataDetails[] getSegmentList() throws IOException {
    try {
      if (loadMetadataDetails == null) {
        takeCarbonIndexFileSnapShot();
      }
      return loadMetadataDetails;

    } catch (IOException ex) {
      throw new IOException("Problem encountered while reading the Table Status file.", ex);
    }
  }

  @Override public Map<String, String> getCommittedIndexFile(Segment segment) throws IOException {
    Map<String, String> indexFiles;
    if (segment.getSegmentFileName() == null) {
      String path =
          CarbonTablePath.getSegmentPath(identifier.getTablePath(), segment.getSegmentNo());
      indexFiles = new SegmentIndexFileStore().getMergeOrIndexFilesFromSegment(path);
    } else {
      SegmentFileStore fileStore =
          new SegmentFileStore(identifier.getTablePath(), segment.getSegmentFileName());
      indexFiles = fileStore.getIndexOrMergeFiles();
    }
    return indexFiles;
  }

  public SegmentRefreshInfo getCommittedSegmentRefreshInfo(Segment segment, UpdateVO updateVo)
      throws IOException {
    SegmentRefreshInfo segmentRefreshInfo;
    if (updateVo != null) {
      segmentRefreshInfo = new SegmentRefreshInfo(updateVo.getCreatedOrUpdatedTimeStamp(), 0);
    } else {
      segmentRefreshInfo = new SegmentRefreshInfo(0L, 0);
    }
    return segmentRefreshInfo;
  }

  @Override public void takeCarbonIndexFileSnapShot() throws IOException {
    // Only Segment Information is updated.
    // File information will be fetched on the fly according to the fecthed segment info.
    this.loadMetadataDetails = SegmentStatusManager
        .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
  }

  @Override public Configuration getConfiguration() {
    return configuration;
  }

  @Override public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override public String getFilePath() {
    return identifier.getTablePath();
  }
}
