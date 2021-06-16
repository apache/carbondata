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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.Segment;
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

  @Override
  public LoadMetadataDetails[] getSegmentList() throws IOException {
    try {
      if (loadMetadataDetails == null) {
        takeCarbonIndexFileSnapShot();
      }
      return loadMetadataDetails;

    } catch (IOException ex) {
      throw new IOException("Problem encountered while reading the Table Status file.", ex);
    }
  }

  @Override
  public Map<String, String> getCommittedIndexFile(Segment segment) throws IOException {
    Map<String, String> indexFiles;
    SegmentFileStore fileStore = null;
    if (segment.getSegmentFileName() != null && !segment.getSegmentFileName().isEmpty()) {
      fileStore = new SegmentFileStore(identifier.getTablePath(), segment.getSegmentFileName());
    }
    if (segment.getSegmentFileName() == null || fileStore.getSegmentFile() == null) {
      String path =
          CarbonTablePath.getSegmentPath(identifier.getTablePath(), segment.getSegmentNo());
      indexFiles = new SegmentIndexFileStore().getMergeOrIndexFilesFromSegment(path);
      Set<String> mergedIndexFiles =
          SegmentFileStore.getInvalidAndMergedIndexFiles(new ArrayList<>(indexFiles.keySet()));
      Map<String, String> filteredIndexFiles = indexFiles;
      if (mergedIndexFiles.size() > 0) {
        // do not include already merged index files details.
        filteredIndexFiles = indexFiles.entrySet().stream()
            .filter(indexFile -> !mergedIndexFiles.contains(indexFile.getKey()))
            .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
      }
      return filteredIndexFiles;
    } else {
      indexFiles = fileStore.getIndexAndMergeFiles();
      if (fileStore.getSegmentFile() != null) {
        segment.setSegmentMetaDataInfo(fileStore.getSegmentFile().getSegmentMetaDataInfo());
      }
    }
    return indexFiles;
  }

  public SegmentRefreshInfo getCommittedSegmentRefreshInfo(Segment segment, UpdateVO updateVo) {
    SegmentRefreshInfo segmentRefreshInfo;
    long segmentFileTimeStamp = 0L;
    String segmentFileName = segment.getSegmentFileName();
    if (null != segmentFileName) {
      // Do not use getLastModifiedTime API on segment file carbon file object as it will slow down
      // operation in Object stores like S3. Now the segment file is always written for operations
      // which was overwriting earlier, so this timestamp can be checked always to check whether
      // to refresh the cache or not
      segmentFileTimeStamp = Long.parseLong(segmentFileName
          .substring(segmentFileName.indexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              segmentFileName.lastIndexOf(CarbonCommonConstants.POINT)));
    }
    if (updateVo != null) {
      segmentRefreshInfo =
          new SegmentRefreshInfo(updateVo.getLatestUpdateTimestamp(), 0, segmentFileTimeStamp);
    } else {
      segmentRefreshInfo = new SegmentRefreshInfo(0L, 0, segmentFileTimeStamp);
    }
    return segmentRefreshInfo;
  }

  @Override
  public void takeCarbonIndexFileSnapShot() throws IOException {
    // Only Segment Information is updated.
    // File information will be fetched on the fly according to the fetched segment info.
    this.loadMetadataDetails = SegmentStatusManager
        .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String getFilePath() {
    return identifier.getTablePath();
  }
}
