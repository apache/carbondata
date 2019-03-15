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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;

/**
 * This is a readCommittedScope for non transactional carbon table
 */
@InterfaceAudience.Internal
@InterfaceStability.Stable
public class LatestFilesReadCommittedScope implements ReadCommittedScope {

  private static final long serialVersionUID = -839970494288861816L;
  private String carbonFilePath;
  private String segmentId;
  private ReadCommittedIndexFileSnapShot readCommittedIndexFileSnapShot;
  private LoadMetadataDetails[] loadMetadataDetails;
  private transient Configuration configuration;

  /**
   * a new constructor of this class
   *
   * @param path      carbon file path
   * @param segmentId segment id
   */
  public LatestFilesReadCommittedScope(String path, String segmentId, Configuration configuration)
      throws IOException {
    this.configuration = configuration;
    Objects.requireNonNull(path);
    this.carbonFilePath = path;
    this.segmentId = segmentId;
    takeCarbonIndexFileSnapShot();
  }

  /**
   * a new constructor with path
   *
   * @param path carbon file path
   */
  public LatestFilesReadCommittedScope(String path, Configuration configuration)
      throws IOException {
    this(path, null, configuration);
  }

  /**
   * a new constructor with carbon index files
   *
   * @param indexFiles carbon index files
   */
  public LatestFilesReadCommittedScope(CarbonFile[] indexFiles, Configuration configuration) {
    this.configuration = configuration;
    takeCarbonIndexFileSnapShot(indexFiles);
  }

  private void prepareLoadMetadata() {
    int loadCount = 0;
    Map<String, List<String>> snapshotMap =
        this.readCommittedIndexFileSnapShot.getSegmentIndexFileMap();
    LoadMetadataDetails[] loadMetadataDetailsArray = new LoadMetadataDetails[snapshotMap.size()];
    String segmentID;
    for (Map.Entry<String, List<String>> entry : snapshotMap.entrySet()) {
      segmentID = entry.getKey();
      LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
      long timeSet;
      try {
        timeSet = Long.parseLong(segmentID);
      } catch (NumberFormatException nu) {
        timeSet = 0;
      }
      loadMetadataDetails.setLoadEndTime(timeSet);
      loadMetadataDetails.setLoadStartTime(timeSet);
      loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
      loadMetadataDetails.setLoadName(segmentID);
      loadMetadataDetailsArray[loadCount++] = loadMetadataDetails;
    }
    this.loadMetadataDetails = loadMetadataDetailsArray;
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
    Map<String, String> indexFileStore = new HashMap<>();
    Map<String, List<String>> snapShot = readCommittedIndexFileSnapShot.getSegmentIndexFileMap();
    String segName;
    if (segment.getSegmentNo() != null) {
      segName = segment.getSegmentNo();
    } else {
      segName = segment.getSegmentFileName();
    }
    List<String> index = snapShot.get(segName);
    if (null == index) {
      index = new LinkedList<>();
    }
    for (String indexPath : index) {
      if (indexPath.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        indexFileStore.put(indexPath, indexPath.substring(indexPath.lastIndexOf('/') + 1));
      } else {
        indexFileStore.put(indexPath, null);
      }
    }
    return indexFileStore;
  }

  @Override
  public SegmentRefreshInfo getCommittedSegmentRefreshInfo(Segment segment, UpdateVO updateVo)
      throws IOException {
    Map<String, SegmentRefreshInfo> snapShot =
        readCommittedIndexFileSnapShot.getSegmentTimestampUpdaterMap();
    String segName;
    if (segment.getSegmentNo() != null) {
      segName = segment.getSegmentNo();
    } else {
      segName = segment.getSegmentFileName();
    }
    SegmentRefreshInfo segmentRefreshInfo = snapShot.get(segName);
    return segmentRefreshInfo;
  }

  private String getSegmentID(String carbonIndexFileName, String indexFilePath) {
    if (indexFilePath.contains("/Fact/Part0/Segment_")) {
      // This is CarbonFile case where the Index files are present inside the Segment Folder
      // So the Segment has to be extracted from the path not from the CarbonIndex file.
      String segString = indexFilePath.substring(0, indexFilePath.lastIndexOf("/") + 1);
      String segName =
          segString.substring(segString.lastIndexOf("_") + 1, segString.lastIndexOf("/"));
      return segName;
    } else {
      String fileName = carbonIndexFileName;
      String segId = fileName.substring(fileName.lastIndexOf("-") + 1, fileName.lastIndexOf("."));
      return segId;
    }
  }

  @Override public void takeCarbonIndexFileSnapShot() throws IOException {
    // Read the current file Path get the list of indexes from the path.
    CarbonFile file = FileFactory.getCarbonFile(carbonFilePath, configuration);

    CarbonFile[] carbonIndexFiles = null;
    if (file.isDirectory()) {
      if (segmentId == null) {
        List<CarbonFile> indexFiles = new ArrayList<>();
        SegmentIndexFileStore.getCarbonIndexFilesRecursively(file, indexFiles);
        carbonIndexFiles = indexFiles.toArray(new CarbonFile[0]);
      } else {
        String segmentPath = CarbonTablePath.getSegmentPath(carbonFilePath, segmentId);
        carbonIndexFiles = SegmentIndexFileStore.getCarbonIndexFiles(segmentPath, configuration);
      }
      if (carbonIndexFiles.length == 0) {
        throw new IOException(
            "No Index files are present in the table location :" + carbonFilePath);
      }
      takeCarbonIndexFileSnapShot(carbonIndexFiles);
    } else {
      throw new IOException("Path is not pointing to directory");
    }
  }

  private void takeCarbonIndexFileSnapShot(CarbonFile[] carbonIndexFiles) {
    Map<String, List<String>> indexFileStore = new HashMap<>();
    Map<String, SegmentRefreshInfo> segmentTimestampUpdaterMap = new HashMap<>();
    for (int i = 0; i < carbonIndexFiles.length; i++) {
      // TODO. Nested File Paths.
      if (carbonIndexFiles[i].getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)
          || carbonIndexFiles[i].getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        // Get Segment Name from the IndexFile.
        String indexFilePath =
            FileFactory.getUpdatedFilePath(carbonIndexFiles[i].getAbsolutePath());
        String segId = getSegmentID(carbonIndexFiles[i].getName(), indexFilePath);
        // TODO. During Partition table handling, place Segment File Name.
        List<String> indexList;
        SegmentRefreshInfo segmentRefreshInfo;
        if (indexFileStore.get(segId) == null) {
          indexList = new ArrayList<>(1);
          segmentRefreshInfo =
              new SegmentRefreshInfo(carbonIndexFiles[i].getLastModifiedTime(), 0);
          segmentTimestampUpdaterMap.put(segId, segmentRefreshInfo);
        } else {
          // Entry is already present.
          indexList = indexFileStore.get(segId);
          segmentRefreshInfo = segmentTimestampUpdaterMap.get(segId);
        }
        indexList.add(indexFilePath);
        if (segmentRefreshInfo.getSegmentUpdatedTimestamp() < carbonIndexFiles[i]
            .getLastModifiedTime()) {
          segmentRefreshInfo
              .setSegmentUpdatedTimestamp(carbonIndexFiles[i].getLastModifiedTime());
        }
        indexFileStore.put(segId, indexList);
        segmentRefreshInfo.setCountOfFileInSegment(indexList.size());
      }
    }
    ReadCommittedIndexFileSnapShot readCommittedIndexFileSnapShot =
        new ReadCommittedIndexFileSnapShot(indexFileStore, segmentTimestampUpdaterMap);
    this.readCommittedIndexFileSnapShot = readCommittedIndexFileSnapShot;
    prepareLoadMetadata();
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  @Override public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override public String getFilePath() {
    return carbonFilePath;
  }
}
