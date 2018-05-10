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
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This is a readCommittedScope for non transactional carbon table
 */
@InterfaceAudience.Internal
@InterfaceStability.Stable
public class LatestFilesReadCommittedScope implements ReadCommittedScope {

  private String carbonFilePath;
  private ReadCommittedIndexFileSnapShot readCommittedIndexFileSnapShot;
  private LoadMetadataDetails[] loadMetadataDetails;

  public LatestFilesReadCommittedScope(String path) {
    this.carbonFilePath = path;
    try {
      takeCarbonIndexFileSnapShot();
    } catch (IOException ex) {
      throw new RuntimeException("Error while taking index snapshot", ex);
    }
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
    for (String indexPath : index) {
      indexFileStore.put(indexPath, null);
    }
    return indexFileStore;
  }

  private String getSegmentID(String carbonIndexFileName, String indexFilePath) {
    if (indexFilePath.contains("/Fact/Part0/Segment_")) {
      // This is CarbonFile case where the Index files are present inside the Segment Folder
      // So the Segment has to be extracted from the path not from the CarbonIndex file.
      String segString = indexFilePath.substring(0, indexFilePath.lastIndexOf("/") + 1);
      String segName = segString
          .substring(segString.lastIndexOf("_") + 1, segString.lastIndexOf("/"));
      return segName;
    } else {
      String fileName = carbonIndexFileName;
      String segId = fileName.substring(fileName.lastIndexOf("-") + 1, fileName.lastIndexOf("."));
      return segId;
    }
  }

  @Override public void takeCarbonIndexFileSnapShot() throws IOException {
    // Read the current file Path get the list of indexes from the path.
    CarbonFile file = FileFactory.getCarbonFile(carbonFilePath);
    if (file == null) {
      // For nonTransactional table, files can be removed at any point of time.
      // So cannot assume files will be present
      throw new IOException("No files are present in the table location :"+ carbonFilePath);
    }
    Map<String, List<String>> indexFileStore = new HashMap<>();
    if (file.isDirectory()) {
      CarbonFile[] carbonIndexFiles = SegmentIndexFileStore.getCarbonIndexFiles(carbonFilePath);
      for (int i = 0; i < carbonIndexFiles.length; i++) {
        // TODO. If Required to support merge index, then this code has to be modified.
        // TODO. Nested File Paths.
        if (carbonIndexFiles[i].getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
          // Get Segment Name from the IndexFile.
          String segId =
              getSegmentID(carbonIndexFiles[i].getName(), carbonIndexFiles[i].getAbsolutePath());
          // TODO. During Partition table handling, place Segment File Name.
          List<String> indexList;
          if (indexFileStore.get(segId) == null) {
            indexList = new ArrayList<>(1);
          } else {
            // Entry is already present.
            indexList = indexFileStore.get(segId);
          }
          indexList.add(carbonIndexFiles[i].getAbsolutePath());
          indexFileStore.put(segId, indexList);
        }
      }
      ReadCommittedIndexFileSnapShot readCommittedIndexFileSnapShot =
          new ReadCommittedIndexFileSnapShot(indexFileStore);
      this.readCommittedIndexFileSnapShot = readCommittedIndexFileSnapShot;
      prepareLoadMetadata();
    } else {
      throw new IOException("Path is not pointing to directory");
    }
  }

}
