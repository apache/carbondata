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

package org.apache.carbondata.core.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;

/**
 * It maintains index files and row count info of one segment.
 */
public class SegmentIndexMeta {
  private String segmentPath;
  private final List<CarbonFile> segmentIndexFiles = new ArrayList<>();
  private final Map<String, Long> prunedIndexFileToRowCountMap = new ConcurrentHashMap<>();

  public SegmentIndexMeta(String segmentPath) {
    this.segmentPath = segmentPath;
  }

  public List<CarbonFile> getSegmentIndexFiles() {
    return segmentIndexFiles;
  }

  public Map<String, Long> getPrunedIndexFileToRowCountMap() {
    return prunedIndexFileToRowCountMap;
  }

  public void addSegmentIndexFiles(List<CarbonFile> indexFiles) {
    synchronized (segmentIndexFiles) {
      this.segmentIndexFiles.addAll(indexFiles);
    }
  }

  public String getSegmentPath() {
    return segmentPath;
  }

  public void setSegmentPath(String segmentPath) {
    this.segmentPath = segmentPath;
  }
}
