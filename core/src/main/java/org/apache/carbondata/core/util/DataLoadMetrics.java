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

package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * store data loading metrics
 */
public class DataLoadMetrics implements Serializable {
  // stores the count of files written per task
  private int fileCount;

  // stores output files names with size.
  // fileName1:size1,fileName2:size2
  private List<String> outputFiles;

  // partition path
  private List<String> partitionPath;

  private long mergeIndexSize;

  private long numOutputBytes = 0L;

  private long numOutputRows = 0L;

  public synchronized int getFileCount() {
    return fileCount;
  }

  public synchronized List<String> getOutputFiles() {
    return outputFiles;
  }

  public synchronized List<String> getPartitionPath() {
    return partitionPath;
  }

  public long getMergeIndexSize() {
    return mergeIndexSize;
  }

  public void setMergeIndexSize(long mergeIndexSize) {
    this.mergeIndexSize = mergeIndexSize;
  }

  public synchronized long getNumOutputBytes() {
    return numOutputBytes;
  }

  public synchronized long getNumOutputRows() {
    return numOutputRows;
  }

  public synchronized void incrementCount() {
    // can call in multiple threads in single task
    fileCount++;
  }

  public synchronized void addToOutputFiles(String file) {
    if (outputFiles == null) {
      outputFiles = new ArrayList<>();
    }
    outputFiles.add(file);
  }

  public synchronized void addToPartitionPath(String path) {
    if (partitionPath == null) {
      partitionPath = new ArrayList<>();
    }
    partitionPath.add(path);
  }

  public synchronized void addOutputBytes(long numOutputBytes) {
    this.numOutputBytes += numOutputBytes;
  }

  public synchronized void addOutputRows(long numOutputRows) {
    this.numOutputRows += numOutputRows;
  }
}
