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
package org.apache.carbondata.core.indexstore;

import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Blocklet
 */
public class Blocklet implements Serializable {

  private String path;

  private String segmentId;

  private String blockletId;

  private BlockletDetailInfo detailInfo;

  private long length;

  private String[] location;

  public Blocklet(String path, String blockletId) {
    this.path = path;
    this.blockletId = blockletId;
  }

  public Path getPath() {
    return new Path(path);
  }

  public String getBlockletId() {
    return blockletId;
  }

  public BlockletDetailInfo getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(BlockletDetailInfo detailInfo) {
    this.detailInfo = detailInfo;
  }

  public void updateLocations() throws IOException {
    Path fspath = new Path(path);
    FileSystem fs = fspath.getFileSystem(FileFactory.getConfiguration());
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(fspath);
    LocatedFileStatus fileStatus = iter.next();
    location = fileStatus.getBlockLocations()[0].getHosts();
    length = fileStatus.getLen();
  }

  public String[] getLocations() {
    return location;
  }

  public long getLength() {
    return length;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

}
