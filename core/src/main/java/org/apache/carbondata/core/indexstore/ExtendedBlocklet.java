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

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Detailed blocklet information
 */
public class ExtendedBlocklet extends Blocklet {

  private String segmentId;

  private BlockletDetailInfo detailInfo;

  private long length;

  private String[] location;

  private String path;

  private String dataMapWriterPath;

  public ExtendedBlocklet(String path, String blockletId) {
    super(path, blockletId);
    this.path = path;
  }

  public BlockletDetailInfo getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(BlockletDetailInfo detailInfo) {
    this.detailInfo = detailInfo;
  }

  /**
   * It gets the hdfs block locations and length for this blocklet. It is used internally to get the
   * locations for allocating tasks.
   * @throws IOException
   */
  public void updateLocations() throws IOException {
    Path path = new Path(this.path);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    LocatedFileStatus fileStatus = iter.next();
    location = fileStatus.getBlockLocations()[0].getHosts();
    length = fileStatus.getLen();
  }

  public String[] getLocations() throws IOException {
    return location;
  }

  public long getLength() throws IOException {
    return length;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  public String getPath() {
    return path;
  }

  public String getDataMapWriterPath() {
    return dataMapWriterPath;
  }

  public void setDataMapWriterPath(String dataMapWriterPath) {
    this.dataMapWriterPath = dataMapWriterPath;
  }
}
