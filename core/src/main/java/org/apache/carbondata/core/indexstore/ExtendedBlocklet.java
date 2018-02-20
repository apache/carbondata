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

/**
 * Detailed blocklet information
 */
public class ExtendedBlocklet extends Blocklet {

  private String segmentId;

  private BlockletDetailInfo detailInfo;

  private long length;

  private String[] location;

  private String dataMapWriterPath;

  private String dataMapUniqueId;

  public ExtendedBlocklet(String path, String blockletId) {
    super(path, blockletId);
  }

  public BlockletDetailInfo getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(BlockletDetailInfo detailInfo) {
    this.detailInfo = detailInfo;
  }

  public void setLocation(String[] location) {
    this.location = location;
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

  public String getPath() {
    return getBlockId();
  }

  public String getDataMapWriterPath() {
    return dataMapWriterPath;
  }

  public void setDataMapWriterPath(String dataMapWriterPath) {
    this.dataMapWriterPath = dataMapWriterPath;
  }

  public String getDataMapUniqueId() {
    return dataMapUniqueId;
  }

  public void setDataMapUniqueId(String dataMapUniqueId) {
    this.dataMapUniqueId = dataMapUniqueId;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) {
      return false;
    }

    ExtendedBlocklet that = (ExtendedBlocklet) o;

    return segmentId != null ? segmentId.equals(that.segmentId) : that.segmentId == null;
  }

  @Override public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (segmentId != null ? segmentId.hashCode() : 0);
    return result;
  }
}
