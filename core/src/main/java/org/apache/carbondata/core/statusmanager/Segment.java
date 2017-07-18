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
package org.apache.carbondata.core.statusmanager;

/**
 * Segment is unit of one dataload.
 */
public class Segment {

  /**
   * By using this same name carbondata creates folder and use for loading and reading segment.
   */
  private String segmentName;

  private long updatedTime;

  private LoadStatus loadStatus;

  /**
   * Any extra information should be encoded as json string and set here.
   */
  private String properties;

  public String getSegmentName() {
    return segmentName;
  }

  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  public long getUpdatedTime() {
    return updatedTime;
  }

  public void setUpdatedTime(long updatedTime) {
    this.updatedTime = updatedTime;
  }

  public LoadStatus getLoadStatus() {
    return loadStatus;
  }

  public void setLoadStatus(LoadStatus loadStatus) {
    this.loadStatus = loadStatus;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }
}
