/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.newflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.schema.BucketingInfo;

public class CarbonDataLoadConfiguration {

  private DataField[] dataFields;

  private AbsoluteTableIdentifier tableIdentifier;

  private String[] header;

  private String partitionId;

  private String segmentId;

  private String taskNo;

  private BucketingInfo bucketingInfo;

  private Map<String, Object> dataLoadProperties = new HashMap<>();

  public int getDimensionCount() {
    int dimCount = 0;
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().isDimesion()) {
        dimCount++;
      }
    }
    return dimCount;
  }

  public int getNoDictionaryCount() {
    int dimCount = 0;
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().isDimesion() && !dataFields[i].hasDictionaryEncoding()) {
        dimCount++;
      }
    }
    return dimCount;
  }

  public int getComplexDimensionCount() {
    int dimCount = 0;
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().isComplex()) {
        dimCount++;
      }
    }
    return dimCount;
  }

  public int getMeasureCount() {
    int msrCount = 0;
    for (int i = 0; i < dataFields.length; i++) {
      if (!dataFields[i].getColumn().isDimesion()) {
        msrCount++;
      }
    }
    return msrCount;
  }

  public DataField[] getDataFields() {
    return dataFields;
  }

  public void setDataFields(DataField[] dataFields) {
    this.dataFields = dataFields;
  }

  public String[] getHeader() {
    return header;
  }

  public void setHeader(String[] header) {
    this.header = header;
  }

  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(AbsoluteTableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public String getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(String partitionId) {
    this.partitionId = partitionId;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  public String getTaskNo() {
    return taskNo;
  }

  public void setTaskNo(String taskNo) {
    this.taskNo = taskNo;
  }

  public void setDataLoadProperty(String key, Object value) {
    dataLoadProperties.put(key, value);
  }

  public Object getDataLoadProperty(String key, Object defaultValue) {
    Object value = dataLoadProperties.get(key);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  public Object getDataLoadProperty(String key) {
    return dataLoadProperties.get(key);
  }

  public BucketingInfo getBucketingInfo() {
    return bucketingInfo;
  }

  public void setBucketingInfo(BucketingInfo bucketingInfo) {
    this.bucketingInfo = bucketingInfo;
  }
}
