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

package org.apache.carbondata.processing.newflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.processing.newflow.converter.DictionaryCardinalityFinder;

public class CarbonDataLoadConfiguration {

  private DataField[] dataFields;

  private DataField[] dimensionFields;

  private DataField[] measureFields;

  private AbsoluteTableIdentifier tableIdentifier;

  private String[] header;

  private String partitionId;

  private String segmentId;

  private String taskNo;

  private BucketingInfo bucketingInfo;

  private Map<String, Object> dataLoadProperties = new HashMap<>();

  /**
   *  Use one pass to generate dictionary
   */
  private boolean useOnePass;

  /**
   * dictionary server host
   */
  private String dictionaryServerHost;

  /**
   * dictionary sever port
   */
  private int dictionaryServerPort;

  private boolean preFetch;

  private int dimensionCount;

  private int measureCount;

  /**
   * schema updated time stamp to be used for restructure scenarios
   */
  private long schemaUpdatedTimeStamp;

  private DictionaryCardinalityFinder cardinalityFinder;

  public CarbonDataLoadConfiguration() {
  }

  private void initDimensionFields() {
    List<Integer> dimensionIndexes = new ArrayList<>(dataFields.length);
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().isDimesion()) {
        dimensionIndexes.add(i);
        dimensionCount++;
      }
    }
    dimensionFields = new DataField[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      dimensionFields[i] = dataFields[dimensionIndexes.get(i)];
    }
  }

  private void initMeasureFields() {
    List<Integer> measureIndexes = new ArrayList<>(dataFields.length);
    for (int i = 0; i < dataFields.length; i++) {
      if (!dataFields[i].getColumn().isDimesion()) {
        measureIndexes.add(i);
        measureCount++;
      }
    }
    measureFields = new DataField[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureFields[i] = dataFields[measureIndexes.get(i)];
    }
  }

  public int getDimensionCount() {
    return dimensionCount;
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
    return measureCount;
  }

  public DataField[] getDataFields() {
    return dataFields;
  }

  public void setDataFields(DataField[] dataFields) {
    this.dataFields = dataFields;
    initDimensionFields();
    initMeasureFields();
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

  public Object getDataLoadProperty(String key) {
    return dataLoadProperties.get(key);
  }

  public BucketingInfo getBucketingInfo() {
    return bucketingInfo;
  }

  public void setBucketingInfo(BucketingInfo bucketingInfo) {
    this.bucketingInfo = bucketingInfo;
  }

  public boolean getUseOnePass() {
    return useOnePass;
  }

  public void setUseOnePass(boolean useOnePass) {
    this.useOnePass = useOnePass;
  }

  public String getDictionaryServerHost() {
    return dictionaryServerHost;
  }

  public void setDictionaryServerHost(String dictionaryServerHost) {
    this.dictionaryServerHost = dictionaryServerHost;
  }

  public int getDictionaryServerPort() {
    return dictionaryServerPort;
  }

  public void setDictionaryServerPort(int dictionaryServerPort) {
    this.dictionaryServerPort = dictionaryServerPort;
  }

  public boolean isPreFetch() {
    return preFetch;
  }

  public void setPreFetch(boolean preFetch) {
    this.preFetch = preFetch;
  }

  public DataField[] getDimensionFields() {
    return dimensionFields;
  }

  public DataField[] getMeasureFields() {
    return measureFields;
  }

  public long getSchemaUpdatedTimeStamp() {
    return schemaUpdatedTimeStamp;
  }

  public void setSchemaUpdatedTimeStamp(long schemaUpdatedTimeStamp) {
    this.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
  }

  public DictionaryCardinalityFinder getCardinalityFinder() {
    return cardinalityFinder;
  }

  public void setCardinalityFinder(DictionaryCardinalityFinder cardinalityFinder) {
    this.cardinalityFinder = cardinalityFinder;
  }
}
