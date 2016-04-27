/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 */
package org.carbondata.integration.spark.partition.api.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.integration.spark.partition.api.Partition;
import org.carbondata.query.queryinterface.query.metadata.CarbonDimensionLevelFilter;

public class PartitionImpl implements Partition {
  private static final long serialVersionUID = 3020172346383028547L;
  private String uniqueID;
  private String folderPath;

  private Map<String, CarbonDimensionLevelFilter> filterMap =
      new HashMap<String, CarbonDimensionLevelFilter>(
          CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  public PartitionImpl(String uniqueID, String folderPath) {
    this.uniqueID = uniqueID;
    this.folderPath = folderPath;
  }

  @Override public String getUniqueID() {
    return uniqueID;
  }

  @Override public String getFilePath() {
    return folderPath;
  }

  public void setPartitionDetails(String columnName, CarbonDimensionLevelFilter filter) {
    filterMap.put(columnName, filter);
  }

  @Override public String toString() {
    return "{PartitionID -> " + uniqueID + " Path: " + folderPath + '}';
  }

  @Override public List<String> getFilesPath() {
    // TODO Auto-generated method stub
    return null;
  }

}
