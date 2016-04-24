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
package org.carbondata.integration.spark.partition.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.carbondata.query.queryinterface.query.metadata.CarbonDimensionLevelFilter;

public interface Partition extends Serializable {
  /**
   * unique identification for the partition in the cluster.
   */
  String getUniqueID();

  /**
   * File path for the raw data represented by this partition
   */
  String getFilePath();

  /**
   * result
   *
   * @return
   */
  List<String> getFilesPath();
}