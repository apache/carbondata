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

package org.apache.carbondata.spark.partition.api.impl;

import java.util.List;

import org.apache.carbondata.spark.partition.api.Partition;

public class PartitionMultiFileImpl implements Partition {
  private static final long serialVersionUID = -4363447826181193976L;
  private String uniqueID;
  private List<String> folderPath;

  public PartitionMultiFileImpl(String uniqueID, List<String> folderPath) {
    this.uniqueID = uniqueID;
    this.folderPath = folderPath;
  }

  @Override public String getUniqueID() {
    // TODO Auto-generated method stub
    return uniqueID;
  }

  @Override public List<String> getFilesPath() {
    // TODO Auto-generated method stub
    return folderPath;
  }

}
