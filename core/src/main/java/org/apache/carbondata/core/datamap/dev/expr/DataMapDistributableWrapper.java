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
package org.apache.carbondata.core.datamap.dev.expr;

import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.core.datamap.DataMapDistributable;

import org.apache.hadoop.mapreduce.InputSplit;

public class DataMapDistributableWrapper extends InputSplit implements Serializable {

  private String uniqueId;

  private DataMapDistributable distributable;

  public DataMapDistributableWrapper(String uniqueId, DataMapDistributable distributable) {
    this.uniqueId = uniqueId;
    this.distributable = distributable;
  }

  public String getUniqueId() {
    return uniqueId;
  }

  public DataMapDistributable getDistributable() {
    return distributable;
  }

  public void setDistributable(DataMapDistributable distributable) {
    this.distributable = distributable;
  }

  @Override public long getLength() throws IOException, InterruptedException {
    return distributable.getLength();
  }

  @Override public String[] getLocations() throws IOException, InterruptedException {
    return distributable.getLocations();
  }
}
