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

package org.apache.carbondata.core.index.status;

import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * Status of each index
 */
@InterfaceAudience.Internal
public class IndexStatusDetail implements Serializable {

  private static final long serialVersionUID = 1570997199499681821L;
  private String dataMapName;

  private IndexStatus status;

  public IndexStatusDetail() {
  }

  public IndexStatusDetail(String dataMapName, IndexStatus status) {
    this.dataMapName = dataMapName;
    this.status = status;
  }

  public String getDataMapName() {
    return dataMapName;
  }

  public void setDataMapName(String dataMapName) {
    this.dataMapName = dataMapName;
  }

  public IndexStatus getStatus() {
    return status;
  }

  public boolean isEnabled() {
    return status == IndexStatus.ENABLED;
  }

  public void setStatus(IndexStatus status) {
    this.status = status;
  }
}
