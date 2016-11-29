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

package org.apache.carbondata.spark.load;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public class DeletedLoadMetadata implements Serializable {

  private static final long serialVersionUID = 7083059404172117208L;
  private Map<String, String> deletedLoadMetadataMap =
      new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  public void addDeletedLoadMetadata(String loadId, String status) {
    deletedLoadMetadataMap.put(loadId, status);
  }

  public List<String> getDeletedLoadMetadataIds() {
    return new ArrayList<String>(deletedLoadMetadataMap.keySet());
  }

  public String getDeletedLoadMetadataStatus(String loadId) {
    if (deletedLoadMetadataMap.containsKey(loadId)) {
      return deletedLoadMetadataMap.get(loadId);
    } else {
      return null;
    }

  }

}
