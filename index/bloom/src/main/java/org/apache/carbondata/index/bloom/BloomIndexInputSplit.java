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

package org.apache.carbondata.index.bloom;

import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.index.IndexInputSplit;

@InterfaceAudience.Internal
class BloomIndexInputSplit extends IndexInputSplit {
  /**
   * parent folder of the bloomindex file
   */
  private String indexPath;

  /**
   * List of index shards which are already got filtered through CG index operation.
   * This is used for merge shard which cannot prune shard in `toDistributable` function.
   * Other case will be set to Null
   */
  private Set<String> filteredShards;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
  BloomIndexInputSplit(String indexPath, Set<String> filteredShards) {
    this.indexPath = indexPath;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2845
    this.filteredShards = filteredShards;
  }

  public String getIndexPath() {
    return indexPath;
  }

  public Set<String> getFilteredShards() {
    return filteredShards;
  }
}
