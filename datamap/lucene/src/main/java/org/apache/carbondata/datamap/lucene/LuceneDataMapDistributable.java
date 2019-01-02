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

package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.DataMapDistributable;

@InterfaceAudience.Internal
class LuceneDataMapDistributable extends DataMapDistributable {

  // TODO: seems no one use this?
  private String dataPath;

  private String indexPath;

  LuceneDataMapDistributable(String dataPath, String indexPath) {
    this.dataPath = dataPath;
    this.indexPath = indexPath;
  }

  public String getDataPath() {
    return dataPath;
  }

  public String getIndexPath() {
    return indexPath;
  }

  public void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }
}
