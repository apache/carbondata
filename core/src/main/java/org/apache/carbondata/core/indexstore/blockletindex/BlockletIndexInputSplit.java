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

package org.apache.carbondata.core.indexstore.blockletindex;

import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;

/**
 * This class contains required information to make the Blocklet index distributable.
 * Each distributable object can represents one index.
 * Using this object job like spark/MR can be launched and execute each distributable object as
 * one index task.
 */
public class BlockletIndexInputSplit extends IndexInputSplit {

  /**
   * Relative file path from the segment folder.
   */
  private String filePath;

  private String segmentPath;

  private TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier;

  public BlockletIndexInputSplit() {

  }

  public BlockletIndexInputSplit(String indexFilePath) {
    this.filePath = indexFilePath;
  }

  public String getFilePath() {
    return filePath;
  }

  public TableBlockIndexUniqueIdentifier getTableBlockIndexUniqueIdentifier() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    return tableBlockIndexUniqueIdentifier;
  }

  public void setTableBlockIndexUniqueIdentifier(
      TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifiers) {
    this.tableBlockIndexUniqueIdentifier = tableBlockIndexUniqueIdentifiers;
  }

  public String getSegmentPath() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3337
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3306
    return segmentPath;
  }

  public void setSegmentPath(String segmentPath) {
    this.segmentPath = segmentPath;
  }
}
