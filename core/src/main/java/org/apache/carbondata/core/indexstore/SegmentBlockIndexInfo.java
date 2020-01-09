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

package org.apache.carbondata.core.indexstore;

import java.util.Set;

import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo;

/**
 * Holds tableBlockUniqueIdentifiers and block level minMax values for the segment
 */
public class SegmentBlockIndexInfo {

  /**
   * IndexFile's information for the segment
   */
  private Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers;

  /**
   * segment metadata info
   */
  private SegmentMetaDataInfo segmentMetaDataInfo;

  public SegmentBlockIndexInfo(
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers,
      SegmentMetaDataInfo segmentMetaDataInfo) {
    this.tableBlockIndexUniqueIdentifiers = tableBlockIndexUniqueIdentifiers;
    this.segmentMetaDataInfo = segmentMetaDataInfo;
  }

  public Set<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers() {
    return tableBlockIndexUniqueIdentifiers;
  }

  public SegmentMetaDataInfo getSegmentMetaDataInfo() {
    return segmentMetaDataInfo;
  }

}
