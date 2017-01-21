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
package org.apache.carbondata.core.datastore.block;

import java.util.List;

import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.BtreeBuilder;
import org.apache.carbondata.core.datastore.impl.btree.BlockletBTreeBuilder;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of a table block
 */
public class BlockIndex extends AbstractIndex {

  /**
   * Below method will be used to load the data block
   *
   */
  public void buildIndex(List<DataFileFooter> footerList) {
    // create a metadata details
    // this will be useful in query handling
    segmentProperties = new SegmentProperties(footerList.get(0).getColumnInTable(),
        footerList.get(0).getSegmentInfo().getColumnCardinality());
    // create a segment builder info
    BTreeBuilderInfo indexBuilderInfo =
        new BTreeBuilderInfo(footerList, segmentProperties.getDimensionColumnsValueSize());
    BtreeBuilder blocksBuilder = new BlockletBTreeBuilder();
    // load the metadata
    blocksBuilder.build(indexBuilderInfo);
    dataRefNode = blocksBuilder.get();
    totalNumberOfRows = footerList.get(0).getNumberOfRows();
  }
}
