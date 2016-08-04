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
package org.apache.carbondata.core.carbon.datastore.block;

import java.util.List;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.BtreeBuilder;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeBuilder;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of a table segment
 */
public class SegmentTaskIndex extends AbstractIndex {

  /**
   * Below method is store the blocks in some data structure
   *
   * @param blockInfo block detail
   */
  public void buildIndex(List<DataFileFooter> footerList) {
    // create a metadata details
    // this will be useful in query handling
    // all the data file metadata will have common segment properties we
    // can use first one to get create the segment properties
    segmentProperties = new SegmentProperties(footerList.get(0).getColumnInTable(),
        footerList.get(0).getSegmentInfo().getColumnCardinality());
    // create a segment builder info
    // in case of segment create we do not need any file path and each column value size
    // as Btree will be build as per min max and start key
    BTreeBuilderInfo btreeBuilderInfo = new BTreeBuilderInfo(footerList, null);
    BtreeBuilder blocksBuilder = new BlockBTreeBuilder();
    // load the metadata
    blocksBuilder.build(btreeBuilderInfo);
    dataRefNode = blocksBuilder.get();
    for (DataFileFooter footer : footerList) {
      totalNumberOfRows += footer.getNumberOfRows();
    }
  }
}
