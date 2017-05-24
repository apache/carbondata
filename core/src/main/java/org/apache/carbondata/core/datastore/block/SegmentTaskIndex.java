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
import org.apache.carbondata.core.datastore.impl.array.IndexStoreFactory;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of a table segment
 */
public class SegmentTaskIndex extends AbstractIndex {

  public SegmentTaskIndex(SegmentProperties segmentProperties) {
    this.segmentProperties = segmentProperties;
  }

  /**
   * Below method is store the blocks in some data structure
   *
   */
  public void buildIndex(List<DataFileFooter> footerList) {
    // create a segment builder info
    // in case of segment create we do not need any file path and each column value size
    // as Btree will be build as per min max and start key
    BTreeBuilderInfo btreeBuilderInfo =
        new BTreeBuilderInfo(footerList, segmentProperties.getEachDimColumnValueSize());
    BtreeBuilder blocksBuilder = IndexStoreFactory.getDriverIndexBuilder();
    // load the metadata
    blocksBuilder.build(btreeBuilderInfo);
    dataRefNode = blocksBuilder.get();
    for (DataFileFooter footer : footerList) {
      totalNumberOfRows += footer.getNumberOfRows();
    }
  }
}
