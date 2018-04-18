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
package org.apache.carbondata.core.datastore.impl.btree;

import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.block.BlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;

/**
 * Leaf node for btree where only min max will be store this can be used from
 * driver when only we need to find whether particular block be selected for
 * query execution
 */
public class BlockBTreeLeafNode extends AbstractBTreeLeafNode {

  private BlockInfo blockInfo;

  /**
   * Create a leaf node
   *
   * @param builderInfos  builder infos which have required metadata to create a leaf
   *                      node
   * @param metadataIndex metadata index
   */
  BlockBTreeLeafNode(BTreeBuilderInfo builderInfos, int metadataIndex, long nodeNumber) {
    DataFileFooter footer = builderInfos.getFooterList().get(metadataIndex);
    BlockletMinMaxIndex minMaxIndex = footer.getBlockletIndex().getMinMaxIndex();
    maxKeyOfColumns = minMaxIndex.getMaxValues();
    minKeyOfColumns = minMaxIndex.getMinValues();
    numberOfKeys = (int)footer.getNumberOfRows();
    this.nodeNumber = nodeNumber;
    this.blockInfo = footer.getBlockInfo();
  }

  /**
   * Below method is to get the table block info
   * This will be used only in case of BlockBtree leaf node which will
   * be used to from driver
   *
   * @return TableBlockInfo
   */
  public TableBlockInfo getTableBlockInfo() {
    return blockInfo.getTableBlockInfo();
  }

  /**
   * Below method is suppose to return the Blocklet ID.
   * @return
   */
  @Override public short blockletIndex() {
    return blockInfo.getTableBlockInfo().getDetailInfo().getBlockletId();
  }

  /**
   * number of pages in blocklet
   * @return
   */
  @Override
  public int numberOfPages() {
    throw new UnsupportedOperationException("Unsupported operation");
  }

}
