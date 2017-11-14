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

package org.apache.carbondata.hadoop.util;

import java.util.Map;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Block level traverser
 */
public class BlockLevelTraverser {

  /**
   *
   * @param abstractIndex
   * @param blockRowMap
   * @param segId
   * @param updateStatusManager
   * @throws KeyGenException
   */
  public long getBlockRowMapping(AbstractIndex abstractIndex, Map<String, Long> blockRowMap,
      String segId, SegmentUpdateStatusManager updateStatusManager)
      throws KeyGenException {

    IndexKey  searchStartKey =
          FilterUtil.prepareDefaultStartIndexKey(abstractIndex.getSegmentProperties());

    DataRefNodeFinder blockFinder = new BTreeDataRefNodeFinder(
        abstractIndex.getSegmentProperties().getEachDimColumnValueSize(),
        abstractIndex.getSegmentProperties().getNumberOfSortColumns(),
        abstractIndex.getSegmentProperties().getNumberOfNoDictSortColumns());
    DataRefNode currentBlock =
        blockFinder.findFirstDataBlock(abstractIndex.getDataRefNode(), searchStartKey);

    long count = 0;

    while (currentBlock != null) {

      String blockName = ((BlockBTreeLeafNode) currentBlock).getTableBlockInfo().getFilePath();
      blockName = CarbonTablePath.getCarbonDataFileName(blockName);
      blockName = blockName + CarbonTablePath.getCarbonDataExtension();

      long rowCount = currentBlock.nodeSize();

      String key = CarbonUpdateUtil.getSegmentBlockNameKey(segId, blockName);

      // if block is invalid then dont add the count
      SegmentUpdateDetails details = updateStatusManager.getDetailsForABlock(key);

      if (null == details || !CarbonUpdateUtil.isBlockInvalid(details.getSegmentStatus())) {
        blockRowMap.put(key, rowCount);
        count++;
      }
      currentBlock = currentBlock.getNextDataRefNode();
    }

    return count;
  }

}
