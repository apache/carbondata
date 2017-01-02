package org.apache.carbondata.hadoop.util;

import java.util.Map;

import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.update.CarbonUpdateUtil;
import org.apache.carbondata.core.update.SegmentUpdateDetails;
import org.apache.carbondata.core.updatestatus.SegmentUpdateStatusManager;
import org.apache.carbondata.scan.filter.FilterUtil;


/**
 *
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
        abstractIndex.getSegmentProperties().getEachDimColumnValueSize());
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

      if (null == details || !CarbonUpdateUtil.isBlockInvalid(details.getStatus())) {
        blockRowMap.put(key, rowCount);
        count++;
      }
      currentBlock = currentBlock.getNextDataRefNode();
    }

    return count;
  }

}
