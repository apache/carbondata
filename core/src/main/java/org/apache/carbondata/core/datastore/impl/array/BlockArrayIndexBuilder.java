package org.apache.carbondata.core.datastore.impl.array;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.BtreeBuilder;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.impl.btree.BTreeNode;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;

/**
 * Created by root1 on 18/5/17.
 */
public class BlockArrayIndexBuilder implements BtreeBuilder {

  @Override public void build(BTreeBuilderInfo btreeBuilderInfo) {
    int indexSize = 0;
    List<DataFileFooter> footerList = btreeBuilderInfo.getFooterList();
    for (int metadataIndex = 0; metadataIndex < footerList.size(); metadataIndex++) {
      DataFileFooter footer = footerList.get(metadataIndex);
      byte[] startKey = footer.getBlockletIndex().getBtreeIndex().getStartKey();
      indexSize += startKey.length;
      indexSize += 4;
      int size = (int)footer.getNumberOfRows();
      indexSize += 4;
      BlockletMinMaxIndex minMaxIndex = footer.getBlockletIndex().getMinMaxIndex();
      byte[][] maxValues = minMaxIndex.getMaxValues();
      for (int i = 0; i < maxValues.length; i++) {
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] == -1) {
          indexSize += 4;
        }
      }
      // pointer for fix
      indexSize += 4;
      indexSize += 4;

    }
  }

  @Override public DataRefNode get() {
    return null;
  }
}
