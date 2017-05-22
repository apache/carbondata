package org.apache.carbondata.core.datastore.impl.array;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Created by root1 on 18/5/17.
 */
public interface IndexStore extends DataRefNode {

  IndexKey getIndexKey(int index);

  byte[] getMin(int index, int colIndex);

  byte[] getMax(int index, int colIndex);

}
