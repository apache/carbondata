package org.apache.carbondata.core.datastore.impl.array;

import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Created by root1 on 18/5/17.
 */
public interface IndexStore {

  byte[] getMin(int index, int colIndex);

  byte[] getMax(int index, int colIndex);

  IndexKey getIndexKey(int index);

}
