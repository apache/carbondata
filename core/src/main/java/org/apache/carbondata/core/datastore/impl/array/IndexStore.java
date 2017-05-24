package org.apache.carbondata.core.datastore.impl.array;

import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Created by root1 on 18/5/17.
 */
public interface IndexStore {

  byte[] getMin(int index, int colIndex);

  byte[] getMax(int index, int colIndex);

  // TODO It can be removed later and use only {getMin(int index, int colIndex)} instead
  byte[][] getMins(int index);

  byte[][] getMaxs(int index);

  IndexKey getIndexKey(int index);

  boolean isKeyAvailableAtIndex(int index);

  int getRowCount(int index);

  int getIndexKeyCount();

  void clear();

}
