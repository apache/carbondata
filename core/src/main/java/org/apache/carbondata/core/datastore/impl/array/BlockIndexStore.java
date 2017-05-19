package org.apache.carbondata.core.datastore.impl.array;

import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Created by root1 on 18/5/17.
 */
public class BlockIndexStore implements IndexStore {



  @Override public byte[] getMin(int index, int colIndex) {
    return new byte[0];
  }

  @Override public byte[] getMax(int index, int colIndex) {
    return new byte[0];
  }

  @Override public IndexKey getIndexKey(int index) {
    return null;
  }
}
