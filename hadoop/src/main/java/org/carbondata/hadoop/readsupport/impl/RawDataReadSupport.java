package org.carbondata.hadoop.readsupport.impl;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.hadoop.readsupport.CarbonReadSupport;

public class RawDataReadSupport implements CarbonReadSupport<Object[]> {

  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
  }

  /**
   * Just return same data.
   *
   * @param data
   * @return
   */
  @Override public Object[] readRow(Object[] data) {
    return data;
  }

  /**
   * This method iwll be used to clear the dictionary cache and update access count for each
   * column involved which will be used during eviction of columns from LRU cache if memory
   * reaches threshold
   */
  @Override public void close() {

  }
}
