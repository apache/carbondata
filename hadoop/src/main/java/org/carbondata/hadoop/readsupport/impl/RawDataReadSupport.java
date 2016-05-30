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
}
