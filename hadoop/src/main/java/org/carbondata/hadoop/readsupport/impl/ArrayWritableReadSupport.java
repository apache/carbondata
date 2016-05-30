package org.carbondata.hadoop.readsupport.impl;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.io.ArrayWritable;

public class ArrayWritableReadSupport implements CarbonReadSupport<ArrayWritable> {

  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
  }

  @Override public ArrayWritable readRow(Object[] data) {

    String[] writables = new String[data.length];
    for (int i = 0; i < data.length; i++) {
      writables[i] = data[i].toString();
    }
    return new ArrayWritable(writables);
  }
}
