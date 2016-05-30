package org.carbondata.hadoop.readsupport;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;

/**
 * It converts to the desired class while reading the rows from RecordReader
 */
public interface CarbonReadSupport<T> {

  /**
   * It can use [{@link CarbonColumn}] array to create its own schema to create its row.
   *
   * @param carbonColumns
   */
  public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier);

  public T readRow(Object[] data);

}
