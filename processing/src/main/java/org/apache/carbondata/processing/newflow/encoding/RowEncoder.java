package org.apache.carbondata.processing.newflow.encoding;

public interface RowEncoder {

  Object[] encode(Object[] row);

  void finish();
}
