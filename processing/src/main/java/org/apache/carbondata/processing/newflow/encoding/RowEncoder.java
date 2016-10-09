package org.apache.carbondata.processing.newflow.encoding;

/**
 * Encodes the row
 */
public interface RowEncoder {

  Object[] encode(Object[] row);

  void finish();
}
