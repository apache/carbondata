package org.carbondata.processing.newflow.encoding;

public interface RowEncoder {

  Object[] encode(Object[] row);

}
