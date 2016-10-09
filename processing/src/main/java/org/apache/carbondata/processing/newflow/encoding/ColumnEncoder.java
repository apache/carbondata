package org.apache.carbondata.processing.newflow.encoding;

import java.io.DataOutputStream;

public interface ColumnEncoder {

  void encode(ColumnData columnData, DataOutputStream stream);

  byte[] encode(ColumnData columnData);

}
