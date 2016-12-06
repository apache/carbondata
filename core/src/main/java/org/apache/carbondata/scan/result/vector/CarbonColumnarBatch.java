package org.apache.carbondata.scan.result.vector;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;

public class CarbonColumnarBatch {

  private CarbonColumnVector[] columnVectors;

  private int batchSize;

  public CarbonColumnarBatch(DataType[] dataTypes) {

  }

  public CarbonColumnarBatch(CarbonColumnVector[] columnVectors, int batchSize) {
    this.columnVectors = columnVectors;
    this.batchSize = batchSize;
  }

  public int getBatchSize() {
    return batchSize;
  }

}
