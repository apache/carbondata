package org.apache.carbondata.scan.result.vector;

public class CarbonColumnarBatch {

  public CarbonColumnVector[] columnVectors;

  private int batchSize;

  private int actualSize;

  private int rowCounter;

  public CarbonColumnarBatch(CarbonColumnVector[] columnVectors, int batchSize) {
    this.columnVectors = columnVectors;
    this.batchSize = batchSize;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getActualSize() {
    return actualSize;
  }

  public void setActualSize(int actualSize) {
    this.actualSize = actualSize;
  }

  public void reset() {
    actualSize = 0;
    rowCounter = 0;
    for (int i = 0; i < columnVectors.length; i++) {
      columnVectors[i].reset();
    }
  }

  public int getRowCounter() {
    return rowCounter;
  }

  public void setRowCounter(int rowCounter) {
    this.rowCounter = rowCounter;
  }
}
