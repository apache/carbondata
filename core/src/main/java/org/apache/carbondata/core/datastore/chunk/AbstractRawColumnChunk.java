package org.apache.carbondata.core.datastore.chunk;

/**
 * It contains group of uncompressed blocklets on one column.
 */
public abstract class AbstractRawColumnChunk {

  private byte[][] minValues;

  private byte[][] maxValues;

  protected byte[] rawData;

  private int[] lengths;

  private int[] offsets;

  private int[] rowCount;

  protected int pagesCount;

  protected int blockId;

  protected int offSet;

  protected int length;

  public AbstractRawColumnChunk(int blockId, byte[] rawData, int offSet, int length) {
    this.blockId = blockId;
    this.rawData = rawData;
    this.offSet = offSet;
    this.length = length;
  }

  public byte[][] getMinValues() {
    return minValues;
  }

  public void setMinValues(byte[][] minValues) {
    this.minValues = minValues;
  }

  public byte[][] getMaxValues() {
    return maxValues;
  }

  public void setMaxValues(byte[][] maxValues) {
    this.maxValues = maxValues;
  }

  public byte[] getRawData() {
    return rawData;
  }

  public void setRawData(byte[] rawData) {
    this.rawData = rawData;
  }

  public int[] getLengths() {
    return lengths;
  }

  public void setLengths(int[] lengths) {
    this.lengths = lengths;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public void setOffsets(int[] offsets) {
    this.offsets = offsets;
  }

  public int getPagesCount() {
    return pagesCount;
  }

  public void setPagesCount(int pagesCount) {
    this.pagesCount = pagesCount;
  }

  public int[] getRowCount() {
    return rowCount;
  }

  public void setRowCount(int[] rowCount) {
    this.rowCount = rowCount;
  }

  public abstract void freeMemory();

}
