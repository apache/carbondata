package org.apache.carbondata.common.iudprocessor.iuddata;

import java.io.Serializable;

/**
 * VO class Details for block.
 */
public class RowCountDetailsVO implements Serializable {

  private static final long serialVersionUID = 1206104914918491749L;

  private long totalNumberOfRows;

  private long deletedRowsInBlock;

  public RowCountDetailsVO(long totalNumberOfRows, long deletedRowsInBlock) {
    this.totalNumberOfRows = totalNumberOfRows;
    this.deletedRowsInBlock = deletedRowsInBlock;
  }

  public long getTotalNumberOfRows() {
    return totalNumberOfRows;
  }

  public long getDeletedRowsInBlock() {
    return deletedRowsInBlock;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RowCountDetailsVO that = (RowCountDetailsVO) o;

    if (totalNumberOfRows != that.totalNumberOfRows) return false;
    return deletedRowsInBlock == that.deletedRowsInBlock;

  }

  @Override public int hashCode() {
    int result = (int) (totalNumberOfRows ^ (totalNumberOfRows >>> 32));
    result = 31 * result + (int) (deletedRowsInBlock ^ (deletedRowsInBlock >>> 32));
    return result;
  }
}
