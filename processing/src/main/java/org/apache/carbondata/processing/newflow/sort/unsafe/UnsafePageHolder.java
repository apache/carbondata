package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.carbondata.processing.sortandgroupby.sortdata.NewRowComparator;

public class UnsafePageHolder implements SortTempChunkHolder {

  private int counter;

  private int actualSize;

  private UnsafeCarbonRowPage rowPage;

  private Object[] currentRow;

  private long address;

  private NewRowComparator comparator;

  public UnsafePageHolder(UnsafeCarbonRowPage rowPage, int columnSize) {
    this.actualSize = rowPage.getBuffer().getActualSize();
    this.rowPage = rowPage;
    this.comparator = new NewRowComparator(rowPage.getNoDictionaryDimensionMapping());
    currentRow = new Object[columnSize];
  }

  public boolean hasNext() {
    if (counter < actualSize) {
      return true;
    }
    return false;
  }

  public void readRow() {
    address = rowPage.getBuffer().get(counter);
    rowPage.getRow(address + rowPage.getDataBlock().getBaseOffset(), currentRow);
    counter++;
  }

  public Object[] getRow() {
    return currentRow;
  }

  @Override public int compareTo(SortTempChunkHolder o) {
    return comparator.compare(currentRow, o.getRow());
  }

  public int numberOfRows() {
    return actualSize;
  }

  public void close() {
    rowPage.freeMemory();
  }
}
