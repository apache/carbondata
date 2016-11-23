package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

/**
 * Interface for merging temporary sort files/ inmemory data
 */
public interface SortTempChunkHolder extends Comparable<SortTempChunkHolder> {

  boolean hasNext();

  void readRow()  throws CarbonSortKeyAndGroupByException;

  Object[] getRow();

  int numberOfRows();

  void close();
}
