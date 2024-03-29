/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.loading.sort.unsafe.holder;

import java.util.Comparator;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.sort.sortdata.FileMergeSortComparator;

import org.apache.log4j.Logger;

public class UnsafeInmemoryHolder implements SortTempChunkHolder {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(UnsafeInmemoryHolder.class.getName());

  private int counter;

  private int actualSize;

  private UnsafeCarbonRowPage rowPage;

  private IntermediateSortTempRow currentRow;

  private long address;

  private Comparator<IntermediateSortTempRow> comparator;

  public UnsafeInmemoryHolder(UnsafeCarbonRowPage rowPage) {
    this.actualSize = rowPage.getBuffer().getActualSize();
    this.rowPage = rowPage;
    LOGGER.info("Processing unsafe inmemory rows page with size : " + actualSize);
    this.comparator =
        new FileMergeSortComparator(rowPage.getTableFieldStat().getNoDictSchemaDataType(),
            rowPage.getTableFieldStat().getNoDictSortColumnSchemaOrderMapping(),
            rowPage.getTableFieldStat().getNoDictSortColIdxSchemaOrderMapping(),
            rowPage.getTableFieldStat().getDictSortColIdxSchemaOrderMapping());
    this.rowPage.setReadConvertedNoSortField();
  }

  @Override
  public boolean hasNext() {
    if (counter < actualSize) {
      return true;
    }
    return false;
  }

  @Override
  public void readRow() {
    address = rowPage.getBuffer().get(counter);
    currentRow = rowPage.getRow(address + rowPage.getDataBlock().getBaseOffset());
    counter++;
  }

  @Override
  public IntermediateSortTempRow getRow() {
    return currentRow;
  }

  @Override
  public int compareTo(SortTempChunkHolder o) {
    return comparator.compare(currentRow, o.getRow());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof UnsafeInmemoryHolder)) {
      return false;
    }

    UnsafeInmemoryHolder o = (UnsafeInmemoryHolder)obj;

    return this == o;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public int numberOfRows() {
    return actualSize;
  }

  @Override
  public void close() {
    rowPage.freeMemory();
  }
}
