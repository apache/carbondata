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
package org.apache.carbondata.processing.newflow.sort.unsafe.holder;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.sortandgroupby.sortdata.NewRowComparator;

public class UnsafeInmemoryHolder implements SortTempChunkHolder {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeInmemoryHolder.class.getName());

  private int counter;

  private int actualSize;

  private UnsafeCarbonRowPage rowPage;

  private Object[] currentRow;

  private long address;

  private NewRowComparator comparator;

  private int columnSize;

  public UnsafeInmemoryHolder(UnsafeCarbonRowPage rowPage, int columnSize) {
    this.actualSize = rowPage.getBuffer().getActualSize();
    this.rowPage = rowPage;
    LOGGER.audit("Processing unsafe inmemory rows page with size : " + actualSize);
    this.comparator = new NewRowComparator(rowPage.getNoDictionaryDimensionMapping());
    this.columnSize = columnSize;
  }

  public boolean hasNext() {
    if (counter < actualSize) {
      return true;
    }
    return false;
  }

  public void readRow() {
    currentRow = new Object[columnSize];
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
