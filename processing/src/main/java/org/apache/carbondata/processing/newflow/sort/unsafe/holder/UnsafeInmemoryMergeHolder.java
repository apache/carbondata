/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.newflow.sort.unsafe.holder;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.comparator.UnsafeRowComparator;

/**
 * It is used for merging unsafe inmemory intermediate data
 */
public class UnsafeInmemoryMergeHolder implements Comparable<UnsafeInmemoryMergeHolder> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeInmemoryMergeHolder.class.getName());

  private int counter;

  private int actualSize;

  private UnsafeCarbonRowPage rowPage;

  private UnsafeCarbonRowForMerge currentRow;

  private long address;

  private UnsafeRowComparator comparator;

  private Object baseObject;

  public UnsafeInmemoryMergeHolder(UnsafeCarbonRowPage rowPage, byte index) {
    this.actualSize = rowPage.getBuffer().getActualSize();
    this.rowPage = rowPage;
    LOGGER.audit("Processing unsafe inmemory rows page with size : " + actualSize);
    this.comparator = new UnsafeRowComparator(rowPage);
    this.baseObject = rowPage.getDataBlock().getBaseObject();
    currentRow = new UnsafeCarbonRowForMerge();
    currentRow.index = index;
  }

  public boolean hasNext() {
    if (counter < actualSize) {
      return true;
    }
    return false;
  }

  public void readRow() {
    address = rowPage.getBuffer().get(counter);
    currentRow.address = address + rowPage.getDataBlock().getBaseOffset();
    counter++;
  }

  public UnsafeCarbonRowForMerge getRow() {
    return currentRow;
  }

  @Override public int compareTo(UnsafeInmemoryMergeHolder o) {
    return comparator.compare(currentRow, baseObject, o.getRow(), o.getBaseObject());
  }

  public int numberOfRows() {
    return actualSize;
  }

  public Object getBaseObject() {
    return baseObject;
  }

  public void close() {
    rowPage.freeMemory();
  }
}
