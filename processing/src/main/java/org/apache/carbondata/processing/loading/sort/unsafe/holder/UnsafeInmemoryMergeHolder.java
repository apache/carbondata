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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.comparator.UnsafeRowComparator;

import org.apache.log4j.Logger;

/**
 * It is used for merging unsafe inmemory intermediate data
 */
public class UnsafeInmemoryMergeHolder implements Comparable<UnsafeInmemoryMergeHolder> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(UnsafeInmemoryMergeHolder.class.getName());

  private int counter;

  private int actualSize;

  private UnsafeCarbonRowPage rowPage;

  private UnsafeCarbonRowForMerge currentRow;

  private long address;

  private UnsafeRowComparator comparator;

  private Object baseObject;

  private byte index;

  public UnsafeInmemoryMergeHolder(UnsafeCarbonRowPage rowPage, byte index) {
    this.actualSize = rowPage.getBuffer().getActualSize();
    this.rowPage = rowPage;
    LOGGER.info("Processing unsafe inmemory rows page with size : " + actualSize);
    this.comparator = new UnsafeRowComparator(rowPage);
    this.baseObject = rowPage.getDataBlock().getBaseObject();
    currentRow = new UnsafeCarbonRowForMerge();
    this.index = index;
  }

  public boolean hasNext() {
    if (counter < actualSize) {
      return true;
    }
    return false;
  }

  public void readRow() {
    address = rowPage.getBuffer().get(counter);
    currentRow = new UnsafeCarbonRowForMerge();
    currentRow.address = address + rowPage.getDataBlock().getBaseOffset();
    currentRow.index = index;
    counter++;
  }

  public UnsafeCarbonRowForMerge getRow() {
    return currentRow;
  }

  @Override public int compareTo(UnsafeInmemoryMergeHolder o) {
    return comparator.compare(currentRow, baseObject, o.getRow(), o.getBaseObject());
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof UnsafeInmemoryMergeHolder)) {
      return false;
    }

    UnsafeInmemoryMergeHolder o = (UnsafeInmemoryMergeHolder)obj;
    return this == o;
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  public Object getBaseObject() {
    return baseObject;
  }

  public void close() {
    rowPage.freeMemory();
  }
}
