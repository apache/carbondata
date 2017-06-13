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
package org.apache.carbondata.processing.newflow.sort.unsafe.sort;

import org.apache.carbondata.core.memory.IntPointerBuffer;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeCarbonRow;

import org.apache.spark.util.collection.SortDataFormat;

/**
 * Interface implementation for utilities to sort the data.
 */
public class UnsafeIntSortDataFormat
    extends SortDataFormat<UnsafeCarbonRow, IntPointerBuffer> {

  private UnsafeCarbonRowPage page;

  public UnsafeIntSortDataFormat(UnsafeCarbonRowPage page) {
    this.page = page;
  }

  @Override public UnsafeCarbonRow getKey(IntPointerBuffer data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override public UnsafeCarbonRow newKey() {
    return new UnsafeCarbonRow();
  }

  @Override
  public UnsafeCarbonRow getKey(IntPointerBuffer data, int pos, UnsafeCarbonRow reuse) {
    reuse.address = data.get(pos) + page.getDataBlock().getBaseOffset();
    return reuse;
  }

  @Override public void swap(IntPointerBuffer data, int pos0, int pos1) {
    int tempPointer = data.get(pos0);
    data.set(pos0, data.get(pos1));
    data.set(pos1, tempPointer);
  }

  @Override
  public void copyElement(IntPointerBuffer src, int srcPos, IntPointerBuffer dst, int dstPos) {
    dst.set(dstPos, src.get(srcPos));
  }

  @Override
  public void copyRange(IntPointerBuffer src, int srcPos, IntPointerBuffer dst, int dstPos,
      int length) {
    System.arraycopy(src.getPointerBlock(), srcPos, dst.getPointerBlock(), dstPos, length);
  }

  @Override public IntPointerBuffer allocate(int length) {
    return new IntPointerBuffer(length);
  }
}
