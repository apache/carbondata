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

package org.apache.carbondata.core.memory;

import javax.annotation.Nullable;


/**
 * Code ported from Apache Spark {org.apache.spark.unsafe.memory} package
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class MemoryBlock extends MemoryLocation {

  private final long length;

  /**
   * whether freed or not
   */
  private boolean isFreed;

  /**
   * Whether it is offheap or onheap memory type
   */
  private MemoryType memoryType;

  public MemoryBlock(@Nullable Object obj, long offset, long length, MemoryType memoryType) {
    super(obj, offset);
    this.length = length;
    this.isFreed = false;
    this.memoryType = memoryType;
  }

  /**
   * Returns the size of the memory block.
   */
  public long size() {
    return length;
  }

  public boolean isFreedStatus() {
    return this.isFreed;
  }

  public void setFreedStatus(boolean freedStatus) {
    this.isFreed = freedStatus;
  }

  public MemoryType getMemoryType() {
    return memoryType;
  }
}
