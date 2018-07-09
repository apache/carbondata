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

package org.apache.carbondata.core.indexstore;

import java.io.Serializable;

import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

/**
 * Store the data map row @{@link DataMapRow}
 */
public abstract class AbstractMemoryDMStore implements Serializable {

  protected boolean isMemoryFreed;

  protected final long taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  public abstract void addIndexRow(CarbonRowSchema[] schema, DataMapRow indexRow)
      throws MemoryException;

  public abstract DataMapRow getDataMapRow(CarbonRowSchema[] schema, int index);

  public abstract void freeMemory();

  public abstract int getMemoryUsed();

  public abstract int getRowCount();

  public void finishWriting() throws MemoryException {
    // do nothing in default implementation
  }

  public UnsafeMemoryDMStore convertToUnsafeDMStore(CarbonRowSchema[] schema)
      throws MemoryException {
    throw new UnsupportedOperationException("Operation not allowed");
  }
}