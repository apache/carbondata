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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Store the data map row @{@link DataMapRow} data to memory.
 */
public class SafeMemoryDMStore extends AbstractMemoryDMStore {

  /**
   * holds all blocklets metadata in memory
   */
  private List<DataMapRow> dataMapRows =
      new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private int runningLength;

  /**
   * Add the index row to dataMapRows, basically to in memory.
   *
   * @param indexRow
   * @return
   */
  @Override
  public void addIndexRow(CarbonRowSchema[] schema, DataMapRow indexRow) throws MemoryException {
    dataMapRows.add(indexRow);
    runningLength += indexRow.getTotalSizeInBytes();
  }

  @Override
  public DataMapRow getDataMapRow(CarbonRowSchema[] schema, int index) {
    assert (index < dataMapRows.size());
    return dataMapRows.get(index);
  }

  @Override
  public void freeMemory() {
    if (!isMemoryFreed) {
      if (null != dataMapRows) {
        dataMapRows.clear();
      }
      isMemoryFreed = true;
    }
  }

  @Override
  public int getMemoryUsed() {
    return runningLength;
  }

  @Override
  public int getRowCount() {
    return dataMapRows.size();
  }

  @Override
  public UnsafeMemoryDMStore convertToUnsafeDMStore(CarbonRowSchema[] schema)
      throws MemoryException {
    setSchemaDataType(schema);
    UnsafeMemoryDMStore unsafeMemoryDMStore = new UnsafeMemoryDMStore();
    for (DataMapRow dataMapRow : dataMapRows) {
      dataMapRow.setSchemas(schema);
      unsafeMemoryDMStore.addIndexRow(schema, dataMapRow);
    }
    unsafeMemoryDMStore.finishWriting();
    return unsafeMemoryDMStore;
  }

  /**
   * Set the dataType to the schema. Needed in case of serialization / deserialization
   */
  private void setSchemaDataType(CarbonRowSchema[] schema) {
    for (CarbonRowSchema carbonRowSchema : schema) {
      carbonRowSchema.setDataType(DataTypeUtil.valueOf(carbonRowSchema.getDataType(), 0, 0));
    }
  }

}