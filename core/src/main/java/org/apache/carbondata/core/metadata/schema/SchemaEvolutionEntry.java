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

package org.apache.carbondata.core.metadata.schema;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Store the information about the schema evolution
 */
public class SchemaEvolutionEntry implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -7619477063676325276L;

  /**
   * time stamp of restructuring
   */
  private long timeStamp;

  /**
   * new column added in restructuring
   */
  private List<ColumnSchema> added;

  /**
   * column removed in restructuring
   */
  private List<ColumnSchema> removed;

  /**
   * @return the timeStamp
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * @param timeStamp the timeStamp to set
   */
  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  /**
   * @return the added
   */
  public List<ColumnSchema> getAdded() {
    return added;
  }

  /**
   * @param added the added to set
   */
  public void setAdded(List<ColumnSchema> added) {
    this.added = added;
  }

  /**
   * @return the removed
   */
  public List<ColumnSchema> getRemoved() {
    return removed;
  }

  /**
   * @param removed the removed to set
   */
  public void setRemoved(List<ColumnSchema> removed) {
    this.removed = removed;
  }

}
