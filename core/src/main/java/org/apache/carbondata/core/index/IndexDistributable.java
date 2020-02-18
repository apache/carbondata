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

package org.apache.carbondata.core.index;

import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Distributable class for index.
 */
@InterfaceAudience.Internal
public abstract class IndexDistributable extends InputSplit
    implements Distributable, Serializable {

  private String tablePath;

  private Segment segment;

  private String[] locations;

  private DataMapSchema dataMapSchema;

  public String getTablePath() {
    return tablePath;
  }

  public void setTablePath(String tablePath) {
    this.tablePath = tablePath;
  }

  public Segment getSegment() {
    return segment;
  }

  public void setSegment(Segment segment) {
    this.segment = segment;
  }

  public DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  public void setDataMapSchema(DataMapSchema dataMapSchema) {
    this.dataMapSchema = dataMapSchema;
  }

  public void setLocations(String[] locations) {
    this.locations = locations;
  }

  @Override
  public String[] getLocations() {
    return locations;
  }

  @Override
  public int compareTo(Distributable o) {
    return 0;
  }

  @Override
  public long getLength() {
    return 0;
  }
}
