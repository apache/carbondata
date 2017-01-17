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
package org.apache.carbondata.core.datastore.chunk;

import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.metadata.blocklet.datachunk.PresenceMeta;

/**
 * Holder for measure column chunk
 * it will have data and its attributes which will
 * be required for processing
 */
public class MeasureColumnDataChunk {

  /**
   * measure chunk
   */
  private CarbonReadDataHolder measureDataHolder;

  /**
   * below to hold null value holds this information
   * about the null value index this will be helpful in case of
   * to remove the null value while aggregation
   */
  private PresenceMeta nullValueIndexHolder;

  /**
   * @return the measureDataHolder
   */
  public CarbonReadDataHolder getMeasureDataHolder() {
    return measureDataHolder;
  }

  /**
   * @param measureDataHolder the measureDataHolder to set
   */
  public void setMeasureDataHolder(CarbonReadDataHolder measureDataHolder) {
    this.measureDataHolder = measureDataHolder;
  }

  /**
   * @return the nullValueIndexHolder
   */
  public PresenceMeta getNullValueIndexHolder() {
    return nullValueIndexHolder;
  }

  /**
   * @param nullValueIndexHolder the nullValueIndexHolder to set
   */
  public void setNullValueIndexHolder(PresenceMeta nullValueIndexHolder) {
    this.nullValueIndexHolder = nullValueIndexHolder;
  }

  public void freeMemory() {
    this.measureDataHolder.freeMemory();
  }
}
