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
package org.apache.carbondata.core.metadata.blocklet;

import java.io.Serializable;

/**
 * Class holds the information about the segment information
 */
public class SegmentInfo implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -1749874611112709431L;

  /**
   * cardinality of each columns
   * column which is not participating in the multidimensional key cardinality will be -1;
   */
  private int[] columnCardinality;

  /**
   * @return the columnCardinality
   */
  public int[] getColumnCardinality() {
    return columnCardinality;
  }

  /**
   * @param columnCardinality the columnCardinality to set
   */
  public void setColumnCardinality(int[] columnCardinality) {
    this.columnCardinality = columnCardinality;
  }

}
