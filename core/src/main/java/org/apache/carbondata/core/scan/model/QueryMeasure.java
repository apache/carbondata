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

package org.apache.carbondata.core.scan.model;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

/**
 * query plan measure, this class will holds the information
 * about measure present in the query, this is done to avoid the serialization
 * of the heavy object
 */
public class QueryMeasure extends QueryColumn implements Serializable {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 1035512411375495414L;

  /**
   * actual carbon measure object
   */
  private transient CarbonMeasure measure;

  public QueryMeasure(String columnName) {
    super(columnName);
  }

  /**
   * @return the measure
   */
  public CarbonMeasure getMeasure() {
    return measure;
  }

  /**
   * @param measure the measure to set
   */
  public void setMeasure(CarbonMeasure measure) {
    this.measure = measure;
  }

}
