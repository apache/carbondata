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

import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

/**
 * query plan measure, this class will holds the information
 * about measure present in the query, this is done to avoid the serialization
 * of the heavy object
 */
public class ProjectionMeasure extends ProjectionColumn {

  /**
   * actual carbon measure object
   */
  private CarbonMeasure measure;

  public ProjectionMeasure(CarbonMeasure measure) {
    super(measure.getColName());
    this.measure = measure;
  }

  /**
   * @return the measure
   */
  public CarbonMeasure getMeasure() {
    return measure;
  }

}
