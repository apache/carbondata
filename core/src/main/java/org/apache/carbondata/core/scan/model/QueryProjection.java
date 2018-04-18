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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

/**
 * Contains projection columns in the query
 */
public class QueryProjection {

  /**
   * List of dimensions.
   * Ex : select employee_name,department_name,sum(salary) from employee, then employee_name
   * and department_name are dimensions
   * If there is no dimensions asked in query then it would be remained as empty.
   */
  private List<ProjectionDimension> dimensions =
      new ArrayList<ProjectionDimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * List of measures.
   * Ex : select employee_name,department_name,sum(salary) from employee, then sum(salary)
   * would be measure.
   * If there is no dimensions asked in query then it would be remained as empty.
   */
  private List<ProjectionMeasure> measures =
      new ArrayList<ProjectionMeasure>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Constructor created with database name and table name.
   *
   */
  public QueryProjection() {
  }

  /**
   * @return the dimensions
   */
  public List<ProjectionDimension> getDimensions() {
    return dimensions;
  }

  public void addDimension(CarbonDimension dimension, int queryOrdinal) {
    ProjectionDimension queryDimension = new ProjectionDimension(dimension);
    queryDimension.setOrdinal(queryOrdinal);
    this.dimensions.add(queryDimension);
  }

  /**
   * @return the measures
   */
  public List<ProjectionMeasure> getMeasures() {
    return measures;
  }

  public void addMeasure(CarbonMeasure measure, int queryOrdinal) {
    ProjectionMeasure queryMeasure = new ProjectionMeasure(measure);
    queryMeasure.setOrdinal(queryOrdinal);
    this.measures.add(queryMeasure);
  }

}
