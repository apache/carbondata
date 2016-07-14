/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.query.carbon.executor.util;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.carbon.executor.infos.AggregatorInfo;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;

/**
 * Utility class for restructuring
 */
public class RestructureUtil {

  /**
   * Below method will be used to get the updated query dimension updation
   * means, after restructuring some dimension will be not present in older
   * table blocks in that case we need to select only those dimension out of
   * query dimension which is present in the current table block
   *
   * @param queryDimensions
   * @param tableBlockDimensions
   * @return list of query dimension which is present in the table block
   */
  public static List<QueryDimension> getUpdatedQueryDimension(List<QueryDimension> queryDimensions,
      List<CarbonDimension> tableBlockDimensions, List<CarbonDimension> tableComplexDimension) {
    List<QueryDimension> presentDimension =
        new ArrayList<QueryDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // selecting only those dimension which is present in the query
    for (QueryDimension queryDimimension : queryDimensions) {
      for (CarbonDimension tableDimension : tableBlockDimensions) {
        if (tableDimension.equals(queryDimimension.getDimension())) {
          presentDimension.add(queryDimimension);
        }
      }
    }
    for (QueryDimension queryDimimension : queryDimensions) {
      for (CarbonDimension tableDimension : tableComplexDimension) {
        if (tableDimension.equals(queryDimimension.getDimension())) {
          presentDimension.add(queryDimimension);
        }
      }
    }
    return presentDimension;
  }

  /**
   * Below method is to add dimension children for complex type dimension as
   * internally we are creating dimension column for each each complex
   * dimension so when complex query dimension request will come in the query,
   * we need to add its children as it is hidden from the user For example if
   * complex dimension is of Array of String[2] so we are storing 3 dimension
   * and when user will query for complex type i.e. array type we need to add
   * its children and then we will read respective block and create a tuple
   * based on all three dimension
   *
   * @param queryDimensions      current query dimensions
   * @param tableBlockDimensions dimensions which is present in the table block
   * @return updated dimension(after adding complex type children)
   */
  public static List<CarbonDimension> addChildrenForComplexTypeDimension(
      List<CarbonDimension> queryDimensions, List<CarbonDimension> tableBlockDimensions) {
    List<CarbonDimension> updatedQueryDimension = new ArrayList<CarbonDimension>();
    int numberOfChildren = 0;
    for (CarbonDimension queryDimension : queryDimensions) {
      // if number of child is zero, then it is not a complex dimension
      // so directly add it query dimension
      if (queryDimension.numberOfChild() == 0) {
        updatedQueryDimension.add(queryDimension);
      }
      // if number of child is more than 1 then add all its children
      numberOfChildren = queryDimension.getOrdinal() + queryDimension.numberOfChild();
      for (int j = queryDimension.getOrdinal(); j < numberOfChildren; j++) {
        updatedQueryDimension.add(tableBlockDimensions.get(j));
      }
    }
    return updatedQueryDimension;
  }

  /**
   * Below method will be used to get the aggregator info object
   * in this method some of the properties which will be extracted
   * from query measure and current block measures will be set
   *
   * @param queryMeasures        measures present in query
   * @param currentBlockMeasures current block measures
   * @return aggregator info
   */
  public static AggregatorInfo getAggregatorInfos(List<QueryMeasure> queryMeasures,
      List<CarbonMeasure> currentBlockMeasures) {
    AggregatorInfo aggregatorInfos = new AggregatorInfo();
    int numberOfMeasureInQuery = queryMeasures.size();
    int[] measureOrdinals = new int[numberOfMeasureInQuery];
    Object[] defaultValues = new Object[numberOfMeasureInQuery];
    boolean[] measureExistsInCurrentBlock = new boolean[numberOfMeasureInQuery];
    int index = 0;
    for (QueryMeasure queryMeasure : queryMeasures) {
      measureOrdinals[index] = queryMeasure.getMeasure().getOrdinal();
      // if query measure exists in current dimension measures
      // then setting measure exists is true
      // otherwise adding a default value of a measure
      if (currentBlockMeasures.contains(queryMeasure.getMeasure())) {
        measureExistsInCurrentBlock[index] = true;
      } else {
        defaultValues[index] = queryMeasure.getMeasure().getDefaultValue();
      }
      index++;
    }
    aggregatorInfos.setDefaultValues(defaultValues);
    aggregatorInfos.setMeasureOrdinals(measureOrdinals);
    aggregatorInfos.setMeasureExists(measureExistsInCurrentBlock);
    return aggregatorInfos;
  }
}
