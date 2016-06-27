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
package org.carbondata.query.filter.executer;

import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filter.resolver.RowLevelRangeFilterResolverImpl;

public class RowLevelRangeTypeExecuterFacory {

  private RowLevelRangeTypeExecuterFacory() {

  }

  /**
   * The method returns the Row Level Range fiter type instance based on
   * filter tree resolver type.
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @param dataType                     DataType
   * @return the generator instance
   */
  public static RowLevelFilterExecuterImpl getRowLevelRangeTypeExecuter(
      FilterExecuterType filterExecuterType, FilterResolverIntf filterExpressionResolverTree,
      SegmentProperties segmentProperties) {
    switch (filterExecuterType) {

      case ROWLEVEL_LESSTHAN:
        return new RowLevelRangeLessThanFiterExecuterImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvalutorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties), segmentProperties);
      case ROWLEVEL_LESSTHAN_EQUALTO:
        return new RowLevelRangeLessThanEqualFilterExecuterImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvalutorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties), segmentProperties);
      case ROWLEVEL_GREATERTHAN_EQUALTO:
        return new RowLevelRangeGrtrThanEquaToFilterExecuterImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvalutorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties), segmentProperties);
      case ROWLEVEL_GREATERTHAN:
        return new RowLevelRangeGrtThanFiterExecuterImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvalutorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties), segmentProperties);
      default:
        // Scenario wont come logic must break
        return null;

    }
  }

}
