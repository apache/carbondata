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

package org.apache.carbondata.core.scan.filter.executer;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.scan.filter.intf.FilterExecutorType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;

public class RowLevelRangeTypeExecutorFactory {

  private RowLevelRangeTypeExecutorFactory() {

  }

  /**
   * The method returns the Row Level Range filter type instance based on
   * filter tree resolver type.
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return the generator instance
   */
  public static RowLevelFilterExecutorImpl getRowLevelRangeTypeExecutor(
      FilterExecutorType filterExecutorType, FilterResolverIntf filterExpressionResolverTree,
      SegmentProperties segmentProperties) {
    switch (filterExecutorType) {

      case ROWLEVEL_LESSTHAN:
        return new RowLevelRangeLessThanFilterExecutorImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvaluatorInfoList(),
            filterExpressionResolverTree.getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
            .getMeasureFilterRangeValues(), segmentProperties);
      case ROWLEVEL_LESSTHAN_EQUALTO:
        return new RowLevelRangeLessThanEqualFilterExecutorImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvaluatorInfoList(),
            filterExpressionResolverTree.getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMeasureFilterRangeValues(), segmentProperties);
      case ROWLEVEL_GREATERTHAN_EQUALTO:
        return new RowLevelRangeGreaterThanEqualFilterExecutorImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvaluatorInfoList(),
            filterExpressionResolverTree.getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMeasureFilterRangeValues(), segmentProperties);
      case ROWLEVEL_GREATERTHAN:
        return new RowLevelRangeGreaterThanFilterExecutorImpl(
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getDimColEvaluatorInfoList(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMsrColEvaluatorInfoList(),
            filterExpressionResolverTree.getFilterExpression(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getFilterRangeValues(segmentProperties),
            ((RowLevelRangeFilterResolverImpl) filterExpressionResolverTree)
                .getMeasureFilterRangeValues(), segmentProperties);
      default:
        // Scenario wont come logic must break
        return null;

    }
  }

}
