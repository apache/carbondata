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

package org.apache.carbondata.core.scan.filter.resolver.resolverinfo.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

public class MeasureColumnVisitor implements ResolvedFilterInfoVisitorIntf {

  /**
   * Visitor Method will update the filter related details in visitableObj, For no dictionary
   * type columns the filter members will resolved directly, no need to look up in dictionary
   * since it will not be part of dictionary, directly the actual data can be converted as
   * byte[] and can be set. this type of encoding is effective when the particular column
   * is having very high cardinality.
   *
   * @param visitableObj
   * @param metadata
   * @throws FilterUnsupportedException,if exception occurs while evaluating
   *                                       filter models.
   */
  public void populateFilterResolvedInfo(ColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException {
    if (visitableObj instanceof MeasureColumnResolvedFilterInfo) {
      MeasureColumnResolvedFilterInfo resolveDimension =
          (MeasureColumnResolvedFilterInfo) visitableObj;
      ColumnFilterInfo resolvedFilterObject = null;
      List<String> evaluateResultListFinal = new ArrayList<>(1);
      try {
        // handling for is null case scenarios
        if (metadata.getExpression() instanceof EqualToExpression) {
          EqualToExpression expression = (EqualToExpression) metadata.getExpression();
          if (expression.isNull) {
            evaluateResultListFinal = new ArrayList<>(1);
            evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
          }
        } else {
          evaluateResultListFinal = metadata.getExpression().evaluate(null).getListAsString();
        }
        // Adding default  null member inorder to not display the same while
        // displaying the report as per hive compatibility.
        if (!metadata.isIncludeFilter() && !evaluateResultListFinal
            .contains(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        }
      } catch (FilterIllegalMemberException e) {
        throw new FilterUnsupportedException(e);
      }
      resolvedFilterObject = FilterUtil
          .getMeasureValKeyMemberForFilter(evaluateResultListFinal, metadata.isIncludeFilter(),
              metadata.getColumnExpression().getDataType(), resolveDimension.getMeasure());
      resolveDimension.setFilterValues(resolvedFilterObject);
    }
  }
}
