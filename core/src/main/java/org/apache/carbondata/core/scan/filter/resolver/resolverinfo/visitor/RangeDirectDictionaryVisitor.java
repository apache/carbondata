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
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;
import org.apache.carbondata.core.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

public class RangeDirectDictionaryVisitor extends CustomTypeDictionaryVisitor
    implements ResolvedFilterInfoVisitorIntf {

  /**
   * This Visitor method is used to populate the visitableObj with direct dictionary filter details
   * where the filters values will be resolve using dictionary cache.
   *
   * @param visitableObj
   * @param metadata
   * @throws FilterUnsupportedException,if exception occurs while evaluating
   * filter models.
   * @throws IOException
   * @throws FilterUnsupportedException
   */
  public void populateFilterResolvedInfo(DimColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException {
    DimColumnFilterInfo resolvedFilterObject = null;
    List<ExpressionResult> listOfExpressionResults = new ArrayList<ExpressionResult>(20);
    List<String> evaluateResultListFinal = new ArrayList<String>();
    try {
      listOfExpressionResults = ((RangeExpression) metadata.getExpression()).getLiterals();

      for (ExpressionResult result : listOfExpressionResults) {
        if (result.getString() == null) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
          continue;
        }
        evaluateResultListFinal.add(result.getString());
      }
    } catch (FilterIllegalMemberException e) {
      throw new FilterUnsupportedException(e);
    }

    resolvedFilterObject = getDirectDictionaryValKeyMemberForFilter(metadata.getColumnExpression(),
        evaluateResultListFinal, metadata.isIncludeFilter(),
        metadata.getColumnExpression().getDimension().getDataType());

    if (!metadata.isIncludeFilter() && null != resolvedFilterObject && !resolvedFilterObject
        .getFilterList().contains(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY)) {
      // Adding default surrogate key of null member inorder to not display the same while
      // displaying the report as per hive compatibility.
      resolvedFilterObject.getFilterList()
          .add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY);
      Collections.sort(resolvedFilterObject.getFilterList());
    }
    visitableObj.setFilterValues(resolvedFilterObject);
  }
}
