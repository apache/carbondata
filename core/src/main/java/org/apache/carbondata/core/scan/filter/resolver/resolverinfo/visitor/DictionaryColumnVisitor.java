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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

public class DictionaryColumnVisitor implements ResolvedFilterInfoVisitorIntf {

  /**
   * This Visitor method is used to populate the visitableObj with direct dictionary filter details
   * where the filters values will be resolve using dictionary cache.
   *
   * @param visitableObj
   * @param metadata
   * @throws FilterUnsupportedException,if exception occurs while evaluating
   *                                       filter models.
   * @throws IOException
   * @throws FilterUnsupportedException
   */
  public void populateFilterResolvedInfo(ColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException, IOException {
    if (visitableObj instanceof DimColumnResolvedFilterInfo) {
      DimColumnResolvedFilterInfo resolveDimension = (DimColumnResolvedFilterInfo) visitableObj;
      ColumnFilterInfo resolvedFilterObject = null;
      List<String> evaluateResultListFinal;
      try {
        evaluateResultListFinal = metadata.getExpression().evaluate(null).getListAsString();
      } catch (FilterIllegalMemberException e) {
        throw new FilterUnsupportedException(e);
      }

      resolvedFilterObject = FilterUtil
          .getFilterValues(metadata.getTableIdentifier(), metadata.getColumnExpression(),
              evaluateResultListFinal, metadata.isIncludeFilter());
      if (!metadata.isIncludeFilter() && null != resolvedFilterObject) {
        // Adding default surrogate key of null member inorder to not display the same while
        // displaying the report as per hive compatibility.
        // first check of surrogate key for null value is already added then
        // no need to add again otherwise result will be wrong in case of exclude filter
        // this is because two times it will flip the same bit
        if (!resolvedFilterObject.getExcludeFilterList()
            .contains(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY)) {
          resolvedFilterObject.getExcludeFilterList()
              .add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY);
        }
        Collections.sort(resolvedFilterObject.getExcludeFilterList());
      }
      resolveDimension.setFilterValues(resolvedFilterObject);
    }
  }
}
