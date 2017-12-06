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
import java.util.List;

import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

public class ImplicitColumnVisitor implements ResolvedFilterInfoVisitorIntf {

  /**
   * Visitor Method will update the filter related details in visitableObj, For implicit
   * type columns the filter members will resolved directly, no need to look up in dictionary
   * since it will not be part of dictionary, directly the actual data can be taken
   * and can be set. This type of encoding is effective when the particular column
   * is having very high cardinality.
   *
   * @param visitableObj
   * @param metadata
   * @throws FilterUnsupportedException,if exception occurs while evaluating
   *                                       filter models.
   */

  @Override public void populateFilterResolvedInfo(ColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException, IOException {
    if (visitableObj instanceof DimColumnResolvedFilterInfo) {
      ColumnFilterInfo resolvedFilterObject = null;
      List<String> evaluateResultListFinal;
      try {
        evaluateResultListFinal = metadata.getExpression().evaluate(null).getListAsString();
      } catch (FilterIllegalMemberException e) {
        throw new FilterUnsupportedException(e);
      }
      resolvedFilterObject = FilterUtil
          .getImplicitColumnFilterList(evaluateResultListFinal, metadata.isIncludeFilter());
      ((DimColumnResolvedFilterInfo)visitableObj).setFilterValues(resolvedFilterObject);
    }
  }
}
