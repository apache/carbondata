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
package org.carbondata.query.filter.resolver.resolverinfo.visitor;

import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.metadata.FilterResolverMetadata;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

public class NoDictionaryTypeVisitor implements ResolvedFilterInfoVisitorIntf {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(NoDictionaryTypeVisitor.class.getName());

  /**
   * Visitor Method will update the filter related details in visitableObj, For no dictionary
   * type columns the filter members will resolved directly, no need to look up in dictionary
   * since it will not be part of dictionary, directly the actual data can be converted as
   * byte[] and can be set. this type of encoding is effective when the particular column
   * is having very high cardinality.
   *
   * @param visitableObj
   * @param metadata
   */
  public void populateFilterResolvedInfo(DimColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) {
    DimColumnFilterInfo resolvedFilterObject = null;
    try {
      List<String> evaluateResultListFinal =
          metadata.getExpression().evaluate(null).getListAsString();
      resolvedFilterObject = FilterUtil
          .getNoDictionaryValKeyMemberForFilter(metadata.getTableIdentifier(),
              metadata.getColumnExpression(), evaluateResultListFinal, metadata.isIncludeFilter());
      visitableObj.setFilterValues(resolvedFilterObject);
    } catch (FilterUnsupportedException e) {
      LOGGER.audit(e.getMessage());
    }
  }

}
