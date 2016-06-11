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
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.metadata.FilterResolverMetadata;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

public class DictionaryColumnVisitor implements ResolvedFilterInfoVisitorIntf {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DictionaryColumnVisitor.class.getName());

  /**
   * This Visitor method is used to populate the visitableObj with direct dictionary filter details
   * where the filters values will be resolve using dictionary cache.
   *
   * @param visitableObj
   * @param metadata
   * @throws QueryExecutionException
   */
  public void populateFilterResolvedInfo(DimColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException {
    DimColumnFilterInfo resolvedFilterObject = null;
    List<String> evaluateResultListFinal =
        metadata.getExpression().evaluate(null).getListAsString();
    try {
      resolvedFilterObject = FilterUtil
          .getFilterValues(metadata.getTableIdentifier(), metadata.getColumnExpression(),
              evaluateResultListFinal, metadata.isIncludeFilter());
    } catch (QueryExecutionException e) {
      throw new FilterUnsupportedException(e);
    }
    visitableObj.setFilterValues(resolvedFilterObject);
  }
}
