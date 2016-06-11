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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.metadata.FilterResolverMetadata;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

public class CustomTypeDictionaryVisitor implements ResolvedFilterInfoVisitorIntf {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CustomTypeDictionaryVisitor.class.getName());

  /**
   * This Visitor method is been used to resolve or populate the filter details
   * by using custom type dictionary value, the filter membrers will be resolved using
   * custom type function which will generate dictionary for the direct column type filter members
   *
   * @param visitableObj
   * @param metadata
   * @throws FilterUnsupportedException
   */
  public void populateFilterResolvedInfo(DimColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException {
    DimColumnFilterInfo resolvedFilterObject = null;

    List<String> evaluateResultListFinal =
        metadata.getExpression().evaluate(null).getListAsString();
    resolvedFilterObject = getDirectDictionaryValKeyMemberForFilter(metadata.getTableIdentifier(),
        metadata.getColumnExpression(), evaluateResultListFinal, metadata.isIncludeFilter());
    visitableObj.setFilterValues(resolvedFilterObject);
  }

  private DimColumnFilterInfo getDirectDictionaryValKeyMemberForFilter(
      AbsoluteTableIdentifier tableIdentifier, ColumnExpression columnExpression,
      List<String> evaluateResultListFinal, boolean isIncludeFilter) {
    List<Integer> surrogates = new ArrayList<Integer>(20);
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(columnExpression.getDimension().getDataType());
    // Reading the dictionary value direct
    for (String filterMember : evaluateResultListFinal) {
      surrogates.add(directDictionaryGenerator.generateDirectSurrogateKey(filterMember));
    }
    Collections.sort(surrogates);
    DimColumnFilterInfo columnFilterInfo = null;
    if (surrogates.size() > 0) {
      columnFilterInfo = new DimColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setFilterList(surrogates);
    }
    return columnFilterInfo;
  }

}
