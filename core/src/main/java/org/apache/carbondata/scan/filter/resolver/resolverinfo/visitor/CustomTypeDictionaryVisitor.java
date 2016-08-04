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
package org.apache.carbondata.scan.filter.resolver.resolverinfo.visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

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
   * @throws FilterUnsupportedException,if exception occurs while evaluating
   * filter models.
   */
  public void populateFilterResolvedInfo(DimColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException {
    DimColumnFilterInfo resolvedFilterObject = null;

    List<String> evaluateResultListFinal;
    try {
      evaluateResultListFinal = metadata.getExpression().evaluate(null).getListAsString();
    } catch (FilterIllegalMemberException e) {
      throw new FilterUnsupportedException(e);
    }
    boolean isNotTimestampType = FilterUtil.checkIfDataTypeNotTimeStamp(metadata.getExpression());
    resolvedFilterObject = getDirectDictionaryValKeyMemberForFilter(metadata.getTableIdentifier(),
        metadata.getColumnExpression(), evaluateResultListFinal, metadata.isIncludeFilter(),
        isNotTimestampType);
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

  private DimColumnFilterInfo getDirectDictionaryValKeyMemberForFilter(
      AbsoluteTableIdentifier tableIdentifier, ColumnExpression columnExpression,
      List<String> evaluateResultListFinal, boolean isIncludeFilter, boolean isNotTimestampType) {
    List<Integer> surrogates = new ArrayList<Integer>(20);
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(columnExpression.getDimension().getDataType());
    // Reading the dictionary value direct
    getSurrogateValuesForDictionary(evaluateResultListFinal, surrogates, isNotTimestampType,
        directDictionaryGenerator);

    Collections.sort(surrogates);
    DimColumnFilterInfo columnFilterInfo = null;
    if (surrogates.size() > 0) {
      columnFilterInfo = new DimColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setFilterList(surrogates);
    }
    return columnFilterInfo;
  }

  private void getSurrogateValuesForDictionary(List<String> evaluateResultListFinal,
      List<Integer> surrogates, boolean isNotTimestampType,
      DirectDictionaryGenerator directDictionaryGenerator) {
    String timeFormat = CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT;
    if (isNotTimestampType) {
      timeFormat = null;
    }
    for (String filterMember : evaluateResultListFinal) {
      surrogates
          .add(directDictionaryGenerator.generateDirectSurrogateKey(filterMember, timeFormat));
    }
  }
}
