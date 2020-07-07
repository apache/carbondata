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
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.CarbonProperties;

public class CustomTypeDictionaryVisitor implements ResolvedFilterInfoVisitorIntf {

  /**
   * This Visitor method is been used to resolve or populate the filter details
   * by using custom type dictionary value, the filter members will be resolved using
   * custom type function which will generate dictionary for the direct column type filter members
   *
   * @param visitableObj
   * @param metadata
   * @throws FilterUnsupportedException,if exception occurs while evaluating
   *                                       filter models.
   */
  public void populateFilterResolvedInfo(ColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws FilterUnsupportedException {
    ColumnFilterInfo resolvedFilterObject = null;
    if (visitableObj instanceof DimColumnResolvedFilterInfo) {
      DimColumnResolvedFilterInfo resolveDimension = (DimColumnResolvedFilterInfo) visitableObj;
      List<String> evaluateResultListFinal;
      try {
        evaluateResultListFinal = metadata.getExpression().evaluate(null).getListAsString();
      } catch (FilterIllegalMemberException e) {
        throw new FilterUnsupportedException(e);
      }
      resolvedFilterObject =
          getDirectDictionaryValKeyMemberForFilter(metadata.getColumnExpression(),
              evaluateResultListFinal, metadata.isIncludeFilter(),
              metadata.getColumnExpression().getDimension().getDataType());
      if (!metadata.isIncludeFilter() && null != resolvedFilterObject && !resolvedFilterObject
          .getExcludeFilterList()
          .contains(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY)) {
        // Adding default surrogate key of null member inorder to not display the same while
        // displaying the report as per hive compatibility.
        resolvedFilterObject.getExcludeFilterList()
            .add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY);
        Collections.sort(resolvedFilterObject.getExcludeFilterList());
      }
      resolveDimension.setFilterValues(resolvedFilterObject);
    }
  }

  protected ColumnFilterInfo getDirectDictionaryValKeyMemberForFilter(
      ColumnExpression columnExpression, List<String> evaluateResultListFinal,
      boolean isIncludeFilter, DataType dataType) {
    List<Integer> surrogates = new ArrayList<Integer>(20);
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(columnExpression.getDimension().getDataType());
    // Reading the dictionary value direct
    getSurrogateValuesForDictionary(evaluateResultListFinal, surrogates, directDictionaryGenerator,
        dataType);

    Collections.sort(surrogates);
    ColumnFilterInfo columnFilterInfo = null;
    if (surrogates.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      if (!isIncludeFilter) {
        columnFilterInfo.setExcludeFilterList(surrogates);
      } else {
        columnFilterInfo.setFilterList(surrogates);
      }
    }
    return columnFilterInfo;
  }

  private void getSurrogateValuesForDictionary(List<String> evaluateResultListFinal,
      List<Integer> surrogates, DirectDictionaryGenerator directDictionaryGenerator,
      DataType dataType) {
    String timeFormat = null;
    if (dataType == DataTypes.DATE) {
      timeFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
              CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
    } else {
      timeFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    }
    for (String filterMember : evaluateResultListFinal) {
      surrogates
          .add(directDictionaryGenerator.generateDirectSurrogateKey(filterMember, timeFormat));
    }
  }
}
