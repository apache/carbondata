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

import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * Abstract class for restructure
 */
public abstract class RestructureEvaluatorImpl implements FilterExecuter {

  /**
   * This method will check whether a default value for the non-existing column is present
   * in the filter values list
   *
   * @param dimColumnEvaluatorInfo
   * @return
   */
  protected boolean isDimensionDefaultValuePresentInFilterValues(
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) {
    boolean isDefaultValuePresentInFilterValues = false;
    ColumnFilterInfo filterValues = dimColumnEvaluatorInfo.getFilterValues();
    CarbonDimension dimension = dimColumnEvaluatorInfo.getDimension();
    byte[] defaultValue = dimension.getDefaultValue();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
    if (dimension.getDataType() != DataTypes.DATE) {
      // for no dictionary cases
      // 3 cases: is NUll, is Not Null and filter on default value of newly added column
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
      if (null == defaultValue && dimension.getDataType() == DataTypes.STRING) {
        // default value for case where user gives is Null condition
        defaultValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      } else if (null == defaultValue) {
        defaultValue = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
      }
      List<byte[]> noDictionaryFilterValuesList = filterValues.getNoDictionaryFilterValuesList();
      for (byte[] filterValue : noDictionaryFilterValuesList) {
        int compare = FilterUtil.compareValues(filterValue, defaultValue, dimension, true);
        if (compare == 0) {
          isDefaultValuePresentInFilterValues = true;
          break;
        }
      }
    } else {
      // for dictionary and direct dictionary cases
      // 3 cases: is NUll, is Not Null and filter on default value of newly added column
      int defaultSurrogateValueToCompare = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
      if (null != defaultValue) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
        if (dimension.getDataType() == DataTypes.DATE) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2277
          DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(dimension.getDataType());
          if (directDictionaryGenerator != null) {
            String value =
                new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
            defaultSurrogateValueToCompare = Integer.parseInt(value);
          }
        } else {
          defaultSurrogateValueToCompare++;
        }
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1704
      List<Integer> filterList = null;
      if (filterValues.isIncludeFilter() && !filterValues.isOptimized()) {
        filterList = filterValues.getFilterList();
      } else {
        filterList = filterValues.getExcludeFilterList();
      }
      for (Integer filterValue : filterList) {
        if (defaultSurrogateValueToCompare == filterValue) {
          isDefaultValuePresentInFilterValues = true;
          break;
        }
      }
    }
    return isDefaultValuePresentInFilterValues;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      throws FilterUnsupportedException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3014
    throw new FilterUnsupportedException("Unsupported RestructureEvaluatorImpl on pune pages");
  }

  /**
   * This method will check whether a default value for the non-existing column is present
   * in the filter values list
   *
   * @param measureColumnResolvedFilterInfo
   * @return
   */
  protected boolean isMeasureDefaultValuePresentInFilterValues(
      MeasureColumnResolvedFilterInfo measureColumnResolvedFilterInfo) {
    boolean isDefaultValuePresentInFilterValues = false;
    ColumnFilterInfo filterValues = measureColumnResolvedFilterInfo.getFilterValues();
    CarbonMeasure measure = measureColumnResolvedFilterInfo.getMeasure();
    SerializableComparator comparator =
        Comparator.getComparatorByDataTypeForMeasure(measure.getDataType());
    Object defaultValue = null;
    if (null != measure.getDefaultValue()) {
      // default value for case where user gives is Null condition
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2277
      defaultValue = RestructureUtil
          .getMeasureDefaultValue(measure.getColumnSchema(), measure.getDefaultValue());
    }
    List<Object> measureFilterValuesList = filterValues.getMeasuresFilterValuesList();
    for (Object filterValue : measureFilterValuesList) {
      int compare = comparator.compare(defaultValue, filterValue);
      if (compare == 0) {
        isDefaultValuePresentInFilterValues = true;
        break;
      }
    }
    return isDefaultValuePresentInFilterValues;
  }
}
