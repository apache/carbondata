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

package org.apache.carbondata.core.scan.filter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.executer.AndFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.DimColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.ExcludeColGroupFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.ExcludeFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.FalseFilterExecutor;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.IncludeColGroupFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.IncludeFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.MeasureColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.OrFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RangeValueFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureExcludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelRangeTypeExecuterFacory;
import org.apache.carbondata.core.scan.filter.executer.TrueFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import org.apache.commons.lang.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;

public final class FilterUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(FilterUtil.class.getName());

  private FilterUtil() {

  }

  /**
   * Pattern used : Visitor Pattern
   * Method will create filter executer tree based on the filter resolved tree,
   * in this algorithm based on the resolver instance the executers will be visited
   * and the resolved surrogates will be converted to keys
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return FilterExecuter instance
   */
  private static FilterExecuter createFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap) {
    FilterExecuterType filterExecuterType = filterExpressionResolverTree.getFilterExecuterType();
    if (null != filterExecuterType) {
      switch (filterExecuterType) {
        case INCLUDE:
          if (null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              && null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues() && filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues().isOptimized()) {
            return getExcludeFilterExecuter(
                filterExpressionResolverTree.getDimColResolvedFilterInfo(),
                filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
          }
          return getIncludeFilterExecuter(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
        case EXCLUDE:
          return getExcludeFilterExecuter(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
        case OR:
          return new OrFilterExecuterImpl(
              createFilterExecuterTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap),
              createFilterExecuterTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap));
        case AND:
          return new AndFilterExecuterImpl(
              createFilterExecuterTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap),
              createFilterExecuterTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap));
        case ROWLEVEL_LESSTHAN:
        case ROWLEVEL_LESSTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN:
          return RowLevelRangeTypeExecuterFacory
              .getRowLevelRangeTypeExecuter(filterExecuterType, filterExpressionResolverTree,
                  segmentProperties);
        case RANGE:
          return new RangeValueFilterExecuterImpl(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getFilterExpression(),
              ((ConditionalFilterResolverImpl) filterExpressionResolverTree)
                  .getFilterRangeValues(segmentProperties), segmentProperties);
        case TRUE:
          return new TrueFilterExecutor();
        case FALSE:
          return new FalseFilterExecutor();
        case ROWLEVEL:
        default:
          return new RowLevelFilterExecuterImpl(
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getDimColEvaluatorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getMsrColEvalutorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
              segmentProperties, complexDimensionInfoMap);

      }
    }
    return new RowLevelFilterExecuterImpl(
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getDimColEvaluatorInfoList(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getMsrColEvalutorInfoList(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
        segmentProperties, complexDimensionInfoMap);

  }

  /**
   * It gives filter executer based on columnar or column group
   *
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return
   */
  private static FilterExecuter getIncludeFilterExecuter(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      MeasureColumnResolvedFilterInfo msrColResolvedFilterInfo,
      SegmentProperties segmentProperties) {
    if (null != msrColResolvedFilterInfo && msrColResolvedFilterInfo.getMeasure().isColumnar()) {
      CarbonMeasure measuresFromCurrentBlock = segmentProperties
          .getMeasureFromCurrentBlock(msrColResolvedFilterInfo.getMeasure().getColumnId());
      if (null != measuresFromCurrentBlock) {
        // update dimension and column index according to the dimension position in current block
        MeasureColumnResolvedFilterInfo msrColResolvedFilterInfoCopyObject =
            msrColResolvedFilterInfo.getCopyObject();
        msrColResolvedFilterInfoCopyObject.setMeasure(measuresFromCurrentBlock);
        msrColResolvedFilterInfoCopyObject.setColumnIndex(measuresFromCurrentBlock.getOrdinal());
        msrColResolvedFilterInfoCopyObject.setType(measuresFromCurrentBlock.getDataType());
        return new IncludeFilterExecuterImpl(null, msrColResolvedFilterInfoCopyObject,
            segmentProperties, true);
      } else {
        return new RestructureIncludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, true);
      }
    }
    if (null != dimColResolvedFilterInfo) {
      CarbonDimension dimension = dimColResolvedFilterInfo.getDimension();
      if (dimension.hasEncoding(Encoding.IMPLICIT)) {
        return new ImplicitIncludeFilterExecutorImpl(dimColResolvedFilterInfo);
      } else if (dimension.isColumnar()) {
        CarbonDimension dimensionFromCurrentBlock =
            segmentProperties.getDimensionFromCurrentBlock(dimColResolvedFilterInfo.getDimension());
        if (null != dimensionFromCurrentBlock) {
          // update dimension and column index according to the dimension position in current block
          DimColumnResolvedFilterInfo dimColResolvedFilterInfoCopyObject =
              dimColResolvedFilterInfo.getCopyObject();
          dimColResolvedFilterInfoCopyObject.setDimension(dimensionFromCurrentBlock);
          dimColResolvedFilterInfoCopyObject.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
          return new IncludeFilterExecuterImpl(dimColResolvedFilterInfoCopyObject, null,
              segmentProperties, false);
        } else {
          return new RestructureIncludeFilterExecutorImpl(dimColResolvedFilterInfo,
              msrColResolvedFilterInfo, false);
        }
      } else {
        return new IncludeColGroupFilterExecuterImpl(dimColResolvedFilterInfo, segmentProperties);
      }
    } else {
      return new IncludeColGroupFilterExecuterImpl(null, segmentProperties);
    }
  }

  /**
   * It gives filter executer based on columnar or column group
   *
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return
   */
  private static FilterExecuter getExcludeFilterExecuter(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      MeasureColumnResolvedFilterInfo msrColResolvedFilterInfo,
      SegmentProperties segmentProperties) {

    if (null != msrColResolvedFilterInfo && msrColResolvedFilterInfo.getMeasure().isColumnar()) {
      CarbonMeasure measuresFromCurrentBlock = segmentProperties
          .getMeasureFromCurrentBlock(msrColResolvedFilterInfo.getMeasure().getColumnId());
      if (null != measuresFromCurrentBlock) {
        // update dimension and column index according to the dimension position in current block
        MeasureColumnResolvedFilterInfo msrColResolvedFilterInfoCopyObject =
            msrColResolvedFilterInfo.getCopyObject();
        msrColResolvedFilterInfoCopyObject.setMeasure(measuresFromCurrentBlock);
        msrColResolvedFilterInfoCopyObject.setColumnIndex(measuresFromCurrentBlock.getOrdinal());
        msrColResolvedFilterInfoCopyObject.setType(measuresFromCurrentBlock.getDataType());
        return new ExcludeFilterExecuterImpl(null, msrColResolvedFilterInfoCopyObject,
            segmentProperties, true);
      } else {
        return new RestructureExcludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, true);
      }
    }
    if ((null != dimColResolvedFilterInfo) && (dimColResolvedFilterInfo.getDimension()
        .isColumnar())) {
      CarbonDimension dimensionFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(dimColResolvedFilterInfo.getDimension());
      if (null != dimensionFromCurrentBlock) {
        // update dimension and column index according to the dimension position in current block
        DimColumnResolvedFilterInfo dimColResolvedFilterInfoCopyObject =
            dimColResolvedFilterInfo.getCopyObject();
        dimColResolvedFilterInfoCopyObject.setDimension(dimensionFromCurrentBlock);
        dimColResolvedFilterInfoCopyObject.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
        return new ExcludeFilterExecuterImpl(dimColResolvedFilterInfoCopyObject, null,
            segmentProperties, false);
      } else {
        return new RestructureExcludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, false);
      }
    } else {
      return new ExcludeColGroupFilterExecuterImpl(dimColResolvedFilterInfo, segmentProperties);
    }
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively.
   *
   * @return
   */
  public static boolean checkIfExpressionContainsColumn(Expression expression) {
    if (expression instanceof ColumnExpression) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfExpressionContainsColumn(child)) {
        return true;
      }
    }

    return false;
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively.
   *
   * @return
   */
  public static boolean checkIfLeftExpressionRequireEvaluation(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.UNKNOWN
        || !(expression instanceof ColumnExpression)) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfLeftExpressionRequireEvaluation(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method will check if a given literal expression is not a timestamp datatype
   * recursively.
   *
   * @return
   */
  public static boolean checkIfDataTypeNotTimeStamp(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.LITERAL
        && expression instanceof LiteralExpression) {
      DataType dataType = ((LiteralExpression) expression).getLiteralExpDataType();
      if (!(dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE)) {
        return true;
      }
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfDataTypeNotTimeStamp(child)) {
        return true;
      }
    }
    return false;
  }
  /**
   * This method will check if a given expression contains a column expression
   * recursively.
   *
   * @return
   */
  public static boolean checkIfRightExpressionRequireEvaluation(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.UNKNOWN
        || !(expression instanceof LiteralExpression) && !(expression instanceof ListExpression)) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfRightExpressionRequireEvaluation(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * method will get the masked keys based on the keys generated from surrogates.
   *
   * @param ranges
   * @param key
   * @return byte[]
   */
  private static byte[] getMaskedKey(int[] ranges, byte[] key) {
    byte[] maskkey = new byte[ranges.length];

    for (int i = 0; i < maskkey.length; i++) {
      maskkey[i] = key[ranges[i]];
    }
    return maskkey;
  }

  /**
   * This method will return the ranges for the masked Bytes based on the key
   * Generator.
   *
   * @param queryDimensionsOrdinal
   * @param generator
   * @return
   */
  private static int[] getRangesForMaskedByte(int queryDimensionsOrdinal, KeyGenerator generator) {
    Set<Integer> integers = new TreeSet<Integer>();
    int[] range = generator.getKeyByteOffsets(queryDimensionsOrdinal);
    for (int j = range[0]; j <= range[1]; j++) {
      integers.add(j);
    }

    int[] byteIndexs = new int[integers.size()];
    int j = 0;
    for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
      Integer integer = iterator.next();
      byteIndexs[j++] = integer;
    }
    return byteIndexs;
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in ColumnFilterInfo
   *
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return ColumnFilterInfo
   */
  public static ColumnFilterInfo getNoDictionaryValKeyMemberForFilter(
      List<String> evaluateResultListFinal, boolean isIncludeFilter, DataType dataType)
      throws FilterUnsupportedException {
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    String result = null;
    try {
      int length = evaluateResultListFinal.size();
      String timeFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      for (int i = 0; i < length; i++) {
        result = evaluateResultListFinal.get(i);
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(result)) {
          if (dataType == DataTypes.STRING) {
            filterValuesList.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
          } else {
            filterValuesList.add(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
          }
          continue;
        }
        filterValuesList.add(DataTypeUtil
            .getBytesBasedOnDataTypeForNoDictionaryColumn(result, dataType, timeFormat));
      }
    } catch (Throwable ex) {
      throw new FilterUnsupportedException("Unsupported Filter condition: " + result, ex);
    }

    java.util.Comparator<byte[]> filterNoDictValueComaparator = new java.util.Comparator<byte[]>() {

      @Override public int compare(byte[] filterMember1, byte[] filterMember2) {
        // TODO Auto-generated method stub
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterMember1, filterMember2);
      }

    };
    Collections.sort(filterValuesList, filterNoDictValueComaparator);
    ColumnFilterInfo columnFilterInfo = null;
    if (filterValuesList.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setFilterListForNoDictionaryCols(filterValuesList);

    }
    return columnFilterInfo;
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in ColumnFilterInfo
   *
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return ColumnFilterInfo
   */
  public static ColumnFilterInfo getMeasureValKeyMemberForFilter(
      List<String> evaluateResultListFinal, boolean isIncludeFilter, DataType dataType,
      CarbonMeasure carbonMeasure) throws FilterUnsupportedException {
    List<Object> filterValuesList = new ArrayList<>(20);
    String result = null;
    try {
      int length = evaluateResultListFinal.size();
      for (int i = 0; i < length; i++) {
        result = evaluateResultListFinal.get(i);
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(result)) {
          filterValuesList.add(null);
          continue;
        }

        filterValuesList
            .add(DataTypeUtil.getMeasureValueBasedOnDataType(result, dataType, carbonMeasure));

      }
    } catch (Throwable ex) {
      throw new FilterUnsupportedException("Unsupported Filter condition: " + result, ex);
    }

    SerializableComparator filterMeasureComaparator =
        Comparator.getComparatorByDataTypeForMeasure(dataType);
    Collections.sort(filterValuesList, filterMeasureComaparator);
    ColumnFilterInfo columnFilterInfo = null;
    if (filterValuesList.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setMeasuresFilterValuesList(filterValuesList);

    }
    return columnFilterInfo;
  }

  /**
   * Method will prepare the  dimfilterinfo instance by resolving the filter
   * expression value to its respective surrogates.
   *
   * @param tableIdentifier
   * @param columnExpression
   * @param evaluateResultList
   * @param isIncludeFilter
   * @return
   * @throws QueryExecutionException
   */
  public static ColumnFilterInfo getFilterValues(AbsoluteTableIdentifier tableIdentifier,
      ColumnExpression columnExpression, List<String> evaluateResultList, boolean isIncludeFilter,
      TableProvider tableProvider)
      throws QueryExecutionException, FilterUnsupportedException, IOException {
    Dictionary forwardDictionary = null;
    ColumnFilterInfo filterInfo = null;
    List<Integer> surrogates =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    try {
      // Reading the dictionary value from cache.
      forwardDictionary =
          getForwardDictionaryCache(tableIdentifier, columnExpression.getDimension(),
              tableProvider);
      sortFilterModelMembers(columnExpression, evaluateResultList);
      getDictionaryValue(evaluateResultList, forwardDictionary, surrogates);
      filterInfo =
          getFilterValues(forwardDictionary, isIncludeFilter, surrogates);
      if (filterInfo.isOptimized()) {
        return getDimColumnFilterInfoAfterApplyingCBO(forwardDictionary, filterInfo);
      }
    } finally {
      CarbonUtil.clearDictionaryCache(forwardDictionary);
    }
    return filterInfo;
  }

  /**
   * Method will prepare the  dimfilterinfo instance by resolving the filter
   * expression value to its respective surrogates.
   *
   * @param forwardDictionary
   * @param isIncludeFilter
   * @param filterInfo
   * @param surrogates
   * @return
   */
  private static ColumnFilterInfo getFilterValues(Dictionary forwardDictionary,
      boolean isIncludeFilter, List<Integer> surrogates) {
    // Default value has to be added
    if (surrogates.isEmpty()) {
      surrogates.add(0);
    }
    boolean isExcludeFilterNeedsToApply = false;
    if (isIncludeFilter) {
      isExcludeFilterNeedsToApply =
          isExcludeFilterNeedsToApply(forwardDictionary, surrogates.size());
    }
    Collections.sort(surrogates);
    ColumnFilterInfo columnFilterInfo = null;
    if (surrogates.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      if (isExcludeFilterNeedsToApply) {
        columnFilterInfo.setOptimized(true);
      }
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      if (!isIncludeFilter) {
        columnFilterInfo.setExcludeFilterList(surrogates);
      } else {
        columnFilterInfo.setFilterList(surrogates);
      }
    }
    return columnFilterInfo;
  }

  private static boolean isExcludeFilterNeedsToApply(Dictionary forwardDictionary,
      int size) {
    if ((size * 100) / forwardDictionary.getDictionaryChunks().getSize() >= 60) {
      LOGGER.info("Applying CBO to convert include filter to exclude filter.");
      return true;
    }
    return false;
  }

  private static ColumnFilterInfo getDimColumnFilterInfoAfterApplyingCBO(
      Dictionary forwardDictionary, ColumnFilterInfo filterInfo) throws FilterUnsupportedException {
    List<Integer> excludeMemberSurrogates =
        prepareExcludeFilterMembers(forwardDictionary, filterInfo.getFilterList());
    filterInfo.setExcludeFilterList(excludeMemberSurrogates);
    return filterInfo;
  }

  private static void prepareIncludeFilterMembers(Expression expression,
      final ColumnExpression columnExpression, boolean isIncludeFilter,
      Dictionary forwardDictionary, List<Integer> surrogates)
      throws FilterUnsupportedException {
    DictionaryChunksWrapper dictionaryWrapper;
    dictionaryWrapper = forwardDictionary.getDictionaryChunks();
    int surrogateCount = 0;
    while (dictionaryWrapper.hasNext()) {
      byte[] columnVal = dictionaryWrapper.next();
      ++surrogateCount;
      try {
        RowIntf row = new RowImpl();
        String stringValue =
            new String(columnVal, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        if (stringValue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
          stringValue = null;
        }
        row.setValues(new Object[] { DataTypeUtil.getDataBasedOnDataType(stringValue,
            columnExpression.getCarbonColumn().getDataType()) });
        Boolean rslt = expression.evaluate(row).getBoolean();
        if (null != rslt) {
          if (rslt) {
            if (null == stringValue) {
              // this is for query like select name from table unknowexpr(name,1)
              // != 'value' -> for null dictionary value
              surrogates.add(CarbonCommonConstants.DICT_VALUE_NULL);
            } else if (isIncludeFilter) {
              // this is for query like select ** from * where unknwonexpr(*) == 'value'
              surrogates.add(surrogateCount);
            }
          } else if (null != stringValue && !isIncludeFilter) {
            // this is for isNot null or not in query( e.x select ** from t where name is not null
            surrogates.add(surrogateCount);
          }
        }
      } catch (FilterIllegalMemberException e) {
        LOGGER.debug(e.getMessage());
      }
    }
  }

  private static List<Integer> prepareExcludeFilterMembers(
      Dictionary forwardDictionary,List<Integer> includeSurrogates)
      throws FilterUnsupportedException {
    DictionaryChunksWrapper dictionaryWrapper;
    RoaringBitmap bitMapOfSurrogates = RoaringBitmap.bitmapOf(
        ArrayUtils.toPrimitive(includeSurrogates.toArray(new Integer[includeSurrogates.size()])));
    dictionaryWrapper = forwardDictionary.getDictionaryChunks();
    List<Integer> excludeFilterList = new ArrayList<Integer>(includeSurrogates.size());
    int surrogateCount = 0;
    while (dictionaryWrapper.hasNext()) {
      dictionaryWrapper.next();
      ++surrogateCount;
      if (!bitMapOfSurrogates.contains(surrogateCount)) {
        excludeFilterList.add(surrogateCount);
      }
    }
    return excludeFilterList;
  }



  /**
   * This API will get the Dictionary value for the respective filter member
   * string.
   *
   * @param evaluateResultList filter value
   * @param surrogates
   */
  private static void getDictionaryValue(List<String> evaluateResultList,
      Dictionary forwardDictionary, List<Integer> surrogates) {
    ((ForwardDictionary) forwardDictionary)
        .getSurrogateKeyByIncrementalSearch(evaluateResultList, surrogates);
  }

  /**
   * This method will get all the members of column from the forward dictionary
   * cache, this method will be basically used in row level filter resolver.
   *
   * @param tableIdentifier
   * @param expression
   * @param columnExpression
   * @param isIncludeFilter
   * @return ColumnFilterInfo
   * @throws FilterUnsupportedException
   * @throws IOException
   */
  public static ColumnFilterInfo getFilterListForAllValues(AbsoluteTableIdentifier tableIdentifier,
      Expression expression, final ColumnExpression columnExpression, boolean isIncludeFilter,
      TableProvider tableProvider, boolean isExprEvalReqd)
      throws FilterUnsupportedException, IOException {
    Dictionary forwardDictionary = null;
    List<Integer> surrogates = new ArrayList<Integer>(20);
    try {
      forwardDictionary =
          getForwardDictionaryCache(tableIdentifier, columnExpression.getDimension(),
              tableProvider);
      if (isExprEvalReqd && !isIncludeFilter) {
        surrogates.add(CarbonCommonConstants.DICT_VALUE_NULL);
      }
      prepareIncludeFilterMembers(expression, columnExpression, isIncludeFilter, forwardDictionary,
          surrogates);
      ColumnFilterInfo filterInfo =
          getFilterValues(forwardDictionary, isIncludeFilter, surrogates);
      if (filterInfo.isOptimized()) {
        return getDimColumnFilterInfoAfterApplyingCBO(forwardDictionary,
            filterInfo);
      }
      return filterInfo;
    } finally {
      CarbonUtil.clearDictionaryCache(forwardDictionary);
    }
  }

  private static void sortFilterModelMembers(final ColumnExpression columnExpression,
      List<String> evaluateResultListFinal) {
    java.util.Comparator<String> filterActualValueComaparator = new java.util.Comparator<String>() {

      @Override public int compare(String filterMember1, String filterMember2) {
        return compareFilterMembersBasedOnActualDataType(filterMember1, filterMember2,
            columnExpression.getDataType());
      }

    };
    Collections.sort(evaluateResultListFinal, filterActualValueComaparator);
  }

  /**
   * Method will prepare the  dimfilterinfo instance by resolving the filter
   * expression value to its respective surrogates in the scenario of restructure.
   *
   * @param expression
   * @param defaultValues
   * @param defaultSurrogate
   * @return
   * @throws FilterUnsupportedException
   */
  public static ColumnFilterInfo getFilterListForRS(Expression expression, String defaultValues,
      int defaultSurrogate) throws FilterUnsupportedException {
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    ColumnFilterInfo columnFilterInfo = null;
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    try {
      List<ExpressionResult> evaluateResultList = expression.evaluate(null).getList();
      for (ExpressionResult result : evaluateResultList) {
        if (result.getString() == null) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
          continue;
        }
        evaluateResultListFinal.add(result.getString());
      }

      for (int i = 0; i < evaluateResultListFinal.size(); i++) {
        if (evaluateResultListFinal.get(i).equals(defaultValues)) {
          filterValuesList.add(defaultSurrogate);
          break;
        }
      }
      if (filterValuesList.size() > 0) {
        columnFilterInfo = new ColumnFilterInfo();
        columnFilterInfo.setFilterList(filterValuesList);
      }
    } catch (FilterIllegalMemberException e) {
      LOGGER.audit(e.getMessage());
    }
    return columnFilterInfo;
  }

  /**
   * This method will get the member based on filter expression evaluation from the
   * forward dictionary cache, this method will be basically used in restructure.
   *
   * @param expression
   * @param columnExpression
   * @param defaultValues
   * @param defaultSurrogate
   * @param isIncludeFilter
   * @return
   * @throws FilterUnsupportedException
   */
  public static ColumnFilterInfo getFilterListForAllMembersRS(Expression expression,
      ColumnExpression columnExpression, String defaultValues, int defaultSurrogate,
      boolean isIncludeFilter) throws FilterUnsupportedException {
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    ColumnFilterInfo columnFilterInfo = null;

    try {
      RowIntf row = new RowImpl();
      if (defaultValues.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        defaultValues = null;
      }
      row.setValues(new Object[] { DataTypeUtil.getDataBasedOnDataType(defaultValues,
          columnExpression.getCarbonColumn().getDataType()) });
      Boolean rslt = expression.evaluate(row).getBoolean();
      if (null != rslt && rslt == isIncludeFilter) {
        if (null == defaultValues) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        } else {
          evaluateResultListFinal.add(defaultValues);
        }
      }
    } catch (FilterIllegalMemberException e) {
      LOGGER.audit(e.getMessage());
    }

    if (null == defaultValues) {
      defaultValues = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
    }
    columnFilterInfo = new ColumnFilterInfo();
    for (int i = 0; i < evaluateResultListFinal.size(); i++) {
      if (evaluateResultListFinal.get(i).equals(defaultValues)) {
        filterValuesList.add(defaultSurrogate);
        break;
      }
    }
    columnFilterInfo.setFilterList(filterValuesList);
    return columnFilterInfo;
  }

  /**
   * Below method will be used to covert the filter surrogate keys
   * to mdkey
   *
   * @param columnFilterInfo
   * @param carbonDimension
   * @param segmentProperties
   * @return
   */
  public static byte[][] getKeyArray(ColumnFilterInfo columnFilterInfo,
      CarbonDimension carbonDimension, SegmentProperties segmentProperties,  boolean isExclude) {
    if (!carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      return columnFilterInfo.getNoDictionaryFilterValuesList()
          .toArray((new byte[columnFilterInfo.getNoDictionaryFilterValuesList().size()][]));
    }
    KeyGenerator blockLevelKeyGenerator = segmentProperties.getDimensionKeyGenerator();
    int[] dimColumnsCardinality = segmentProperties.getDimColumnsCardinality();
    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    Arrays.fill(keys, 0);
    int keyOrdinalOfDimensionFromCurrentBlock = carbonDimension.getKeyOrdinal();
    if (null != columnFilterInfo) {
      int[] rangesForMaskedByte =
          getRangesForMaskedByte(keyOrdinalOfDimensionFromCurrentBlock, blockLevelKeyGenerator);
      List<Integer> listOfsurrogates = null;
      if (!isExclude && columnFilterInfo.isIncludeFilter()) {
        listOfsurrogates = columnFilterInfo.getFilterList();
      } else if (isExclude || !columnFilterInfo.isIncludeFilter()) {
        listOfsurrogates = columnFilterInfo.getExcludeFilterList();
      }
      if (null != listOfsurrogates) {
        for (Integer surrogate : listOfsurrogates) {
          try {
            if (surrogate <= dimColumnsCardinality[keyOrdinalOfDimensionFromCurrentBlock]) {
              keys[keyOrdinalOfDimensionFromCurrentBlock] = surrogate;
              filterValuesList
                  .add(getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys)));
            } else {
              break;
            }
          } catch (KeyGenException e) {
            LOGGER.error(e.getMessage());
          }
        }
      }
    }
    return filterValuesList.toArray(new byte[filterValuesList.size()][]);

  }

  /**
   * The method is used to get the single dictionary key's mask key
   *
   * @param surrogate
   * @param carbonDimension
   * @param blockLevelKeyGenerator
   * @return
   */
  public static byte[] getMaskKey(int surrogate, CarbonDimension carbonDimension,
      KeyGenerator blockLevelKeyGenerator) {

    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    byte[] maskedKey = null;
    Arrays.fill(keys, 0);
    int[] rangesForMaskedByte =
        getRangesForMaskedByte((carbonDimension.getKeyOrdinal()), blockLevelKeyGenerator);
    try {
      keys[carbonDimension.getKeyOrdinal()] = surrogate;
      maskedKey = getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys));
    } catch (KeyGenException e) {
      LOGGER.error(e.getMessage());
    }
    return maskedKey;
  }

  /**
   * Method will return the start key based on KeyGenerator for the respective
   * filter resolved instance.
   *
   * @param dimensionFilter
   * @param startKey
   * @param startKeyList
   * @return long[] start key
   */
  public static void getStartKey(Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      SegmentProperties segmentProperties, long[] startKey, List<long[]> startKeyList) {
    for (int i = 0; i < startKey.length; i++) {
      // The min surrogate key is 1, set it as the init value for starkey of each column level
      startKey[i] = 1;
    }
    getStartKeyWithFilter(dimensionFilter, segmentProperties, startKey, startKeyList);
  }

  /**
   * Algorithm for getting the start key for a filter
   * step 1: Iterate through each dimension and verify whether its not an exclude filter.
   * step 2: Intialize start key with the first filter member value present in each filter model
   * for the respective dimensions.
   * step 3: since its a no dictionary start key there will only actual value so compare
   * the first filter model value with respect to the dimension data type.
   * step 4: The least value will be considered as the start key of dimension by comparing all
   * its filter model.
   * step 5: create a byte array of start key which comprises of least filter member value of
   * all dimension and the indexes which will help to read the respective filter value.
   *
   * @param dimColResolvedFilterInfo
   * @param setOfStartKeyByteArray
   * @return
   */
  public static void getStartKeyForNoDictionaryDimension(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray) {
    Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    // step 1
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      if (!entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        List<ColumnFilterInfo> listOfDimColFilterInfo = entry.getValue();
        if (null == listOfDimColFilterInfo) {
          continue;
        }
        boolean isExcludePresent = false;
        for (ColumnFilterInfo info : listOfDimColFilterInfo) {
          if (!info.isIncludeFilter()) {
            isExcludePresent = true;
          }
        }
        if (isExcludePresent) {
          continue;
        }
        // in case of restructure scenarios it can happen that the filter dimension is not
        // present in the current block. In those cases no need to determine the key
        CarbonDimension dimensionFromCurrentBlock = CarbonUtil
            .getDimensionFromCurrentBlock(segmentProperties.getDimensions(), entry.getKey());
        if (null == dimensionFromCurrentBlock) {
          continue;
        }
        // step 2
        byte[] noDictionaryStartKey =
            listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList().get(0);
        if (setOfStartKeyByteArray.isEmpty()) {
          setOfStartKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryStartKey);
        } else if (null == setOfStartKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal())) {
          setOfStartKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryStartKey);

        } else if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(setOfStartKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal()),
                noDictionaryStartKey) > 0) {
          setOfStartKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryStartKey);
        }
      }
    }
  }

  /**
   * Algorithm for getting the end key for a filter
   * step 1: Iterate through each dimension and verify whether its not an exclude filter.
   * step 2: Initialize end key with the last filter member value present in each filter model
   * for the respective dimensions.(Already filter models are sorted)
   * step 3: since its a no dictionary end key there will only actual value so compare
   * the last filter model value with respect to the dimension data type.
   * step 4: The highest value will be considered as the end key of dimension by comparing all
   * its filter model.
   * step 5: create a byte array of end key which comprises of highest filter member value of
   * all dimension and the indexes which will help to read the respective filter value.
   *
   * @param dimColResolvedFilterInfo
   * @param setOfEndKeyByteArray
   * @return end key array
   */
  public static void getEndKeyForNoDictionaryDimension(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray) {

    Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    // step 1
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      if (!entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        List<ColumnFilterInfo> listOfDimColFilterInfo = entry.getValue();
        if (null == listOfDimColFilterInfo) {
          continue;
        }
        boolean isExcludePresent = false;
        for (ColumnFilterInfo info : listOfDimColFilterInfo) {
          if (!info.isIncludeFilter()) {
            isExcludePresent = true;
          }
        }
        if (isExcludePresent) {
          continue;
        }
        // in case of restructure scenarios it can happen that the filter dimension is not
        // present in the current block. In those cases no need to determine the key
        CarbonDimension dimensionFromCurrentBlock = CarbonUtil
            .getDimensionFromCurrentBlock(segmentProperties.getDimensions(), entry.getKey());
        if (null == dimensionFromCurrentBlock) {
          continue;
        }
        // step 2
        byte[] noDictionaryEndKey = listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList()
            .get(listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList().size() - 1);
        if (setOfEndKeyByteArray.isEmpty()) {
          setOfEndKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryEndKey);
        } else if (null == setOfEndKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal())) {
          setOfEndKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryEndKey);

        } else if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(setOfEndKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal()),
                noDictionaryEndKey) < 0) {
          setOfEndKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryEndKey);
        }

      }
    }
  }

  /**
   * Method will pack all the byte[] to a single byte[] value by appending the
   * indexes of the byte[] value which needs to be read. this method will be mailny used
   * in case of no dictionary dimension processing for filters.
   *
   * @param noDictionaryValKeyList
   * @return packed key with its indexes added in starting and its actual values.
   */
  private static byte[] getKeyWithIndexesAndValues(List<byte[]> noDictionaryValKeyList) {
    ByteBuffer[] buffArr = new ByteBuffer[noDictionaryValKeyList.size()];
    int index = 0;
    for (byte[] singleColVal : noDictionaryValKeyList) {
      buffArr[index] = ByteBuffer.allocate(singleColVal.length);
      buffArr[index].put(singleColVal);
      buffArr[index++].rewind();
    }
    // byteBufer.
    return CarbonUtil.packByteBufferIntoSingleByteArray(buffArr);

  }

  /**
   * This method will fill the start key array  with the surrogate key present
   * in filterinfo instance.
   *
   * @param dimensionFilter
   * @param startKey
   */
  private static void getStartKeyWithFilter(
      Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      SegmentProperties segmentProperties, long[] startKey, List<long[]> startKeyList) {
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<ColumnFilterInfo> values = entry.getValue();
      if (null == values || !entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      boolean isExcludePresent = false;
      for (ColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludePresent = true;
        }
      }
      if (isExcludePresent) {
        continue;
      }
      // search the query dimension in current block dimensions. If the dimension is not found
      // that means the key cannot be included in start key formation.
      // Applicable for restructure scenarios
      CarbonDimension dimensionFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(entry.getKey());
      if (null == dimensionFromCurrentBlock) {
        continue;
      }
      int keyOrdinalOfDimensionFromCurrentBlock = dimensionFromCurrentBlock.getKeyOrdinal();
      for (ColumnFilterInfo info : values) {
        if (keyOrdinalOfDimensionFromCurrentBlock < startKey.length) {
          if (startKey[keyOrdinalOfDimensionFromCurrentBlock] < info.getFilterList().get(0)) {
            startKey[keyOrdinalOfDimensionFromCurrentBlock] = info.getFilterList().get(0);
          }
        }
      }
      long[] newStartKey = new long[startKey.length];
      System.arraycopy(startKey, 0, newStartKey, 0, startKey.length);
      startKeyList.add(newStartKey);
    }
  }

  public static void getEndKey(Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      long[] endKey, SegmentProperties segmentProperties,
      List<long[]> endKeyList) {

    List<CarbonDimension> updatedDimListBasedOnKeyGenerator =
        getCarbonDimsMappedToKeyGenerator(segmentProperties.getDimensions());
    for (int i = 0; i < endKey.length; i++) {
      endKey[i] = getMaxValue(updatedDimListBasedOnKeyGenerator.get(i),
          segmentProperties.getDimColumnsCardinality());
    }
    getEndKeyWithFilter(dimensionFilter, segmentProperties, endKey, endKeyList);

  }

  private static List<CarbonDimension> getCarbonDimsMappedToKeyGenerator(
      List<CarbonDimension> carbonDimensions) {
    List<CarbonDimension> listOfCarbonDimPartOfKeyGen =
        new ArrayList<CarbonDimension>(carbonDimensions.size());
    for (CarbonDimension carbonDim : carbonDimensions) {
      if (CarbonUtil.hasEncoding(carbonDim.getEncoder(), Encoding.DICTIONARY) || CarbonUtil
          .hasEncoding(carbonDim.getEncoder(), Encoding.DIRECT_DICTIONARY)) {
        listOfCarbonDimPartOfKeyGen.add(carbonDim);
      }

    }
    return listOfCarbonDimPartOfKeyGen;
  }

  private static void getEndKeyWithFilter(
      Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      SegmentProperties segmentProperties, long[] endKey, List<long[]> endKeyList) {
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<ColumnFilterInfo> values = entry.getValue();
      if (null == values || !entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      boolean isExcludeFilterPresent = false;
      for (ColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludeFilterPresent = true;
        }
      }
      if (isExcludeFilterPresent) {
        continue;
      }
      // search the query dimension in current block dimensions. If the dimension is not found
      // that means the key cannot be included in start key formation.
      // Applicable for restructure scenarios
      CarbonDimension dimensionFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(entry.getKey());
      if (null == dimensionFromCurrentBlock) {
        continue;
      }
      int keyOrdinalOfDimensionFromCurrentBlock = dimensionFromCurrentBlock.getKeyOrdinal();
      int endFilterValue = 0;
      for (ColumnFilterInfo info : values) {
        if (keyOrdinalOfDimensionFromCurrentBlock < endKey.length) {
          endFilterValue = info.getFilterList().get(info.getFilterList().size() - 1);
          if (endFilterValue == 0) {
            endFilterValue =
                segmentProperties.getDimColumnsCardinality()[keyOrdinalOfDimensionFromCurrentBlock];
          }
          if (endKey[keyOrdinalOfDimensionFromCurrentBlock] > endFilterValue) {
            endKey[keyOrdinalOfDimensionFromCurrentBlock] = endFilterValue;
          }
        }
      }
      long[] newEndKey = new long[endKey.length];
      System.arraycopy(endKey, 0, newEndKey, 0, endKey.length);
      endKeyList.add(newEndKey);
    }

  }

  /**
   * This API will get the max value of surrogate key which will be used for
   * determining the end key of particular btree.
   *
   * @param dimCarinality
   */
  private static long getMaxValue(CarbonDimension carbonDimension, int[] dimCarinality) {
    // Get data from all the available slices of the table
    if (null != dimCarinality) {
      return dimCarinality[carbonDimension.getKeyOrdinal()];
    }
    return -1;
  }

  /**
   * @param tableIdentifier
   * @param carbonDimension
   * @return
   */
  public static Dictionary getForwardDictionaryCache(AbsoluteTableIdentifier tableIdentifier,
      CarbonDimension carbonDimension) throws IOException {
    return getForwardDictionaryCache(tableIdentifier, carbonDimension, null);
  }

  /**
   * @param carbonDimension
   * @param tableProvider
   * @return
   */
  public static Dictionary getForwardDictionaryCache(
      AbsoluteTableIdentifier dictionarySourceAbsoluteTableIdentifier,
      CarbonDimension carbonDimension, TableProvider tableProvider) throws IOException {
    String dictionaryPath = null;
    ColumnIdentifier columnIdentifier = carbonDimension.getColumnIdentifier();
    if (null != tableProvider) {
      CarbonTable carbonTable = tableProvider
          .getCarbonTable(dictionarySourceAbsoluteTableIdentifier.getCarbonTableIdentifier());
      dictionaryPath = carbonTable.getTableInfo().getFactTable().getTableProperties()
          .get(CarbonCommonConstants.DICTIONARY_PATH);
      if (null != carbonDimension.getColumnSchema().getParentColumnTableRelations() &&
          carbonDimension.getColumnSchema().getParentColumnTableRelations().size() == 1) {
        dictionarySourceAbsoluteTableIdentifier =
            QueryUtil.getTableIdentifierForColumn(carbonDimension,
                carbonTable.getAbsoluteTableIdentifier());
        columnIdentifier = new ColumnIdentifier(
            carbonDimension.getColumnSchema().getParentColumnTableRelations().get(0).getColumnId(),
            carbonDimension.getColumnProperties(), carbonDimension.getDataType());
      } else {
        dictionarySourceAbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
      }
    }
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(dictionarySourceAbsoluteTableIdentifier,
            columnIdentifier, carbonDimension.getDataType(), dictionaryPath);
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY);
    // get the forward dictionary object
    return forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
  }

  public static IndexKey createIndexKeyFromResolvedFilterVal(long[] startOrEndKey,
      KeyGenerator keyGenerator, byte[] startOrEndKeyForNoDictDimension) {
    IndexKey indexKey = null;
    try {
      indexKey =
          new IndexKey(keyGenerator.generateKey(startOrEndKey), startOrEndKeyForNoDictDimension);
    } catch (KeyGenException e) {
      LOGGER.error(e.getMessage());
    }
    return indexKey;
  }

  /**
   * API will create an filter executer tree based on the filter resolver
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return
   */
  public static FilterExecuter getFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap) {
    return createFilterExecuterTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap);
  }

  /**
   * API will prepare the Keys from the surrogates of particular filter resolver
   *
   * @param filterValues
   * @param segmentProperties
   * @param dimension
   * @param dimColumnExecuterInfo
   */
  public static void prepareKeysFromSurrogates(ColumnFilterInfo filterValues,
      SegmentProperties segmentProperties, CarbonDimension dimension,
      DimColumnExecuterFilterInfo dimColumnExecuterInfo, CarbonMeasure measures,
      MeasureColumnExecuterFilterInfo msrColumnExecuterInfo) {
    if (null != measures) {
      DataTypeConverterImpl converter = new DataTypeConverterImpl();
      Object[] keysBasedOnFilter = filterValues.getMeasuresFilterValuesList()
          .toArray((new Object[filterValues.getMeasuresFilterValuesList().size()]));
      for (int i = 0; i < keysBasedOnFilter.length; i++) {
        if (keysBasedOnFilter[i] != null) {
          keysBasedOnFilter[i] = DataTypeUtil
              .getDataBasedOnDataType(keysBasedOnFilter[i].toString(), measures.getDataType(),
                  converter);
        }
      }
      msrColumnExecuterInfo.setFilterKeys(keysBasedOnFilter);
    } else {
      if (filterValues == null) {
        dimColumnExecuterInfo.setFilterKeys(new byte[0][]);
      } else {
        byte[][] keysBasedOnFilter = getKeyArray(filterValues, dimension, segmentProperties, false);
        if (!filterValues.isIncludeFilter() || filterValues.isOptimized()) {
          dimColumnExecuterInfo
              .setExcludeFilterKeys(getKeyArray(filterValues, dimension, segmentProperties, true));
        }
        dimColumnExecuterInfo.setFilterKeys(keysBasedOnFilter);
      }
    }
  }



  /**
   * method will create a default end key in case of no end key is been derived using existing
   * filter or in case of non filter queries.
   *
   * @param segmentProperties
   * @return
   * @throws KeyGenException
   */
  public static IndexKey prepareDefaultEndIndexKey(SegmentProperties segmentProperties)
      throws KeyGenException {
    long[] dictionarySurrogateKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    int index = 0;
    int[] dimColumnsCardinality = segmentProperties.getDimColumnsCardinality();
    for (int i = 0; i < dictionarySurrogateKey.length; i++) {
      dictionarySurrogateKey[index++] = dimColumnsCardinality[i];
    }
    IndexKey endIndexKey;
    byte[] dictionaryendMdkey =
        segmentProperties.getSortColumnsGenerator().generateKey(dictionarySurrogateKey);
    byte[] noDictionaryEndKeyBuffer = getNoDictionaryDefaultEndKey(segmentProperties);
    endIndexKey = new IndexKey(dictionaryendMdkey, noDictionaryEndKeyBuffer);
    return endIndexKey;
  }

  public static byte[] getNoDictionaryDefaultEndKey(SegmentProperties segmentProperties) {

    int numberOfNoDictionaryDimension = segmentProperties.getNumberOfNoDictSortColumns();
    // in case of non filter query when no dictionary columns are present we
    // need to set the default end key, as for non filter query
    // we need to get the last
    // block of the btree so we are setting the max byte value in the end key
    ByteBuffer noDictionaryEndKeyBuffer = ByteBuffer.allocate(
        (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE)
            + numberOfNoDictionaryDimension);
    // end key structure will be
    //<Offset of first No Dictionary key in 2 Bytes><Offset of second No Dictionary key in 2 Bytes>
    //<Offset of n No Dictionary key in 2 Bytes><first no dictionary column value>
    // <second no dictionary column value> <N no dictionary column value>
    //example if we have 2 no dictionary column
    //<[0,4,0,5,127,127]>
    short startPoint =
        (short) (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryEndKeyBuffer.putShort((startPoint));
      startPoint++;
    }
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryEndKeyBuffer.put((byte) 0xFF);
    }
    return noDictionaryEndKeyBuffer.array();
  }

  /**
   * method will create a default end key in case of no end key is been
   * derived using existing filter or in case of non filter queries.
   *
   * @param segmentProperties
   * @return
   * @throws KeyGenException
   */
  public static IndexKey prepareDefaultStartIndexKey(SegmentProperties segmentProperties)
      throws KeyGenException {
    IndexKey startIndexKey;
    long[] dictionarySurrogateKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    byte[] dictionaryStartMdkey =
        segmentProperties.getSortColumnsGenerator().generateKey(dictionarySurrogateKey);
    byte[] noDictionaryStartKeyArray = getNoDictionaryDefaultStartKey(segmentProperties);

    startIndexKey = new IndexKey(dictionaryStartMdkey, noDictionaryStartKeyArray);
    return startIndexKey;
  }

  public static byte[] getNoDictionaryDefaultStartKey(SegmentProperties segmentProperties) {

    int numberOfNoDictionaryDimension = segmentProperties.getNumberOfNoDictSortColumns();
    // in case of non filter query when no dictionary columns are present we
    // need to set the default start key, as for non filter query we need to get the first
    // block of the btree so we are setting the least byte value in the start key
    ByteBuffer noDictionaryStartKeyBuffer = ByteBuffer.allocate(
        (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE)
            + numberOfNoDictionaryDimension);
    // end key structure will be
    //<Offset of first No Dictionary key in 2 Bytes><Offset of second No Dictionary key in 2 Bytes>
    //<Offset of n No Dictionary key in 2 Bytes><first no dictionary column value>
    // <second no dictionary column value> <N no dictionary column value>
    //example if we have 2 no dictionary column
    //<[0,4,0,5,0,0]>
    short startPoint =
        (short) (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryStartKeyBuffer.putShort((startPoint));
      startPoint++;
    }
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryStartKeyBuffer.put((byte) 0);
    }
    return noDictionaryStartKeyBuffer.array();
  }

  public static int compareFilterKeyBasedOnDataType(String dictionaryVal, String memberVal,
      DataType dataType) {
    try {
      if (dataType == DataTypes.BOOLEAN) {
        return Boolean.compare((Boolean.parseBoolean(dictionaryVal)),
                (Boolean.parseBoolean(memberVal)));
      } else if (dataType == DataTypes.SHORT) {
        return Short.compare((Short.parseShort(dictionaryVal)), (Short.parseShort(memberVal)));
      } else if (dataType == DataTypes.INT) {
        return Integer.compare((Integer.parseInt(dictionaryVal)), (Integer.parseInt(memberVal)));
      } else if (dataType == DataTypes.DOUBLE) {
        return Double.compare((Double.parseDouble(dictionaryVal)), (Double.parseDouble(memberVal)));
      } else if (dataType == DataTypes.LONG) {
        return Long.compare((Long.parseLong(dictionaryVal)), (Long.parseLong(memberVal)));
      } else if (dataType == DataTypes.BOOLEAN) {
        return Boolean.compare(
            (Boolean.parseBoolean(dictionaryVal)), (Boolean.parseBoolean(memberVal)));
      } else if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
        String format = CarbonUtil.getFormatFromProperty(dataType);
        SimpleDateFormat parser = new SimpleDateFormat(format);
        Date dateToStr;
        Date dictionaryDate;
        dateToStr = parser.parse(memberVal);
        dictionaryDate = parser.parse(dictionaryVal);
        return dictionaryDate.compareTo(dateToStr);
      } else if (DataTypes.isDecimal(dataType)) {
        java.math.BigDecimal javaDecValForDictVal = new java.math.BigDecimal(dictionaryVal);
        java.math.BigDecimal javaDecValForMemberVal = new java.math.BigDecimal(memberVal);
        return javaDecValForDictVal.compareTo(javaDecValForMemberVal);
      } else {
        return -1;
      }
    } catch (ParseException | NumberFormatException e) {
      return -1;
    }
  }

  /**
   * method will set the start and end key for as per the filter resolver tree
   * utilized visitor pattern inorder to populate the start and end key population.
   *
   * @param segmentProperties
   * @param filterResolver
   * @param listOfStartEndKeys
   */
  public static void traverseResolverTreeAndGetStartAndEndKey(SegmentProperties segmentProperties,
      FilterResolverIntf filterResolver, List<IndexKey> listOfStartEndKeys) {
    IndexKey searchStartKey = null;
    IndexKey searchEndKey = null;
    long[] startKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    long[] endKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    List<byte[]> listOfStartKeyByteArray =
        new ArrayList<byte[]>(segmentProperties.getNumberOfNoDictionaryDimension());
    List<byte[]> listOfEndKeyByteArray =
        new ArrayList<byte[]>(segmentProperties.getNumberOfNoDictionaryDimension());
    SortedMap<Integer, byte[]> setOfStartKeyByteArray = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> setOfEndKeyByteArray = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> defaultStartValues = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> defaultEndValues = new TreeMap<Integer, byte[]>();
    List<long[]> startKeyList = new ArrayList<long[]>();
    List<long[]> endKeyList = new ArrayList<long[]>();
    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolver, segmentProperties, startKey,
        setOfStartKeyByteArray, endKey, setOfEndKeyByteArray,
        startKeyList, endKeyList);
    if (endKeyList.size() > 0) {
      //get the new end key from list
      for (int i = 0; i < endKey.length; i++) {
        long[] endkeyColumnLevel = new long[endKeyList.size()];
        int j = 0;
        for (long[] oneEndKey : endKeyList) {
          //get each column level end key
          endkeyColumnLevel[j++] = oneEndKey[i];
        }
        Arrays.sort(endkeyColumnLevel);
        // get the max one as end of this column level
        endKey[i] = endkeyColumnLevel[endkeyColumnLevel.length - 1];
      }
    }

    if (startKeyList.size() > 0) {
      //get the new start key from list
      for (int i = 0; i < startKey.length; i++) {
        long[] startkeyColumnLevel = new long[startKeyList.size()];
        int j = 0;
        for (long[] oneStartKey : startKeyList) {
          //get each column level start key
          startkeyColumnLevel[j++] = oneStartKey[i];
        }
        Arrays.sort(startkeyColumnLevel);
        // get the min - 1 as start of this column level, for example if a block contains 5,6
        // the filter is 6, but that block's start key is 5, if not -1, this block will missing.
        startKey[i] = startkeyColumnLevel[0] - 1;
      }
    }

    fillDefaultStartValue(defaultStartValues, segmentProperties);
    fillDefaultEndValue(defaultEndValues, segmentProperties);
    fillNullValuesStartIndexWithDefaultKeys(setOfStartKeyByteArray, segmentProperties);
    fillNullValuesEndIndexWithDefaultKeys(setOfEndKeyByteArray, segmentProperties);
    pruneStartAndEndKeys(setOfStartKeyByteArray, listOfStartKeyByteArray);
    pruneStartAndEndKeys(setOfEndKeyByteArray, listOfEndKeyByteArray);

    if (segmentProperties.getNumberOfNoDictSortColumns() == 0) {
      listOfStartKeyByteArray = new ArrayList<byte[]>();
      listOfEndKeyByteArray = new ArrayList<byte[]>();
    } else {
      while (segmentProperties.getNumberOfNoDictSortColumns() < listOfStartKeyByteArray.size()) {
        listOfStartKeyByteArray.remove(listOfStartKeyByteArray.size() - 1);
        listOfEndKeyByteArray.remove(listOfEndKeyByteArray.size() - 1);
      }
    }

    searchStartKey = FilterUtil
        .createIndexKeyFromResolvedFilterVal(startKey, segmentProperties.getSortColumnsGenerator(),
            FilterUtil.getKeyWithIndexesAndValues(listOfStartKeyByteArray));

    searchEndKey = FilterUtil
        .createIndexKeyFromResolvedFilterVal(endKey, segmentProperties.getSortColumnsGenerator(),
            FilterUtil.getKeyWithIndexesAndValues(listOfEndKeyByteArray));
    listOfStartEndKeys.add(searchStartKey);
    listOfStartEndKeys.add(searchEndKey);

  }

  private static int compareFilterMembersBasedOnActualDataType(String filterMember1,
      String filterMember2, DataType dataType) {
    try {
      if (dataType == DataTypes.SHORT ||
          dataType == DataTypes.INT ||
          dataType == DataTypes.LONG ||
          dataType == DataTypes.DOUBLE) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
          return 1;
        }
        Double d1 = Double.parseDouble(filterMember1);
        Double d2 = Double.parseDouble(filterMember2);
        return d1.compareTo(d2);
      } else if (DataTypes.isDecimal(dataType)) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
          return 1;
        }
        java.math.BigDecimal val1 = new BigDecimal(filterMember1);
        java.math.BigDecimal val2 = new BigDecimal(filterMember2);
        return val1.compareTo(val2);
      } else if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
          return 1;
        }
        String format = null;
        if (dataType == DataTypes.DATE) {
          format = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                  CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
        } else {
          format = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        }
        SimpleDateFormat parser = new SimpleDateFormat(format);
        Date date1 = null;
        Date date2 = null;
        date1 = parser.parse(filterMember1);
        date2 = parser.parse(filterMember2);
        return date1.compareTo(date2);
      } else {
        return filterMember1.compareTo(filterMember2);
      }
    } catch (ParseException | NumberFormatException e) {
      return -1;
    }
  }

  private static void fillNullValuesStartIndexWithDefaultKeys(
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      if (null == setOfStartKeyByteArray.get(dimension.getOrdinal())) {
        setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { 0 });
      }

    }
  }

  private static void fillNullValuesEndIndexWithDefaultKeys(
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      if (null == setOfStartKeyByteArray.get(dimension.getOrdinal())) {
        setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { (byte) 0xFF });
      }

    }
  }

  private static void pruneStartAndEndKeys(SortedMap<Integer, byte[]> setOfStartKeyByteArray,
      List<byte[]> listOfStartKeyByteArray) {
    for (Map.Entry<Integer, byte[]> entry : setOfStartKeyByteArray.entrySet()) {
      listOfStartKeyByteArray.add(entry.getValue());
    }
  }

  private static void fillDefaultStartValue(SortedMap<Integer, byte[]> setOfStartKeyByteArray,
      SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { 0 });
    }

  }

  private static void fillDefaultEndValue(SortedMap<Integer, byte[]> setOfEndKeyByteArray,
      SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      setOfEndKeyByteArray.put(dimension.getOrdinal(), new byte[] { (byte) 0xFF });
    }
  }

  private static void traverseResolverTreeAndPopulateStartAndEndKeys(
      FilterResolverIntf filterResolverTree, SegmentProperties segmentProperties, long[] startKeys,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, long[] endKeys,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray, List<long[]> startKeyList,
      List<long[]> endKeyList) {
    if (null == filterResolverTree) {
      return;
    }
    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolverTree.getLeft(),
        segmentProperties, startKeys, setOfStartKeyByteArray, endKeys, setOfEndKeyByteArray,
        startKeyList, endKeyList);
    filterResolverTree
        .getStartKey(segmentProperties, startKeys, setOfStartKeyByteArray, startKeyList);
    filterResolverTree.getEndKey(segmentProperties, endKeys, setOfEndKeyByteArray,
        endKeyList);

    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolverTree.getRight(),
        segmentProperties, startKeys, setOfStartKeyByteArray, endKeys, setOfEndKeyByteArray,
        startKeyList, endKeyList);
  }

  /**
   * Method will find whether the expression needs to be resolved, this can happen
   * if the expression is exclude and data type is null(mainly in IS NOT NULL filter scenario)
   *
   * @param rightExp
   * @param isIncludeFilter
   * @return
   */
  public static boolean isExpressionNeedsToResolved(Expression rightExp, boolean isIncludeFilter) {
    if (!isIncludeFilter && rightExp instanceof LiteralExpression && (
        DataTypes.NULL == ((LiteralExpression) rightExp)
            .getLiteralExpDataType())) {
      return true;
    }
    for (Expression child : rightExp.getChildren()) {
      if (isExpressionNeedsToResolved(child, isIncludeFilter)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method will print the error log.
   *
   * @param e
   */
  public static void logError(Throwable e, boolean invalidRowsPresent) {
    if (!invalidRowsPresent) {
      LOGGER.error(e, CarbonCommonConstants.FILTER_INVALID_MEMBER + e.getMessage());
    }
  }

  /**
   * This method will compare double values for its equality and also it will preserve
   * the -0.0 and 0.0 equality as per == ,also preserve NaN equality check as per
   * java.lang.Double.equals()
   *
   * @param d1 double value for equality check
   * @param d2 double value for equality check
   * @return boolean after comparing two double values.
   */
  public static boolean nanSafeEqualsDoubles(Double d1, Double d2) {
    if ((d1.doubleValue() == d2.doubleValue()) || (Double.isNaN(d1) && Double.isNaN(d2))) {
      return true;
    }
    return false;

  }

  /**
   * This method will create default bitset group. Applicable for restructure scenarios.
   *
   * @param pageCount
   * @param totalRowCount
   * @param defaultValue
   * @return
   */
  public static BitSetGroup createBitSetGroupWithDefaultValue(int pageCount, int totalRowCount,
      boolean defaultValue) {
    BitSetGroup bitSetGroup = new BitSetGroup(pageCount);
    int numberOfRows = CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
    int pagesTobeFullFilled = totalRowCount / numberOfRows;
    int rowCountForLastPage = totalRowCount % numberOfRows;
    for (int i = 0; i < pagesTobeFullFilled; i++) {
      BitSet bitSet = new BitSet(numberOfRows);
      bitSet.set(0, numberOfRows, defaultValue);
      bitSetGroup.setBitSet(bitSet, i);
    }
    // create and fill bitset for the last page if any records are left
    if (rowCountForLastPage > 0) {
      BitSet bitSet = new BitSet(rowCountForLastPage);
      bitSet.set(0, rowCountForLastPage, defaultValue);
      bitSetGroup.setBitSet(bitSet, pagesTobeFullFilled);
    }
    return bitSetGroup;
  }

  /**
   * This method will compare the selected data against null values and
   * flip the bitSet if any null value is found
   *
   * @param dimensionColumnPage
   * @param bitSet
   */
  public static void removeNullValues(DimensionColumnPage dimensionColumnPage,
      BitSet bitSet, byte[] defaultValue) {
    if (!bitSet.isEmpty()) {
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
        if (dimensionColumnPage.compareTo(i, defaultValue) == 0) {
          bitSet.flip(i);
        }
      }
    }
  }

  public static void updateIndexOfColumnExpression(Expression exp, int dimOridnalMax) {
    // if expression is null, not require to update index.
    if (exp == null) {
      return;
    }
    if (exp.getChildren() == null || exp.getChildren().size() == 0) {
      if (exp instanceof ColumnExpression) {
        ColumnExpression ce = (ColumnExpression) exp;
        CarbonColumn column = ce.getCarbonColumn();
        if (column.isDimension()) {
          ce.setColIndex(column.getOrdinal());
        } else {
          ce.setColIndex(dimOridnalMax + column.getOrdinal());
        }
      }
    } else {
      if (exp.getChildren().size() > 0) {
        List<Expression> children = exp.getChildren();
        for (int i = 0; i < children.size(); i++) {
          updateIndexOfColumnExpression(children.get(i), dimOridnalMax);
        }
      }
    }
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in DimColumnFilterInfo
   *
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return
   */
  public static ColumnFilterInfo getImplicitColumnFilterList(List<String> evaluateResultListFinal,
      boolean isIncludeFilter) {
    ColumnFilterInfo columnFilterInfo = new ColumnFilterInfo();
    columnFilterInfo.setIncludeFilter(isIncludeFilter);
    if (null != evaluateResultListFinal) {
      columnFilterInfo.setImplicitColumnFilterList(evaluateResultListFinal);
    }
    return columnFilterInfo;
  }

  /**
   * This method will check for ColumnExpression with column name positionID and if found will
   * replace the InExpression with true expression. This is done to stop serialization of List
   * expression which is right children of InExpression as it can impact the query performance
   * as the size of list grows bigger.
   *
   * @param expression
   */
  public static void removeInExpressionNodeWithPositionIdColumn(Expression expression) {
    ExpressionType filterExpressionType = expression.getFilterExpressionType();
    if (ExpressionType.AND == filterExpressionType) {
      Expression rightExpression = ((AndExpression) expression).getRight();
      if (rightExpression instanceof InExpression) {
        List<Expression> children = rightExpression.getChildren();
        if (null != children && !children.isEmpty()) {
          Expression childExpression = children.get(0);
          // check for the positionId as the column name in ColumnExpression
          if (childExpression instanceof ColumnExpression && ((ColumnExpression) childExpression)
              .getColumnName().equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)) {
            // Remove the right expression node and point the expression to left node expression
            expression
                .findAndSetChild(((AndExpression) expression).getRight(), new TrueExpression(null));
            LOGGER.info("In expression removed from the filter expression list to prevent it from"
                + " serializing on executor");
          }
        }
      }
    }
  }
}
