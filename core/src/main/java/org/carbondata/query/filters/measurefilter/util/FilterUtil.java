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

package org.carbondata.query.filters.measurefilter.util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.cache.dictionary.ForwardDictionary;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.evaluators.DimColumnExecuterFilterInfo;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.LiteralExpression;
import org.carbondata.query.expression.exception.FilterIllegalMemberException;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.executer.AndFilterExecuterImpl;
import org.carbondata.query.filter.executer.ExcludeColGroupFilterExecuterImpl;
import org.carbondata.query.filter.executer.ExcludeFilterExecuterImpl;
import org.carbondata.query.filter.executer.FilterExecuter;
import org.carbondata.query.filter.executer.IncludeColGroupFilterExecuterImpl;
import org.carbondata.query.filter.executer.IncludeFilterExecuterImpl;
import org.carbondata.query.filter.executer.OrFilterExecuterImpl;
import org.carbondata.query.filter.executer.RestructureFilterExecuterImpl;
import org.carbondata.query.filter.executer.RowLevelFilterExecuterImpl;
import org.carbondata.query.filter.executer.RowLevelRangeTypeExecuterFacory;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filter.resolver.RowLevelFilterResolverImpl;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

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
          return getIncludeFilterExecuter(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(), segmentProperties);
        case EXCLUDE:
          return getExcludeFilterExecuter(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(), segmentProperties);
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
        case RESTRUCTURE:
          return new RestructureFilterExecuterImpl(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              segmentProperties.getDimensionKeyGenerator());
        case ROWLEVEL_LESSTHAN:
        case ROWLEVEL_LESSTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN:
          return RowLevelRangeTypeExecuterFacory
              .getRowLevelRangeTypeExecuter(filterExecuterType, filterExpressionResolverTree,
                  segmentProperties);
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
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo, SegmentProperties segmentProperties) {

    if (dimColResolvedFilterInfo.getDimension().isColumnar()) {
      return new IncludeFilterExecuterImpl(dimColResolvedFilterInfo, segmentProperties);
    } else {
      return new IncludeColGroupFilterExecuterImpl(dimColResolvedFilterInfo, segmentProperties);
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
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo, SegmentProperties segmentProperties) {

    if (dimColResolvedFilterInfo.getDimension().isColumnar()) {
      return new ExcludeFilterExecuterImpl(dimColResolvedFilterInfo, segmentProperties);
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
  public static boolean checkIfExpressionContainsUnknownExp(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.UNKNOWN) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfExpressionContainsUnknownExp(child)) {
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
      byteIndexs[j++] = integer.intValue();
    }
    return byteIndexs;
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in DimColumnFilterInfo
   *
   * @param tableIdentifier
   * @param columnExpression
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return DimColumnFilterInfo
   */
  public static DimColumnFilterInfo getNoDictionaryValKeyMemberForFilter(
      AbsoluteTableIdentifier tableIdentifier, ColumnExpression columnExpression,
      List<String> evaluateResultListFinal, boolean isIncludeFilter) {
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    for (String result : evaluateResultListFinal) {
      filterValuesList.add(result.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    }

    Comparator<byte[]> filterNoDictValueComaparator = new Comparator<byte[]>() {

      @Override public int compare(byte[] filterMember1, byte[] filterMember2) {
        // TODO Auto-generated method stub
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterMember1, filterMember2);
      }

    };
    Collections.sort(filterValuesList, filterNoDictValueComaparator);
    DimColumnFilterInfo columnFilterInfo = null;
    if (filterValuesList.size() > 0) {
      columnFilterInfo = new DimColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setFilterListForNoDictionaryCols(filterValuesList);

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
  public static DimColumnFilterInfo getFilterValues(AbsoluteTableIdentifier tableIdentifier,
      ColumnExpression columnExpression, List<String> evaluateResultList, boolean isIncludeFilter)
      throws QueryExecutionException {
    Dictionary forwardDictionary = null;
    try {
      // Reading the dictionary value from cache.
      forwardDictionary =
          getForwardDictionaryCache(tableIdentifier, columnExpression.getDimension());
      return getFilterValues(columnExpression, evaluateResultList, forwardDictionary,
          isIncludeFilter);
    } finally {
      CarbonUtil.clearDictionaryCache(forwardDictionary);
    }
  }

  /**
   * Method will prepare the  dimfilterinfo instance by resolving the filter
   * expression value to its respective surrogates.
   *
   * @param columnExpression
   * @param evaluateResultList
   * @param forwardDictionary
   * @param isIncludeFilter
   * @return
   * @throws QueryExecutionException
   */
  private static DimColumnFilterInfo getFilterValues(ColumnExpression columnExpression,
      List<String> evaluateResultList, Dictionary forwardDictionary, boolean isIncludeFilter)
      throws QueryExecutionException {
    sortFilterModelMembers(columnExpression, evaluateResultList);
    List<Integer> surrogates =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // Reading the dictionary value from cache.
    getDictionaryValue(evaluateResultList, forwardDictionary, surrogates);
    Collections.sort(surrogates);
    DimColumnFilterInfo columnFilterInfo = null;
    if (surrogates.size() > 0) {
      columnFilterInfo = new DimColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setFilterList(surrogates);
    }
    return columnFilterInfo;
  }

  /**
   * This API will get the Dictionary value for the respective filter member
   * string.
   *
   * @param evaluateResultList filter value
   * @param surrogates
   * @throws QueryExecutionException
   */
  private static void getDictionaryValue(List<String> evaluateResultList,
      Dictionary forwardDictionary, List<Integer> surrogates) throws QueryExecutionException {
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
   * @return DimColumnFilterInfo
   * @throws FilterUnsupportedException
   * @throws QueryExecutionException
   */
  public static DimColumnFilterInfo getFilterListForAllValues(
      AbsoluteTableIdentifier tableIdentifier, Expression expression,
      final ColumnExpression columnExpression, boolean isIncludeFilter)
      throws FilterUnsupportedException {
    Dictionary forwardDictionary = null;
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    DictionaryChunksWrapper dictionaryWrapper = null;
    try {
      forwardDictionary =
          getForwardDictionaryCache(tableIdentifier, columnExpression.getDimension());
      dictionaryWrapper = forwardDictionary.getDictionaryChunks();
      while (dictionaryWrapper.hasNext()) {
        byte[] columnVal = dictionaryWrapper.next();
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
          if (null != rslt && !(rslt ^ isIncludeFilter)) {
            if (null == stringValue) {
              evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
            } else {
              evaluateResultListFinal.add(stringValue);
            }
          }
        } catch (FilterIllegalMemberException e) {
          LOGGER.debug(e.getMessage());
        }
      }
      return getFilterValues(columnExpression, evaluateResultListFinal, forwardDictionary,
          isIncludeFilter);
    } catch (QueryExecutionException e) {
      throw new FilterUnsupportedException(e.getMessage());
    } finally {
      CarbonUtil.clearDictionaryCache(forwardDictionary);
    }
  }

  private static void sortFilterModelMembers(final ColumnExpression columnExpression,
      List<String> evaluateResultListFinal) {
    Comparator<String> filterActualValueComaparator = new Comparator<String>() {

      @Override public int compare(String filterMember1, String filterMember2) {
        return compareFilterMembersBasedOnActualDataType(filterMember1, filterMember2,
            columnExpression.getDataType());
      }

    };
    Collections.sort(evaluateResultListFinal, filterActualValueComaparator);
  }

  /**
   * Metahod will resolve the filter member to its respective surrogates by
   * scanning the dictionary cache.
   *
   * @param tableIdentifier
   * @param expression
   * @param columnExpression
   * @param isIncludeFilter
   * @return
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  public static DimColumnFilterInfo getFilterList(AbsoluteTableIdentifier tableIdentifier,
      Expression expression, ColumnExpression columnExpression, boolean isIncludeFilter)
      throws QueryExecutionException, FilterUnsupportedException {
    DimColumnFilterInfo resolvedFilterObject = null;
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

      if (null != columnExpression.getCarbonColumn() && !columnExpression.getCarbonColumn()
          .hasEncoding(Encoding.DICTIONARY)) {
        resolvedFilterObject =
            getNoDictionaryValKeyMemberForFilter(tableIdentifier, columnExpression,
                evaluateResultListFinal, isIncludeFilter);
      } else {
        resolvedFilterObject =
            getFilterValues(tableIdentifier, columnExpression, evaluateResultListFinal,
                isIncludeFilter);
      }
    } catch (FilterIllegalMemberException e) {
      LOGGER.audit(e.getMessage());
    }
    return resolvedFilterObject;
  }

  /**
   * Method will prepare the  dimfilterinfo instance by resolving the filter
   * expression value to its respective surrogates in the scenario of restructure.
   *
   * @param expression
   * @param columnExpression
   * @param defaultValues
   * @param defaultSurrogate
   * @return
   * @throws FilterUnsupportedException
   */
  public static DimColumnFilterInfo getFilterListForRS(Expression expression,
      ColumnExpression columnExpression, String defaultValues, int defaultSurrogate)
      throws FilterUnsupportedException {
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    DimColumnFilterInfo columnFilterInfo = null;
    // List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    // KeyGenerator keyGenerator =
    // KeyGeneratorFactory.getKeyGenerator(new int[] { defaultSurrogate });
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
        columnFilterInfo = new DimColumnFilterInfo();
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
  public static DimColumnFilterInfo getFilterListForAllMembersRS(Expression expression,
      ColumnExpression columnExpression, String defaultValues, int defaultSurrogate,
      boolean isIncludeFilter) throws FilterUnsupportedException {
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    DimColumnFilterInfo columnFilterInfo = null;

    // KeyGenerator keyGenerator =
    // KeyGeneratorFactory.getKeyGenerator(new int[] { defaultSurrogate });
    try {
      RowIntf row = new RowImpl();
      if (defaultValues.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        defaultValues = null;
      }
      row.setValues(new Object[] { DataTypeUtil.getDataBasedOnDataType(defaultValues,
          columnExpression.getCarbonColumn().getDataType()) });
      Boolean rslt = expression.evaluate(row).getBoolean();
      if (null != rslt && !(rslt ^ isIncludeFilter)) {
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
    columnFilterInfo = new DimColumnFilterInfo();
    for (int i = 0; i < evaluateResultListFinal.size(); i++) {
      if (evaluateResultListFinal.get(i).equals(defaultValues)) {
        filterValuesList.add(defaultSurrogate);
        break;
      }
    }
    columnFilterInfo.setFilterList(filterValuesList);
    return columnFilterInfo;
  }

  public static byte[][] getKeyArray(DimColumnFilterInfo dimColumnFilterInfo,
      CarbonDimension carbonDimension, KeyGenerator blockLevelKeyGenerator) {
    if (!carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      return dimColumnFilterInfo.getNoDictionaryFilterValuesList()
          .toArray((new byte[dimColumnFilterInfo.getNoDictionaryFilterValuesList().size()][]));
    }
    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    Arrays.fill(keys, 0);
    int[] rangesForMaskedByte =
        getRangesForMaskedByte((carbonDimension.getKeyOrdinal()), blockLevelKeyGenerator);
    if (null != dimColumnFilterInfo) {
      for (Integer surrogate : dimColumnFilterInfo.getFilterList()) {
        try {
          keys[carbonDimension.getKeyOrdinal()] = surrogate;
          filterValuesList
              .add(getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys)));
        } catch (KeyGenException e) {
          LOGGER.error(e.getMessage());
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
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return long[] start key
   */
  public static long[] getStartKey(DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties, long[] startKey) {
    Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    for (Entry<CarbonDimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<DimColumnFilterInfo> values = entry.getValue();
      if (null == values || !entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      boolean isExcludePresent = false;
      for (DimColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludePresent = true;
        }
      }
      if (isExcludePresent) {
        continue;
      }
      getStartKeyBasedOnFilterResoverInfo(dimensionFilter, startKey);
    }
    return startKey;
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
   * @param segmentProperties
   * @param setOfStartKeyByteArray
   * @return
   */
  public static void getStartKeyForNoDictionaryDimension(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo, SegmentProperties segmentProperties,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray) {
    Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    // step 1
    for (Entry<CarbonDimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      if (!entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        List<DimColumnFilterInfo> listOfDimColFilterInfo = entry.getValue();
        if (null == listOfDimColFilterInfo) {
          continue;
        }
        boolean isExcludePresent = false;
        for (DimColumnFilterInfo info : listOfDimColFilterInfo) {
          if (!info.isIncludeFilter()) {
            isExcludePresent = true;
          }
        }
        if (isExcludePresent) {
          continue;
        }
        // step 2
        byte[] noDictionaryStartKey =
            listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList().get(0);
        if (setOfStartKeyByteArray.isEmpty()) {
          setOfStartKeyByteArray.put(entry.getKey().getOrdinal(), noDictionaryStartKey);
        } else if (null == setOfStartKeyByteArray.get(entry.getKey().getOrdinal())) {
          setOfStartKeyByteArray.put(entry.getKey().getOrdinal(), noDictionaryStartKey);

        } else if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(setOfStartKeyByteArray.get(entry.getKey().getOrdinal()),
                noDictionaryStartKey) > 0) {
          setOfStartKeyByteArray.put(entry.getKey().getOrdinal(), noDictionaryStartKey);
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
   * @param segmentProperties
   * @param setOfEndKeyByteArray
   * @return end key array
   */
  public static void getEndKeyForNoDictionaryDimension(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo, SegmentProperties segmentProperties,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray) {

    Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    // step 1
    for (Entry<CarbonDimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      if (!entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        List<DimColumnFilterInfo> listOfDimColFilterInfo = entry.getValue();
        if (null == listOfDimColFilterInfo) {
          continue;
        }
        boolean isExcludePresent = false;
        for (DimColumnFilterInfo info : listOfDimColFilterInfo) {
          if (!info.isIncludeFilter()) {
            isExcludePresent = true;
          }
        }
        if (isExcludePresent) {
          continue;
        }
        // step 2
        byte[] noDictionaryEndKey = listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList()
            .get(listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList().size() - 1);
        if (setOfEndKeyByteArray.isEmpty()) {
          setOfEndKeyByteArray.put(entry.getKey().getOrdinal(), noDictionaryEndKey);
        } else if (null == setOfEndKeyByteArray.get(entry.getKey().getOrdinal())) {
          setOfEndKeyByteArray.put(entry.getKey().getOrdinal(), noDictionaryEndKey);

        } else if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(setOfEndKeyByteArray.get(entry.getKey().getOrdinal()), noDictionaryEndKey)
            < 0) {
          setOfEndKeyByteArray.put(entry.getKey().getOrdinal(), noDictionaryEndKey);
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
  private static void getStartKeyBasedOnFilterResoverInfo(
      Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter, long[] startKey) {
    for (Entry<CarbonDimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<DimColumnFilterInfo> values = entry.getValue();
      if (null == values) {
        continue;
      }
      boolean isExcludePresent = false;
      for (DimColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludePresent = true;
        }
      }
      if (isExcludePresent) {
        continue;
      }
      for (DimColumnFilterInfo info : values) {
        if (startKey[entry.getKey().getKeyOrdinal()] < info.getFilterList().get(0)) {
          startKey[entry.getKey().getKeyOrdinal()] = info.getFilterList().get(0);
        }
      }
    }
  }

  public static void getEndKey(Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter,
      AbsoluteTableIdentifier tableIdentifier, long[] endKey, SegmentProperties segmentProperties)
      throws QueryExecutionException {

    List<CarbonDimension> updatedDimListBasedOnKeyGenerator =
        getCarbonDimsMappedToKeyGenerator(segmentProperties.getDimensions());
    for (int i = 0; i < endKey.length; i++) {
      endKey[i] = getMaxValue(tableIdentifier, updatedDimListBasedOnKeyGenerator.get(i),
          segmentProperties.getDimColumnsCardinality());
    }
    getEndKeyWithFilter(dimensionFilter, endKey);

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
      Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter, long[] endKey) {
    for (Entry<CarbonDimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<DimColumnFilterInfo> values = entry.getValue();
      if (null == values || !entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      boolean isExcludeFilterPresent = false;
      for (DimColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludeFilterPresent = true;
        }
      }
      if (isExcludeFilterPresent) {
        continue;
      }

      for (DimColumnFilterInfo info : values) {
        if (endKey[entry.getKey().getKeyOrdinal()] > info.getFilterList()
            .get(info.getFilterList().size() - 1)) {
          endKey[entry.getKey().getKeyOrdinal()] =
              info.getFilterList().get(info.getFilterList().size() - 1);
        }
      }
    }

  }

  /**
   * This API will get the max value of surrogate key which will be used for
   * determining the end key of particular btree.
   *
   * @param dimCarinality
   * @throws QueryExecutionException
   */
  private static long getMaxValue(AbsoluteTableIdentifier tableIdentifier,
      CarbonDimension carbonDimension, int[] dimCarinality) throws QueryExecutionException {
    //    if (DataType.TIMESTAMP == carbonDimension.getDataType()) {
    //      return Integer.MAX_VALUE;
    //    }
    // Get data from all the available slices of the cube
    if (null != dimCarinality) {
      return dimCarinality[carbonDimension.getKeyOrdinal()];
    }
    return -1;
  }

  /**
   * @param tableIdentifier
   * @param carbonDimension
   * @return
   * @throws QueryExecutionException
   */
  public static Dictionary getForwardDictionaryCache(AbsoluteTableIdentifier tableIdentifier,
      CarbonDimension carbonDimension) throws QueryExecutionException {
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(tableIdentifier.getCarbonTableIdentifier(),
            carbonDimension.getColumnIdentifier(), carbonDimension.getDataType());
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache forwardDictionaryCache =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, tableIdentifier.getStorePath());
    // get the forward dictionary object
    Dictionary forwardDictionary = null;
    try {
      forwardDictionary = (Dictionary) forwardDictionaryCache.get(dictionaryColumnUniqueIdentifier);
    } catch (CarbonUtilException e) {
      throw new QueryExecutionException(e);
    }
    return forwardDictionary;
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
   * @param blockKeyGenerator
   * @param dimension
   * @param dimColumnExecuterInfo
   */
  public static void prepareKeysFromSurrogates(DimColumnFilterInfo filterValues,
      KeyGenerator blockKeyGenerator, CarbonDimension dimension,
      DimColumnExecuterFilterInfo dimColumnExecuterInfo) {
    byte[][] keysBasedOnFilter = getKeyArray(filterValues, dimension, blockKeyGenerator);
    dimColumnExecuterInfo.setFilterKeys(keysBasedOnFilter);

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
    long[] dictionarySurrogateKey =
        new long[segmentProperties.getDimensions().size() - segmentProperties
            .getNumberOfNoDictionaryDimension()];
    Arrays.fill(dictionarySurrogateKey, Long.MAX_VALUE);
    IndexKey endIndexKey;
    byte[] dictionaryendMdkey =
        segmentProperties.getDimensionKeyGenerator().generateKey(dictionarySurrogateKey);
    byte[] noDictionaryEndKeyBuffer = getNoDictionaryDefaultEndKey(segmentProperties);
    endIndexKey = new IndexKey(dictionaryendMdkey, noDictionaryEndKeyBuffer);
    return endIndexKey;
  }

  public static byte[] getNoDictionaryDefaultEndKey(SegmentProperties segmentProperties) {
    // in case of non filter query when no dictionary columns are present we
    // need to set the default end key, as for non filter query
    // we need to get the last
    // block of the btree so we are setting the max byte value in the end key
    ByteBuffer noDictionaryEndKeyBuffer = ByteBuffer.allocate(
        (segmentProperties.getNumberOfNoDictionaryDimension()
            * CarbonCommonConstants.SHORT_SIZE_IN_BYTE) + segmentProperties
            .getNumberOfNoDictionaryDimension());
    // end key structure will be
    //<Offset of first No Dictionary key in 2 Bytes><Offset of second No Dictionary key in 2 Bytes>
    //<Offset of n No Dictionary key in 2 Bytes><first no dictionary column value>
    // <second no dictionary column value> <N no dictionary column value>
    //example if we have 2 no dictionary column
    //<[0,4,0,5,127,127]>
    short startPoint = (short) (segmentProperties.getNumberOfNoDictionaryDimension()
        * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    for (int i = 0; i < segmentProperties.getNumberOfNoDictionaryDimension(); i++) {
      noDictionaryEndKeyBuffer.putShort((startPoint));
      startPoint++;
    }
    for (int i = 0; i < segmentProperties.getNumberOfNoDictionaryDimension(); i++) {
      noDictionaryEndKeyBuffer.put((byte) 127);
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
    long[] dictionarySurrogateKey =
        new long[segmentProperties.getDimensions().size() - segmentProperties
            .getNumberOfNoDictionaryDimension()];
    byte[] dictionaryStartMdkey =
        segmentProperties.getDimensionKeyGenerator().generateKey(dictionarySurrogateKey);
    byte[] noDictionaryStartKeyArray = getNoDictionaryDefaultStartKey(segmentProperties);

    startIndexKey = new IndexKey(dictionaryStartMdkey, noDictionaryStartKeyArray);
    return startIndexKey;
  }

  public static byte[] getNoDictionaryDefaultStartKey(SegmentProperties segmentProperties) {
    // in case of non filter query when no dictionary columns are present we
    // need to set the default start key, as for non filter query we need to get the first
    // block of the btree so we are setting the least byte value in the start key
    ByteBuffer noDictionaryStartKeyBuffer = ByteBuffer.allocate(
        (segmentProperties.getNumberOfNoDictionaryDimension()
            * CarbonCommonConstants.SHORT_SIZE_IN_BYTE) + segmentProperties
            .getNumberOfNoDictionaryDimension());
    // end key structure will be
    //<Offset of first No Dictionary key in 2 Bytes><Offset of second No Dictionary key in 2 Bytes>
    //<Offset of n No Dictionary key in 2 Bytes><first no dictionary column value>
    // <second no dictionary column value> <N no dictionary column value>
    //example if we have 2 no dictionary column
    //<[0,4,0,5,0,0]>
    short startPoint = (short) (segmentProperties.getNumberOfNoDictionaryDimension()
        * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    for (int i = 0; i < segmentProperties.getNumberOfNoDictionaryDimension(); i++) {
      noDictionaryStartKeyBuffer.putShort((startPoint));
      startPoint++;
    }
    for (int i = 0; i < segmentProperties.getNumberOfNoDictionaryDimension(); i++) {
      noDictionaryStartKeyBuffer.put((byte) 0);
    }
    return noDictionaryStartKeyBuffer.array();
  }

  public static int compareFilterKeyBasedOnDataType(String dictionaryVal, String memberVal,
      DataType dataType) {
    try {
      switch (dataType) {
        case INT:

          return Integer.compare((Integer.parseInt(dictionaryVal)), (Integer.parseInt(memberVal)));
        case DOUBLE:
          return Double
              .compare((Double.parseDouble(dictionaryVal)), (Double.parseDouble(memberVal)));
        case LONG:
          return Long.compare((Long.parseLong(dictionaryVal)), (Long.parseLong(memberVal)));
        case BOOLEAN:
          return Boolean
              .compare((Boolean.parseBoolean(dictionaryVal)), (Boolean.parseBoolean(memberVal)));
        case TIMESTAMP:
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr;
          Date dictionaryDate;
          dateToStr = parser.parse(memberVal);
          dictionaryDate = parser.parse(dictionaryVal);
          return dictionaryDate.compareTo(dateToStr);

        case DECIMAL:
          java.math.BigDecimal javaDecValForDictVal = new java.math.BigDecimal(dictionaryVal);
          java.math.BigDecimal javaDecValForMemberVal = new java.math.BigDecimal(memberVal);
          return javaDecValForDictVal.compareTo(javaDecValForMemberVal);
        default:
          return -1;
      }
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * method will set the start and end key for as per the filter resolver tree
   * utilized visitor pattern inorder to populate the start and end key population.
   *
   * @param segmentProperties
   * @param tableIdentifier
   * @param filterResolver
   * @param listOfStartEndKeys
   * @throws QueryExecutionException
   */
  public static void traverseResolverTreeAndGetStartAndEndKey(SegmentProperties segmentProperties,
      AbsoluteTableIdentifier tableIdentifier, FilterResolverIntf filterResolver,
      List<IndexKey> listOfStartEndKeys) throws QueryExecutionException {
    IndexKey searchStartKey = null;
    IndexKey searchEndKey = null;
    long[] startKey = new long[segmentProperties.getDimensionKeyGenerator().getDimCount()];
    long[] endKey = new long[segmentProperties.getDimensionKeyGenerator().getDimCount()];
    List<byte[]> listOfStartKeyByteArray =
        new ArrayList<byte[]>(segmentProperties.getNumberOfNoDictionaryDimension());
    List<byte[]> listOfEndKeyByteArray =
        new ArrayList<byte[]>(segmentProperties.getNumberOfNoDictionaryDimension());
    SortedMap<Integer, byte[]> setOfStartKeyByteArray = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> setOfEndKeyByteArray = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> defaultStartValues = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> defaultEndValues = new TreeMap<Integer, byte[]>();
    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolver, tableIdentifier,
        segmentProperties, startKey, setOfStartKeyByteArray, endKey, setOfEndKeyByteArray);
    fillDefaultStartValue(defaultStartValues, segmentProperties);
    fillDefaultEndValue(defaultEndValues, segmentProperties);
    fillNullValuesStartIndexWithDefaultKeys(setOfStartKeyByteArray, segmentProperties);
    fillNullValuesEndIndexWithDefaultKeys(setOfEndKeyByteArray, segmentProperties);
    pruneStartAndEndKeys(setOfStartKeyByteArray, listOfStartKeyByteArray);
    pruneStartAndEndKeys(setOfEndKeyByteArray, listOfEndKeyByteArray);

    searchStartKey = FilterUtil
        .createIndexKeyFromResolvedFilterVal(startKey, segmentProperties.getDimensionKeyGenerator(),
            FilterUtil.getKeyWithIndexesAndValues(listOfStartKeyByteArray));

    searchEndKey = FilterUtil
        .createIndexKeyFromResolvedFilterVal(endKey, segmentProperties.getDimensionKeyGenerator(),
            FilterUtil.getKeyWithIndexesAndValues(listOfEndKeyByteArray));
    listOfStartEndKeys.add(searchStartKey);
    listOfStartEndKeys.add(searchEndKey);

  }

  private static int compareFilterMembersBasedOnActualDataType(String filterMember1,
      String filterMember2, org.carbondata.query.expression.DataType dataType) {
    try {
      switch (dataType) {
        case IntegerType:
        case LongType:
        case DoubleType:

          if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
            return 1;
          }
          Double d1 = Double.parseDouble(filterMember1);
          Double d2 = Double.parseDouble(filterMember2);
          return d1.compareTo(d2);
        case DecimalType:
          if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
            return 1;
          }
          java.math.BigDecimal val1 = new BigDecimal(filterMember1);
          java.math.BigDecimal val2 = new BigDecimal(filterMember2);
          return val1.compareTo(val2);
        case TimestampType:
          if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
            return 1;
          }
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date date1 = null;
          Date date2 = null;
          date1 = parser.parse(filterMember1);
          date2 = parser.parse(filterMember2);
          return date1.compareTo(date2);
        case StringType:
        default:
          return filterMember1.compareTo(filterMember2);
      }
    } catch (Exception e) {
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
        setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { 127 });
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
      setOfEndKeyByteArray.put(dimension.getOrdinal(), new byte[] { 127 });
    }
  }

  private static void traverseResolverTreeAndPopulateStartAndEndKeys(
      FilterResolverIntf filterResolverTree, AbsoluteTableIdentifier tableIdentifier,
      SegmentProperties segmentProperties, long[] startKeys,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, long[] endKeys,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray) throws QueryExecutionException {
    if (null == filterResolverTree) {
      return;
    }
    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolverTree.getLeft(), tableIdentifier,
        segmentProperties, startKeys, setOfStartKeyByteArray, endKeys, setOfEndKeyByteArray);

    filterResolverTree.getStartKey(segmentProperties, startKeys, setOfStartKeyByteArray);
    filterResolverTree.getEndKey(segmentProperties, tableIdentifier, endKeys, setOfEndKeyByteArray);

    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolverTree.getRight(), tableIdentifier,
        segmentProperties, startKeys, setOfStartKeyByteArray, endKeys, setOfEndKeyByteArray);
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
        org.carbondata.query.expression.DataType.NullType == ((LiteralExpression) rightExp)
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
      invalidRowsPresent = true;
      LOGGER.error(e, CarbonCommonConstants.FILTER_INVALID_MEMBER + e.getMessage());
    }
  }
}