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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.evaluators.DimColumnExecuterFilterInfo;
import org.carbondata.query.evaluators.DimColumnResolvedFilterInfo;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.executer.AndFilterExecuterImpl;
import org.carbondata.query.filter.executer.ExcludeFilterExecuterImpl;
import org.carbondata.query.filter.executer.FilterExecuter;
import org.carbondata.query.filter.executer.IncludeFilterExecuterImpl;
import org.carbondata.query.filter.executer.OrFilterExecuterImpl;
import org.carbondata.query.filter.executer.RestructureFilterExecuterImpl;
import org.carbondata.query.filter.executer.RowLevelFilterExecuterImpl;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filter.resolver.RowLevelFilterResolverImpl;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;
import org.carbondata.query.util.DataTypeConverter;

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
   * @param blockKeyGenerator
   * @return FilterExecuter instance
   */
  private static FilterExecuter createFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, KeyGenerator blockKeyGenerator) {
    FilterExecuterType filterExecuterType = filterExpressionResolverTree.getFilterExecuterType();
    switch (filterExecuterType) {
      case INCLUDE:
        return new IncludeFilterExecuterImpl(
            filterExpressionResolverTree.getDimColResolvedFilterInfo(), blockKeyGenerator);
      case EXCLUDE:
        return new ExcludeFilterExecuterImpl(
            filterExpressionResolverTree.getDimColResolvedFilterInfo(), blockKeyGenerator);
      case OR:
        return new OrFilterExecuterImpl(
            createFilterExecuterTree(filterExpressionResolverTree.getLeft(), blockKeyGenerator),
            createFilterExecuterTree(filterExpressionResolverTree.getRight(), blockKeyGenerator));
      case AND:
        return new AndFilterExecuterImpl(
            createFilterExecuterTree(filterExpressionResolverTree.getLeft(), blockKeyGenerator),
            createFilterExecuterTree(filterExpressionResolverTree.getRight(), blockKeyGenerator));
      case RESTRUCTURE:
        return new RestructureFilterExecuterImpl(
            filterExpressionResolverTree.getDimColResolvedFilterInfo(), blockKeyGenerator);
      case ROWLEVEL:
        if (filterExpressionResolverTree instanceof RowLevelFilterResolverImpl) {
          return new RowLevelFilterExecuterImpl(
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getDimColEvaluatorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getMsrColEvalutorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier());
        }

    }
    return null;

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
  private static DimColumnFilterInfo getNoDictionaryValKeyMemberForFilter(
      AbsoluteTableIdentifier tableIdentifier, ColumnExpression columnExpression,
      List<String> evaluateResultListFinal, boolean isIncludeFilter) {
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    for (String result : evaluateResultListFinal) {
      filterValuesList.add(result.getBytes());
    }
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
      throws QueryExecutionException

  {
    List<Integer> surrogates = new ArrayList<Integer>(20);
    for (String resultVal : evaluateResultList) {
      // Reading the dictionary value from cache.
      Integer dictionaryVal =
          getDictionaryValue(resultVal, tableIdentifier, columnExpression.getDimension());
      if (null != dictionaryVal) {
        surrogates.add(dictionaryVal);
      }
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

  /**
   * This API will get the Dictionary value for the respective filter member
   * string.
   *
   * @param value           filter value
   * @param tableIdentifier
   * @param dim             , column expression dimension type.
   * @return the dictionary value.
   * @throws QueryExecutionException
   */
  private static Integer getDictionaryValue(String value, AbsoluteTableIdentifier tableIdentifier,
      CarbonDimension dim) throws QueryExecutionException {
    Dictionary forwardDictionary = getForwardDictionaryCache(tableIdentifier, dim);
    if (null != forwardDictionary) {
      return forwardDictionary.getSurrogateKey(value);
    }
    return null;
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
   * @throws QueryExecutionException
   */
  public static DimColumnFilterInfo getFilterListForAllValues(
      AbsoluteTableIdentifier tableIdentifier, Expression expression,
      ColumnExpression columnExpression, boolean isIncludeFilter) throws QueryExecutionException {
    Dictionary forwardDictionary =
        FilterUtil.getForwardDictionaryCache(tableIdentifier, columnExpression.getDimension());
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    DictionaryChunksWrapper dictionaryWrapper = forwardDictionary.getDictionaryChunks();
    while (dictionaryWrapper.hasNext()) {
      byte[] columnVal = dictionaryWrapper.next();
      try {
        RowIntf row = new RowImpl();
        String stringValue = new String(columnVal);
        if (stringValue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
          stringValue = null;
        }
        row.setValues(new Object[] { DataTypeConverter.getDataBasedOnDataType(stringValue,
            columnExpression.getCarbonColumn().getDataType()) });
        Boolean rslt = expression.evaluate(row).getBoolean();
        if (null != rslt && !(rslt ^ isIncludeFilter)) {
          if (null == stringValue) {
            evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
          } else {
            evaluateResultListFinal.add(stringValue);
          }
        }
      } catch (FilterUnsupportedException e) {
        LOGGER.audit(e.getMessage());
      }
    }
    return getFilterValues(tableIdentifier, columnExpression, evaluateResultListFinal,
        isIncludeFilter);
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
   */
  public static DimColumnFilterInfo getFilterList(AbsoluteTableIdentifier tableIdentifier,
      Expression expression, ColumnExpression columnExpression, boolean isIncludeFilter)
      throws QueryExecutionException {
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
    } catch (FilterUnsupportedException e) {
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
   */
  public static DimColumnFilterInfo getFilterListForRS(Expression expression,
      ColumnExpression columnExpression, String defaultValues, int defaultSurrogate) {
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
    } catch (FilterUnsupportedException e) {
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
   */
  public static DimColumnFilterInfo getFilterListForAllMembersRS(Expression expression,
      ColumnExpression columnExpression, String defaultValues, int defaultSurrogate,
      boolean isIncludeFilter) {
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
      row.setValues(new Object[] { DataTypeConverter.getDataBasedOnDataType(defaultValues,
          columnExpression.getCarbonColumn().getDataType()) });
      Boolean rslt = expression.evaluate(row).getBoolean();
      if (null != rslt && !(rslt ^ isIncludeFilter)) {
        if (null == defaultValues) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        } else {
          evaluateResultListFinal.add(defaultValues);
        }
      }
    } catch (FilterUnsupportedException e) {
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
    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    Arrays.fill(keys, 0);
    int[] rangesForMaskedByte =
        getRangesForMaskedByte((carbonDimension.getOrdinal()), blockLevelKeyGenerator);
    for (Integer surrogate : dimColumnFilterInfo.getFilterList()) {
      try {
        keys[carbonDimension.getOrdinal()] = surrogate;
        filterValuesList
            .add(getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys)));
      } catch (KeyGenException e) {
        LOGGER.error(e.getMessage());
      }
    }
    return filterValuesList.toArray(new byte[filterValuesList.size()][]);

  }

  /**
   * Method will return the start key based on KeyGenerator for the respective
   * filter resolved instance.
   *
   * @param dimColResolvedFilterInfo
   * @param keyGen
   * @return long[] start key
   */
  public static long[] getStartKey(DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      KeyGenerator keyGen) {
    long[] startKey = new long[keyGen.getDimCount()];
    Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
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
      getStartKeyBasedOnFilterResoverInfo(dimensionFilter, startKey);
    }
    return startKey;
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
        if (startKey[entry.getKey().getOrdinal()] < info.getFilterList().get(0)) {
          startKey[entry.getKey().getOrdinal()] = info.getFilterList().get(0);
        }
      }
    }
  }

  public static long[] getEndKey(Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter,
      AbsoluteTableIdentifier tableIdentifier, long[] endKey,
      List<CarbonDimension> carbonDimensions) throws QueryExecutionException {

    carbonDimensions = getCarbonDimsMappedToKeyGenerator(carbonDimensions);
    for (int i = 0; i < endKey.length; i++) {
      endKey[i] = getMaxValue(tableIdentifier, carbonDimensions.get(i));
    }
    getEndKeyWithFilter(dimensionFilter, endKey);
    return endKey;
  }

  private static List<CarbonDimension> getCarbonDimsMappedToKeyGenerator(
      List<CarbonDimension> carbonDimensions) {
    List<CarbonDimension> listOfCarbonDimPartOfKeyGen =
        new ArrayList<CarbonDimension>(carbonDimensions.size());
    for (CarbonDimension carbonDim : carbonDimensions) {
      if (CarbonUtil.hasEncoding(carbonDim.getEncoder(), Encoding.DICTIONARY)) {
        listOfCarbonDimPartOfKeyGen.add(carbonDim);
      }

    }
    return listOfCarbonDimPartOfKeyGen;
  }

  private static void getEndKeyWithFilter(
      Map<CarbonDimension, List<DimColumnFilterInfo>> dimensionFilter, long[] endKey) {
    for (Entry<CarbonDimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<DimColumnFilterInfo> values = entry.getValue();
      if (null == values) {
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
        if (endKey[entry.getKey().getOrdinal()] > info.getFilterList()
            .get(info.getFilterList().size() - 1)) {
          endKey[entry.getKey().getOrdinal()] =
              info.getFilterList().get(info.getFilterList().size() - 1);
        }
      }
    }

  }

  /**
   * This API will get the max value of surrogate key which will be used for
   * determining the end key of particular btree.
   *
   * @throws QueryExecutionException
   */
  private static long getMaxValue(AbsoluteTableIdentifier tableIdentifier,
      CarbonDimension carbonDimension) throws QueryExecutionException {
    if (DataType.TIMESTAMP == carbonDimension.getDataType()) {
      return Integer.MAX_VALUE;
    }
    Dictionary forwardDictionary = getForwardDictionaryCache(tableIdentifier, carbonDimension);
    if (null == forwardDictionary) {
      return -1;
    }
    // Get data from all the available slices of the cube
    return forwardDictionary.getDictionaryChunks().getSize();
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
            String.valueOf(carbonDimension.getColumnId()), carbonDimension.getDataType());
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

  public static IndexKey createIndexKeyFromResolvedFilterVal(long[] startKey,
      KeyGenerator keyGenerator) {
    IndexKey indexKey = null;
    try {
      indexKey = new IndexKey(keyGenerator.generateKey(startKey), null);
    } catch (KeyGenException e) {
      LOGGER.error(e.getMessage());
    }
    return indexKey;
  }

  /**
   * API will create an filter executer tree based on the filter resolver
   *
   * @param filterExpressionResolverTree
   * @param blockKeyGenerator
   * @return
   */
  public static FilterExecuter getFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, KeyGenerator blockKeyGenerator) {
    return createFilterExecuterTree(filterExpressionResolverTree, blockKeyGenerator);
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
    if (dimension.getDataType() != DataType.TIMESTAMP) {
      byte[][] keysBasedOnFilter = getKeyArray(filterValues, dimension, blockKeyGenerator);
      dimColumnExecuterInfo.setFilterKeys(keysBasedOnFilter);
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
  public static IndexKey prepareDefaultEndKey(SegmentProperties segmentProperties)
      throws KeyGenException {
    long[] dictionarySurrogateKey =
        new long[segmentProperties.getDimensions().size() - segmentProperties
            .getNumberOfNoDictionaryDimension()];
    Arrays.fill(dictionarySurrogateKey, Long.MAX_VALUE);
    IndexKey endIndexKey;
    byte[] dictionaryendMdkey =
        segmentProperties.getDimensionKeyGenerator().generateKey(dictionarySurrogateKey);
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
    noDictionaryEndKeyBuffer.rewind();
    endIndexKey = new IndexKey(dictionaryendMdkey, noDictionaryEndKeyBuffer.array());
    return endIndexKey;
  }

  /**
   * method will create a default end key in case of no end key is been
   * derived using existing filter or in case of non filter queries.
   *
   * @param segmentProperties
   * @return
   * @throws KeyGenException
   */
  public static IndexKey prepareDefaultStartKey(SegmentProperties segmentProperties)
      throws KeyGenException {
    IndexKey startIndexKey;
    long[] dictionarySurrogateKey =
        new long[segmentProperties.getDimensions().size() - segmentProperties
            .getNumberOfNoDictionaryDimension()];
    byte[] dictionaryStartMdkey =
        segmentProperties.getDimensionKeyGenerator().generateKey(dictionarySurrogateKey);
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
      noDictionaryStartKeyBuffer.put((byte) -128);
    }
    noDictionaryStartKeyBuffer.rewind();
    startIndexKey = new IndexKey(dictionaryStartMdkey, noDictionaryStartKeyBuffer.array());
    return startIndexKey;
  }
}