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

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.*;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.executer.AndFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.DimColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.ExcludeFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.FalseFilterExecutor;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.IncludeFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.MeasureColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.OrFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RangeValueFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureExcludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelRangeTypeExecuterFactory;
import org.apache.carbondata.core.scan.filter.executer.TrueFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import org.apache.log4j.Logger;

public final class FilterUtil {
  private static final Logger LOGGER =
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
   * @param complexDimensionInfoMap
   * @param minMaxCacheColumns
   * @param isStreamDataFile: whether create filter executer tree for stream data files
   * @return FilterExecuter instance
   *
   */
  private static FilterExecuter createFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    FilterExecuterType filterExecuterType = filterExpressionResolverTree.getFilterExecuterType();
    if (null != filterExecuterType) {
      switch (filterExecuterType) {
        case INCLUDE:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1704
          if (null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              && null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues() && filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues().isOptimized()) {
            return getExcludeFilterExecuter(
                filterExpressionResolverTree.getDimColResolvedFilterInfo(),
                filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
          }
          // return true filter expression if filter column min/max is not cached in driver
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(filterExpressionResolverTree,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
              segmentProperties, minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile),
              createFilterExecuterTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile));
        case AND:
          return new AndFilterExecuterImpl(
              createFilterExecuterTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile),
              createFilterExecuterTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile));
        case ROWLEVEL_LESSTHAN:
        case ROWLEVEL_LESSTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN:
          // return true filter expression if filter column min/max is not cached in driver
          RowLevelRangeFilterResolverImpl rowLevelRangeFilterResolver =
              (RowLevelRangeFilterResolverImpl) filterExpressionResolverTree;
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(
              rowLevelRangeFilterResolver.getDimColEvaluatorInfoList(),
              rowLevelRangeFilterResolver.getMsrColEvalutorInfoList(), segmentProperties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
              minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2277
          return RowLevelRangeTypeExecuterFactory
              .getRowLevelRangeTypeExecuter(filterExecuterType, filterExpressionResolverTree,
                  segmentProperties);
        case RANGE:
          // return true filter expression if filter column min/max is not cached in driver
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(filterExpressionResolverTree,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
              segmentProperties, minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return new RangeValueFilterExecuterImpl(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getFilterExpression(),
              ((ConditionalFilterResolverImpl) filterExpressionResolverTree)
                  .getFilterRangeValues(segmentProperties), segmentProperties);
        case TRUE:
          return new TrueFilterExecutor();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1454
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
    if (null != msrColResolvedFilterInfo && msrColResolvedFilterInfo.getMeasure().isMeasure()) {
      CarbonMeasure measuresFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(msrColResolvedFilterInfo.getMeasure());
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1854
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    CarbonDimension dimension = dimColResolvedFilterInfo.getDimension();
    if (dimension.hasEncoding(Encoding.IMPLICIT)) {
      return new ImplicitIncludeFilterExecutorImpl(dimColResolvedFilterInfo);
    } else {
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
    }
  }

  /**
   * check if current need to be replaced with TrueFilter expression. This will happen in case
   * filter column min/max is not cached in the driver
   *
   * @param dimColEvaluatorInfoList
   * @param msrColEvaluatorInfoList
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @return
   */
  private static boolean checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvaluatorInfoList,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
      SegmentProperties segmentProperties, List<CarbonColumn> minMaxCacheColumns,
      boolean isStreamDataFile) {
    boolean replaceCurrentNodeWithTrueFilter = false;
    ColumnResolvedFilterInfo columnResolvedFilterInfo = null;
    if (!msrColEvaluatorInfoList.isEmpty()) {
      columnResolvedFilterInfo = msrColEvaluatorInfoList.get(0);
      replaceCurrentNodeWithTrueFilter =
          checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
              minMaxCacheColumns, true, isStreamDataFile);
    } else {
      columnResolvedFilterInfo = dimColEvaluatorInfoList.get(0);
      if (!columnResolvedFilterInfo.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        replaceCurrentNodeWithTrueFilter =
            checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
                minMaxCacheColumns, false, isStreamDataFile);
      }
    }
    return replaceCurrentNodeWithTrueFilter;
  }

  /**
   * check if current need to be replaced with TrueFilter expression. This will happen in case
   * filter column min/max is not cached in the driver
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @Param isStreamDataFile
   * @return
   */
  private static boolean checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    boolean replaceCurrentNodeWithTrueFilter = false;
    ColumnResolvedFilterInfo columnResolvedFilterInfo = null;
    if (null != filterExpressionResolverTree.getMsrColResolvedFilterInfo()) {
      columnResolvedFilterInfo = filterExpressionResolverTree.getMsrColResolvedFilterInfo();
      replaceCurrentNodeWithTrueFilter =
          checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
              minMaxCacheColumns, true, isStreamDataFile);
    } else {
      columnResolvedFilterInfo = filterExpressionResolverTree.getDimColResolvedFilterInfo();
      if (!columnResolvedFilterInfo.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        replaceCurrentNodeWithTrueFilter =
            checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
                minMaxCacheColumns, false, isStreamDataFile);
      }
    }
    return replaceCurrentNodeWithTrueFilter;
  }

  /**
   * Method to check whether current node needs to be replaced with true filter to avoid pruning
   * for case when filter column is not cached in the min/max cached dimension
   *
   * @param columnResolvedFilterInfo
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @param isMeasure
   * @return
   */
  private static boolean checkIfFilterColumnIsCachedInDriver(
      ColumnResolvedFilterInfo columnResolvedFilterInfo, SegmentProperties segmentProperties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
      List<CarbonColumn> minMaxCacheColumns, boolean isMeasure, boolean isStreamDataFile) {
    boolean replaceCurrentNodeWithTrueFilter = false;
    CarbonColumn columnFromCurrentBlock = null;
    if (isMeasure) {
      columnFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(columnResolvedFilterInfo.getMeasure());
    } else {
      columnFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(columnResolvedFilterInfo.getDimension());
    }
    if (null != columnFromCurrentBlock) {
      // check for filter dimension in the cached column list
      if (null != minMaxCacheColumns) {
        int columnIndexInMinMaxByteArray =
            getFilterColumnIndexInCachedColumns(minMaxCacheColumns, columnFromCurrentBlock);
        if (columnIndexInMinMaxByteArray != -1) {
          columnResolvedFilterInfo.setColumnIndexInMinMaxByteArray(columnIndexInMinMaxByteArray);
        } else {
          // will be true only if column caching is enabled and current filter column is not cached
          replaceCurrentNodeWithTrueFilter = true;
        }
      } else {
        // if columns to be cached are not specified then in that case all columns will be cached
        // and  then the ordinal of column will be its index in the min/max byte array
        if (isMeasure) {
          // when read from stream data file, minmax columns cache don't include complex columns,
          // so it can not use 'segmentProperties.getLastDimensionColOrdinal()' as
          // last dimension ordinal.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
          if (isStreamDataFile) {
            columnResolvedFilterInfo.setColumnIndexInMinMaxByteArray(
                segmentProperties.getDimensions().size() + columnFromCurrentBlock.getOrdinal());
          } else {
            columnResolvedFilterInfo.setColumnIndexInMinMaxByteArray(
                segmentProperties.getLastDimensionColOrdinal() + columnFromCurrentBlock
                .getOrdinal());
          }
        } else {
          columnResolvedFilterInfo
              .setColumnIndexInMinMaxByteArray(columnFromCurrentBlock.getOrdinal());
        }
      }
    }
    return replaceCurrentNodeWithTrueFilter;
  }

  /**
   * Method to check whether the filter dimension exists in the cached dimensions for a table
   *
   * @param carbonDimensionsToBeCached
   * @param filterColumn
   * @return
   */
  private static int getFilterColumnIndexInCachedColumns(
      List<CarbonColumn> carbonDimensionsToBeCached, CarbonColumn filterColumn) {
    int columnIndexInMinMaxByteArray = -1;
    int columnCounter = 0;
    for (CarbonColumn cachedColumn : carbonDimensionsToBeCached) {
      if (cachedColumn.getColumnId().equalsIgnoreCase(filterColumn.getColumnId())) {
        columnIndexInMinMaxByteArray = columnCounter;
        break;
      }
      columnCounter++;
    }
    return columnIndexInMinMaxByteArray;
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

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
    if (null != msrColResolvedFilterInfo && msrColResolvedFilterInfo.getMeasure().isMeasure()) {
      CarbonMeasure measuresFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(msrColResolvedFilterInfo.getMeasure());
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
            msrColResolvedFilterInfo, true);
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
          msrColResolvedFilterInfo, false);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1326
    if (expression.getFilterExpressionType() == ExpressionType.LITERAL
        && expression instanceof LiteralExpression) {
      DataType dataType = ((LiteralExpression) expression).getLiteralExpDataType();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
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
   * This method will get the no dictionary data based on filters and same
   * will be in ColumnFilterInfo
   *
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return ColumnFilterInfo
   */
  public static ColumnFilterInfo getNoDictionaryValKeyMemberForFilter(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-782
      List<String> evaluateResultListFinal, boolean isIncludeFilter, DataType dataType)
      throws FilterUnsupportedException {
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    String result = null;
    try {
      int length = evaluateResultListFinal.size();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1450
      String timeFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      for (int i = 0; i < length; i++) {
        result = evaluateResultListFinal.get(i);
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(result)) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
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

      @Override
      public int compare(byte[] filterMember1, byte[] filterMember2) {
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

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
        filterValuesList.add(DataTypeUtil
            .getMeasureValueBasedOnDataType(result, dataType, carbonMeasure.getScale(),
                carbonMeasure.getPrecision()));

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

  public static DataType getMeasureDataType(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2948
      MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo) {
    if (msrColumnEvaluatorInfo.getType() == DataTypes.BOOLEAN) {
      return DataTypes.BOOLEAN;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.SHORT) {
      return DataTypes.SHORT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.INT) {
      return DataTypes.INT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.LONG) {
      return DataTypes.LONG;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.FLOAT) {
      return DataTypes.FLOAT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.BYTE) {
      return DataTypes.BYTE;
    } else if (DataTypes.isDecimal(msrColumnEvaluatorInfo.getType())) {
      return DataTypes.createDefaultDecimalType();
    } else {
      return DataTypes.DOUBLE;
    }
  }

  public static boolean isExcludeFilterNeedsToApply(int dictionarySize,
      int size) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2589
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2590
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2602
    if ((size * 100) / dictionarySize >= 60) {
      LOGGER.info("Applying CBO to convert include filter to exclude filter.");
      return true;
    }
    return false;
  }

  private static byte[][] getFilterValuesInBytes(ColumnFilterInfo columnFilterInfo,
      boolean isExclude, int[] keys, List<byte[]> filterValuesList,
      int keyOrdinalOfDimensionFromCurrentBlock) {
    if (null != columnFilterInfo) {
      List<Integer> listOfsurrogates = null;
      if (!isExclude && columnFilterInfo.isIncludeFilter()) {
        listOfsurrogates = columnFilterInfo.getFilterList();
      } else if (isExclude || !columnFilterInfo.isIncludeFilter()) {
        listOfsurrogates = columnFilterInfo.getExcludeFilterList();
      }
      if (null != listOfsurrogates) {
        for (Integer surrogate : listOfsurrogates) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
          keys[keyOrdinalOfDimensionFromCurrentBlock] = surrogate;
          filterValuesList.add(ByteUtil.convertIntToBytes(surrogate));
        }
      }
    }
    return filterValuesList.toArray(new byte[filterValuesList.size()][]);
  }

  // This function is used for calculating filter values in case when Range Column
  // is given as a Dictionary Include Column
  private static byte[][] getFilterValueInBytesForDictRange(ColumnFilterInfo columnFilterInfo,
      int[] keys, List<byte[]> filterValuesList, int keyOrdinalOfDimensionFromCurrentBlock) {
    if (null != columnFilterInfo) {
      List<Integer> listOfsurrogates = columnFilterInfo.getFilterList();
      if (listOfsurrogates == null || listOfsurrogates.size() > 1) {
        throw new RuntimeException(
            "Filter values cannot be null in case of range in dictionary include");
      }
      // Here we only get the first column as there can be only one range column.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      keys[keyOrdinalOfDimensionFromCurrentBlock] = listOfsurrogates.get(0);
      filterValuesList.add(ByteUtil.convertIntToBytes(listOfsurrogates.get(0)));
    }
    return filterValuesList.toArray(new byte[filterValuesList.size()][]);
  }


  /**
   * This method will be used to get the Filter key array list for blocks which do not contain
   * filter column and the column Encoding is Direct Dictionary
   *
   * @param columnFilterInfo
   * @param isExclude
   * @return
   */
  public static byte[][] getKeyArray(ColumnFilterInfo columnFilterInfo, boolean isExclude) {
    int[] dimColumnsCardinality = new int[] { Integer.MAX_VALUE };
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(dimColumnsCardinality, new int[] { 1 });
    KeyGenerator blockLevelKeyGenerator = new MultiDimKeyVarLengthGenerator(dimensionBitLength);
    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    Arrays.fill(keys, 0);
    int keyOrdinalOfDimensionFromCurrentBlock = 0;
    List<byte[]> filterValuesList =
        new ArrayList<byte[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    return getFilterValuesInBytes(columnFilterInfo, isExclude, keys, filterValuesList,
        keyOrdinalOfDimensionFromCurrentBlock);
  }

  /**
   * Below method will be used to convert the filter surrogate keys
   * to mdkey
   *
   * @param columnFilterInfo
   * @param carbonDimension
   * @param segmentProperties
   * @param isDictRange
   * @return
   */
  public static byte[][] getKeyArray(ColumnFilterInfo columnFilterInfo,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3343
      CarbonDimension carbonDimension, SegmentProperties segmentProperties, boolean isExclude,
      boolean isDictRange) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
    if (carbonDimension.getDataType() != DataTypes.DATE) {
      return columnFilterInfo.getNoDictionaryFilterValuesList()
          .toArray((new byte[columnFilterInfo.getNoDictionaryFilterValuesList().size()][]));
    }
    int[] keys = new int[segmentProperties.getDimensions().size()];
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    Arrays.fill(keys, 0);
    int keyOrdinalOfDimensionFromCurrentBlock = carbonDimension.getKeyOrdinal();
    if (!isDictRange) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      return getFilterValuesInBytes(columnFilterInfo, isExclude, keys, filterValuesList,
          keyOrdinalOfDimensionFromCurrentBlock);
    } else {
      // For Dictionary Include Range Column
      return getFilterValueInBytesForDictRange(columnFilterInfo, keys, filterValuesList,
          keyOrdinalOfDimensionFromCurrentBlock);
    }
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
      Map<Integer, GenericQueryType> complexDimensionInfoMap, boolean isStreamDataFile) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    return getFilterExecuterTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap, null, isStreamDataFile);
  }

  /**
   * API will create an filter executer tree based on the filter resolver and minMaxColumns
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return
   */
  public static FilterExecuter getFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    return createFilterExecuterTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3486
      DataType filterColumnDataType = DataTypes.valueOf(measures.getDataType().getId());
      DataTypeConverterImpl converter = new DataTypeConverterImpl();
      Object[] keysBasedOnFilter = filterValues.getMeasuresFilterValuesList()
          .toArray((new Object[filterValues.getMeasuresFilterValuesList().size()]));
      for (int i = 0; i < keysBasedOnFilter.length; i++) {
        if (keysBasedOnFilter[i] != null) {
          keysBasedOnFilter[i] = DataTypeUtil
              .getDataBasedOnDataType(keysBasedOnFilter[i].toString(), filterColumnDataType,
                  converter);
        }
      }
      msrColumnExecuterInfo.setFilterKeys(keysBasedOnFilter, filterColumnDataType);
    } else {
      if (filterValues == null) {
        dimColumnExecuterInfo.setFilterKeys(new byte[0][]);
      } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3343
        byte[][] keysBasedOnFilter =
            getKeyArray(filterValues, dimension, segmentProperties, false, false);
        if (!filterValues.isIncludeFilter() || filterValues.isOptimized()) {
          dimColumnExecuterInfo.setExcludeFilterKeys(
              getKeyArray(filterValues, dimension, segmentProperties, true, false));
        }
        dimColumnExecuterInfo.setFilterKeys(keysBasedOnFilter);
      }
    }
  }

  public static int compareFilterKeyBasedOnDataType(String dictionaryVal, String memberVal,
      DataType dataType) {
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1444
      if (dataType == DataTypes.BOOLEAN) {
        return Boolean.compare((Boolean.parseBoolean(dictionaryVal)),
                (Boolean.parseBoolean(memberVal)));
      } else if (dataType == DataTypes.SHORT) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
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
   * Method will find whether the expression needs to be resolved, this can happen
   * if the expression is exclude and data type is null(mainly in IS NOT NULL filter scenario)
   *
   * @param rightExp
   * @param isIncludeFilter
   * @return
   */
  public static boolean isExpressionNeedsToResolved(Expression rightExp, boolean isIncludeFilter) {
    if (!isIncludeFilter && rightExp instanceof LiteralExpression && (
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3024
      LOGGER.error(CarbonCommonConstants.FILTER_INVALID_MEMBER + e.getMessage(), e);
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
    return (d1.doubleValue() == d2.doubleValue()) || (Double.isNaN(d1) && Double.isNaN(d2));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605

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
  public static void removeNullValues(DimensionColumnPage dimensionColumnPage, BitSet bitSet,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2947
      byte[] defaultValue) {
    if (!bitSet.isEmpty()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2975
      if (null != dimensionColumnPage.getNullBits()) {
        if (!dimensionColumnPage.getNullBits().isEmpty()) {
          for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            if (dimensionColumnPage.getNullBits().get(i)) {
              bitSet.flip(i);
            }
          }
        }
      } else {
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          if (dimensionColumnPage.compareTo(i, defaultValue) == 0) {
            bitSet.flip(i);
          }
        }
      }
    }
  }

  public static void updateIndexOfColumnExpression(Expression exp, int dimOridnalMax) {
    // if expression is null, not require to update index.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2151
    if (exp == null) {
      return;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1572
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
   * @param implicitColumnFilterList
   * @param isIncludeFilter
   * @return
   */
  public static ColumnFilterInfo getImplicitColumnFilterList(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3217
      Map<String, Set<Integer>> implicitColumnFilterList, boolean isIncludeFilter) {
    ColumnFilterInfo columnFilterInfo = new ColumnFilterInfo();
    columnFilterInfo.setIncludeFilter(isIncludeFilter);
    if (null != implicitColumnFilterList) {
      columnFilterInfo.setImplicitColumnFilterBlockToBlockletsMap(implicitColumnFilterList);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3217
    if (null != getImplicitFilterExpression(expression)) {
      setTrueExpressionAsRightChild(expression);
    }
  }

  /**
   * This method will check for ColumnExpression with column name positionID and if found will
   * replace the InExpression with true expression. This is done to stop serialization of List
   * expression which is right children of InExpression as it can impact the query performance
   * as the size of list grows bigger.
   *
   * @param expression
   */
  public static void setTrueExpressionAsRightChild(Expression expression) {
    setNewExpressionForRightChild(expression, new TrueExpression(null));
  }

  /**
   * Method to remove right child of the AND expression and set new expression for right child
   *
   * @param expression
   * @param rightChild
   */
  public static void setNewExpressionForRightChild(Expression expression, Expression rightChild) {
    // Remove the right expression node and point the expression to left node expression
    expression.findAndSetChild(((AndExpression) expression).getRight(), rightChild);
    LOGGER.info("In expression removed from the filter expression list to prevent it from"
        + " serializing on executor");
  }

  /**
   * This methdd will check if ImplictFilter is present or not
   * if it is present then return that ImplicitFilterExpression
   *
   * @param expression
   * @return
   */
  public static Expression getImplicitFilterExpression(Expression expression) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2134
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
            // if 1st children is implict column positionID then 2nd children will be
            // implicit filter list
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3217
            return children.get(1);
          }
        }
      }
    }
    return null;
  }

  /**
   * This method will create implicit expression and set as right child in the current expression
   *
   * @param expression
   * @param blockIdToBlockletIdMapping
   */
  public static void createImplicitExpressionAndSetAsRightChild(Expression expression,
      Map<String, Set<Integer>> blockIdToBlockletIdMapping) {
    ColumnExpression columnExpression =
        new ColumnExpression(CarbonCommonConstants.POSITION_ID, DataTypes.STRING);
    ImplicitExpression implicitExpression = new ImplicitExpression(blockIdToBlockletIdMapping);
    InExpression inExpression = new InExpression(columnExpression, implicitExpression);
    setNewExpressionForRightChild(expression, inExpression);
    LOGGER.info("Implicit expression added to the filter expression");
  }

  /**
   * Below method will be called from include and exclude filter to convert filter values
   * based on dictionary when local dictionary is present in blocklet.
   * @param dictionary
   * Dictionary
   * @param actualFilterValues
   * actual filter values
   * @return encoded filter values
   */
  public static byte[][] getEncodedFilterValues(CarbonDictionary dictionary,
      byte[][] actualFilterValues) {
    if (null == dictionary) {
      return actualFilterValues;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2589
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2590
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2602
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX });
    int[] dummy = new int[1];
    List<byte[]> encodedFilters = new ArrayList<>();
    for (byte[] actualFilter : actualFilterValues) {
      for (int i = 1; i < dictionary.getDictionarySize(); i++) {
        if (dictionary.getDictionaryValue(i) == null) {
          continue;
        }
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(actualFilter, dictionary.getDictionaryValue(i)) == 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
          dummy[0] = i;
          encodedFilters.add(keyGenerator.generateKey(dummy));
          break;
        }
      }
    }
    return getSortedEncodedFilters(encodedFilters);
  }

  /**
   * Below method will be used to sort the filter values a filter are applied using incremental
   * binary search
   * @param encodedFilters
   * encoded filter values
   * @return sorted encoded filter values
   */
  private static byte[][] getSortedEncodedFilters(List<byte[]> encodedFilters) {
    java.util.Comparator<byte[]> filterNoDictValueComaparator = new java.util.Comparator<byte[]>() {
      @Override
      public int compare(byte[] filterMember1, byte[] filterMember2) {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterMember1, filterMember2);
      }
    };
    Collections.sort(encodedFilters, filterNoDictValueComaparator);
    return encodedFilters.toArray(new byte[encodedFilters.size()][]);
  }

  /**
   * Below method will be used to get all the include filter values in case of range filters when
   * blocklet is encoded with local dictionary
   * @param expression
   * filter expression
   * @param dictionary
   * dictionary
   * @return include filter bitset
   * @throws FilterUnsupportedException
   */
  private static BitSet getIncludeDictFilterValuesForRange(Expression expression,
      CarbonDictionary dictionary) throws FilterUnsupportedException {
    ConditionalExpression conExp = (ConditionalExpression) expression;
    ColumnExpression columnExpression = conExp.getColumnList().get(0);
    BitSet includeFilterBitSet = new BitSet();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2589
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2590
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2602
    for (int i = 2; i < dictionary.getDictionarySize(); i++) {
      if (null == dictionary.getDictionaryValue(i)) {
        continue;
      }
      try {
        RowIntf row = new RowImpl();
        String stringValue = new String(dictionary.getDictionaryValue(i),
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        row.setValues(new Object[] { DataTypeUtil.getDataBasedOnDataType(stringValue,
            columnExpression.getCarbonColumn().getDataType()) });
        Boolean rslt = expression.evaluate(row).getBoolean();
        if (null != rslt) {
          if (rslt) {
            includeFilterBitSet.set(i);
          }
        }
      } catch (FilterIllegalMemberException e) {
        LOGGER.debug(e.getMessage());
      }
    }
    return includeFilterBitSet;
  }

  /**
   * Below method will used to get encoded filter values for range filter values
   * when local dictionary is present in blocklet for columns
   * If number of include filter is more than 60% of total dictionary size it will
   * convert include to exclude
   * @param includeDictValues
   * include filter values
   * @param carbonDictionary
   * dictionary
   * @param useExclude
   * to check if using exclude will be more optimized
   * @return encoded filter values
   */
  private static byte[][] getEncodedFilterValuesForRange(BitSet includeDictValues,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2589
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2590
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2602
      CarbonDictionary carbonDictionary, boolean useExclude) {
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX });
    List<byte[]> encodedFilterValues = new ArrayList<>();
    int[] dummy = new int[1];
    if (!useExclude) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      for (int i = includeDictValues.nextSetBit(0);
           i >= 0; i = includeDictValues.nextSetBit(i + 1)) {
        dummy[0] = i;
        encodedFilterValues.add(keyGenerator.generateKey(dummy));
      }
      return encodedFilterValues.toArray(new byte[encodedFilterValues.size()][]);
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2589
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2590
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2602
      for (int i = 1; i < carbonDictionary.getDictionarySize(); i++) {
        if (!includeDictValues.get(i) && null != carbonDictionary.getDictionaryValue(i)) {
          dummy[0] = i;
          encodedFilterValues.add(keyGenerator.generateKey(dummy));
        }
      }
    }
    return getSortedEncodedFilters(encodedFilterValues);
  }

  /**
   * Below method will be used to get filter executor instance for range filters
   * when local dictonary is present for in blocklet
   * @param rawColumnChunk
   * raw column chunk
   * @param exp
   * filter expression
   * @param isNaturalSorted
   * is data was already sorted
   * @return
   */
  public static FilterExecuter getFilterExecutorForRangeFilters(
      DimensionRawColumnChunk rawColumnChunk, Expression exp, boolean isNaturalSorted) {
    BitSet includeDictionaryValues;
    try {
      includeDictionaryValues =
          FilterUtil.getIncludeDictFilterValuesForRange(exp, rawColumnChunk.getLocalDictionary());
    } catch (FilterUnsupportedException e) {
      throw new RuntimeException(e);
    }
    boolean isExclude = includeDictionaryValues.cardinality() > 1 && FilterUtil
        .isExcludeFilterNeedsToApply(rawColumnChunk.getLocalDictionary().getDictionaryActualSize(),
            includeDictionaryValues.cardinality());
    byte[][] encodedFilterValues = FilterUtil
        .getEncodedFilterValuesForRange(includeDictionaryValues,
            rawColumnChunk.getLocalDictionary(), isExclude);
    FilterExecuter filterExecuter;
    if (!isExclude) {
      filterExecuter = new IncludeFilterExecuterImpl(encodedFilterValues, isNaturalSorted);
    } else {
      filterExecuter = new ExcludeFilterExecuterImpl(encodedFilterValues, isNaturalSorted);
    }
    return filterExecuter;
  }

  /**
   * This method is used to compare the filter value with min and max values.
   * This is used in case of filter queries on no dictionary column.
   *
   * @param filterValue
   * @param minMaxBytes
   * @param carbonDimension
   * @param isMin
   * @return
   */
  public static int compareValues(byte[] filterValue, byte[] minMaxBytes,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
      CarbonDimension carbonDimension, boolean isMin) {
    DataType dataType = carbonDimension.getDataType();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
    if (DataTypeUtil.isPrimitiveColumn(dataType) &&
        dataType != DataTypes.DATE) {
      Object value =
          DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(minMaxBytes, dataType);
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      Object data = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(filterValue, dataType);
      SerializableComparator comparator = Comparator.getComparator(dataType);
      if (isMin) {
        return comparator.compare(value, data);
      } else {
        return comparator.compare(data, value);
      }
    } else {
      if (isMin) {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(minMaxBytes, filterValue);
      } else {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValue, minMaxBytes);
      }
    }
  }

  /**
   * This method is used to get default null values for a direct dictionary column
   * @param currentBlockDimension
   * @return
   */
  public static byte[] getDefaultNullValue(CarbonDimension currentBlockDimension) {
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(currentBlockDimension.getDataType());
    int key = directDictionaryGenerator.generateDirectSurrogateKey(null);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    return ByteUtil.convertIntToBytes(key);
  }

  public static Expression prepareEqualToExpression(String columnName, String dataType,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3461
      Object value) {
    if (DataTypes.STRING.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.STRING),
          new LiteralExpression(value, DataTypes.STRING));
    } else if (DataTypes.INT.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.INT),
          new LiteralExpression(value, DataTypes.INT));
    } else if (DataTypes.DOUBLE.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.DOUBLE),
          new LiteralExpression(value, DataTypes.DOUBLE));
    } else if (DataTypes.FLOAT.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.FLOAT),
          new LiteralExpression(value, DataTypes.FLOAT));
    } else if (DataTypes.SHORT.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.SHORT),
          new LiteralExpression(value, DataTypes.SHORT));
    } else if (DataTypes.BINARY.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.BINARY),
          new LiteralExpression(value, DataTypes.BINARY));
    } else if (DataTypes.DATE.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.DATE),
          new LiteralExpression(value, DataTypes.DATE));
    } else if (DataTypes.LONG.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.LONG),
          new LiteralExpression(value, DataTypes.LONG));
    } else if (DataTypes.TIMESTAMP.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.TIMESTAMP),
          new LiteralExpression(value, DataTypes.TIMESTAMP));
    } else if (DataTypes.BYTE.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.BYTE),
          new LiteralExpression(value, DataTypes.BYTE));
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  public static Expression prepareOrExpression(List<Expression> expressions) {
    if (expressions.size() < 2) {
      throw new RuntimeException("Please input at least two expressions");
    }
    Expression expression = expressions.get(0);
    for (int i = 1; i < expressions.size(); i++) {
      expression = new OrExpression(expression, expressions.get(i));
    }
    return expression;
  }

  private static Expression prepareEqualToExpressionSet(String columnName, DataType dataType,
      List<Object> values) {
    Expression expression = null;
    if (0 == values.size()) {
      expression = prepareEqualToExpression(columnName, dataType.getName(), null);
    } else {
      expression = prepareEqualToExpression(columnName, dataType.getName(), values.get(0));
    }
    for (int i = 1; i < values.size(); i++) {
      Expression expression2 = prepareEqualToExpression(columnName,
          dataType.getName(), values.get(i));
      expression = new OrExpression(expression, expression2);
    }
    return expression;
  }

  public static Expression prepareEqualToExpressionSet(String columnName, String dataType,
      List<Object> values) {
    if (DataTypes.STRING.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.STRING, values);
    } else if (DataTypes.INT.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.INT, values);
    } else if (DataTypes.DOUBLE.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.DOUBLE, values);
    } else if (DataTypes.FLOAT.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.FLOAT, values);
    } else if (DataTypes.SHORT.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.SHORT, values);
    } else if (DataTypes.BINARY.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.BINARY, values);
    } else if (DataTypes.DATE.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.DATE, values);
    } else if (DataTypes.LONG.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.LONG, values);
    } else if (DataTypes.TIMESTAMP.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.TIMESTAMP, values);
    } else if (DataTypes.BYTE.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.BYTE, values);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1326
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

}
