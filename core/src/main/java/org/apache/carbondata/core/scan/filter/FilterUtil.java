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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
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
import org.apache.carbondata.core.scan.expression.UnknownExpression;
import org.apache.carbondata.core.scan.expression.conditional.*;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.executer.AndFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.CDCBlockImplicitExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.DimColumnExecutorFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.ExcludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.FalseFilterExecutor;
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor;
import org.apache.carbondata.core.scan.filter.executer.ImplicitIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.IncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.MeasureColumnExecutorFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.OrFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RangeValueFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureExcludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelRangeTypeExecutorFactory;
import org.apache.carbondata.core.scan.filter.executer.TrueFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.FilterExecutorType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
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
   * Method will create filter executor tree based on the filter resolved tree,
   * in this algorithm based on the resolver instance the executors will be visited
   * and the resolved surrogates will be converted to keys
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @param complexDimensionInfoMap
   * @param minMaxCacheColumns
   * @param isStreamDataFile: whether create filter executor tree for stream data files
   * @return FilterExecutor instance
   *
   */
  private static FilterExecutor createFilterExecutorTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap,
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    FilterExecutorType filterExecutorType = filterExpressionResolverTree.getFilterExecutorType();
    if (null != filterExecutorType) {
      switch (filterExecutorType) {
        case INCLUDE:
          if (null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              && null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues() && filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues().isOptimized()) {
            return getExcludeFilterExecutor(
                filterExpressionResolverTree.getDimColResolvedFilterInfo(),
                filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
          }
          // return true filter expression if filter column min/max is not cached in driver
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(filterExpressionResolverTree,
              segmentProperties, minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return getIncludeFilterExecutor(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
        case EXCLUDE:
          return getExcludeFilterExecutor(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
        case OR:
          return new OrFilterExecutorImpl(
              createFilterExecutorTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile),
              createFilterExecutorTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile));
        case AND:
          return new AndFilterExecutorImpl(
              createFilterExecutorTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile),
              createFilterExecutorTree(filterExpressionResolverTree.getRight(), segmentProperties,
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
              rowLevelRangeFilterResolver.getMsrColEvaluatorInfoList(), segmentProperties,
              minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return RowLevelRangeTypeExecutorFactory
              .getRowLevelRangeTypeExecutor(filterExecutorType, filterExpressionResolverTree,
                  segmentProperties);
        case RANGE:
          // return true filter expression if filter column min/max is not cached in driver
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(filterExpressionResolverTree,
              segmentProperties, minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return new RangeValueFilterExecutorImpl(
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
          if (filterExpressionResolverTree.getFilterExpression() instanceof UnknownExpression) {
            FilterExecutor filterExecutor =
                ((UnknownExpression) filterExpressionResolverTree.getFilterExpression())
                    .getFilterExecutor(filterExpressionResolverTree, segmentProperties);
            if (filterExecutor != null) {
              return filterExecutor;
            }
          }
          if (filterExpressionResolverTree
              .getFilterExpression() instanceof CDCBlockImplicitExpression) {
            return new CDCBlockImplicitExecutorImpl(
                ((CDCBlockImplicitExpression) filterExpressionResolverTree.getFilterExpression())
                    .getBlocksToScan());
          }
          return new RowLevelFilterExecutorImpl(
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getDimColEvaluatorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getMsrColEvalutorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
              segmentProperties, complexDimensionInfoMap,
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getLimit());

      }
    }
    return new RowLevelFilterExecutorImpl(
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getDimColEvaluatorInfoList(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getMsrColEvalutorInfoList(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
        segmentProperties, complexDimensionInfoMap,
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getLimit());

  }

  /**
   * It gives filter executor based on columnar or column group
   *
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return
   */
  private static FilterExecutor getIncludeFilterExecutor(
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
        return new IncludeFilterExecutorImpl(null, msrColResolvedFilterInfoCopyObject,
            segmentProperties, true);
      } else {
        return new RestructureIncludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, true);
      }
    }
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
        return new IncludeFilterExecutorImpl(dimColResolvedFilterInfoCopyObject, null,
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
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvaluatorInfoList,
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
   * It gives filter executor based on columnar or column group
   *
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return
   */
  private static FilterExecutor getExcludeFilterExecutor(
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
        return new ExcludeFilterExecutorImpl(null, msrColResolvedFilterInfoCopyObject,
            segmentProperties, true);
      } else {
        return new RestructureExcludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, true);
      }
    }
    CarbonDimension dimensionFromCurrentBlock =
        segmentProperties.getDimensionFromCurrentBlock(dimColResolvedFilterInfo.getDimension());
    if (null != dimensionFromCurrentBlock) {
      // update dimension and column index according to the dimension position in current block
      DimColumnResolvedFilterInfo dimColResolvedFilterInfoCopyObject =
          dimColResolvedFilterInfo.getCopyObject();
      dimColResolvedFilterInfoCopyObject.setDimension(dimensionFromCurrentBlock);
      dimColResolvedFilterInfoCopyObject.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
      return new ExcludeFilterExecutorImpl(dimColResolvedFilterInfoCopyObject, null,
          segmentProperties, false);
    } else {
      return new RestructureExcludeFilterExecutorImpl(dimColResolvedFilterInfo,
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

    // TODO Auto-generated method stub
    java.util.Comparator<byte[]> filterNoDictValueComparator =
        ByteUtil.UnsafeComparer.INSTANCE::compareTo;
    filterValuesList.sort(filterNoDictValueComparator);
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

        filterValuesList.add(DataTypeUtil
            .getMeasureValueBasedOnDataType(result, dataType, carbonMeasure.getScale(),
                carbonMeasure.getPrecision()));

      }
    } catch (Throwable ex) {
      throw new FilterUnsupportedException("Unsupported Filter condition: " + result, ex);
    }

    SerializableComparator filterMeasureComparator =
        Comparator.getComparatorByDataTypeForMeasure(dataType);
    filterValuesList.sort(filterMeasureComparator);
    ColumnFilterInfo columnFilterInfo = null;
    if (filterValuesList.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setMeasuresFilterValuesList(filterValuesList);

    }
    return columnFilterInfo;
  }

  public static DataType getMeasureDataType(
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
      List<Integer> listOfSurrogates = null;
      if (!isExclude && columnFilterInfo.isIncludeFilter()) {
        listOfSurrogates = columnFilterInfo.getFilterList();
      } else if (isExclude || !columnFilterInfo.isIncludeFilter()) {
        listOfSurrogates = columnFilterInfo.getExcludeFilterList();
      }
      if (null != listOfSurrogates) {
        for (Integer surrogate : listOfSurrogates) {
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
      List<Integer> listOfSurrogates = columnFilterInfo.getFilterList();
      if (listOfSurrogates == null || listOfSurrogates.size() > 1) {
        throw new RuntimeException(
            "Filter values cannot be null in case of range in dictionary include");
      }
      // Here we only get the first column as there can be only one range column.
      keys[keyOrdinalOfDimensionFromCurrentBlock] = listOfSurrogates.get(0);
      filterValuesList.add(ByteUtil.convertIntToBytes(listOfSurrogates.get(0)));
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
   * to MDKey
   *
   * @param columnFilterInfo
   * @param carbonDimension
   * @param segmentProperties
   * @param isDictRange
   * @return
   */
  public static byte[][] getKeyArray(ColumnFilterInfo columnFilterInfo,
      CarbonDimension carbonDimension, SegmentProperties segmentProperties, boolean isExclude,
      boolean isDictRange) {
    if (carbonDimension.getDataType() != DataTypes.DATE) {
      return columnFilterInfo.getNoDictionaryFilterValuesList()
          .toArray((new byte[columnFilterInfo.getNoDictionaryFilterValuesList().size()][]));
    }
    int[] keys = new int[segmentProperties.getDimensions().size()];
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    Arrays.fill(keys, 0);
    int keyOrdinalOfDimensionFromCurrentBlock = carbonDimension.getKeyOrdinal();
    if (!isDictRange) {
      return getFilterValuesInBytes(columnFilterInfo, isExclude, keys, filterValuesList,
          keyOrdinalOfDimensionFromCurrentBlock);
    } else {
      // For Dictionary Include Range Column
      return getFilterValueInBytesForDictRange(columnFilterInfo, keys, filterValuesList,
          keyOrdinalOfDimensionFromCurrentBlock);
    }
  }

  /**
   * API will create an filter executor tree based on the filter resolver
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return
   */
  public static FilterExecutor getFilterExecutorTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap, boolean isStreamDataFile) {
    return getFilterExecutorTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap, null, isStreamDataFile);
  }

  /**
   * API will create an filter executor tree based on the filter resolver and minMaxColumns
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return
   */
  public static FilterExecutor getFilterExecutorTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap,
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    return createFilterExecutorTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile);
  }

  /**
   * API will prepare the Keys from the surrogates of particular filter resolver
   *
   * @param filterValues
   * @param segmentProperties
   * @param dimension
   * @param dimColumnExecutorInfo
   */
  public static void prepareKeysFromSurrogates(ColumnFilterInfo filterValues,
      SegmentProperties segmentProperties, CarbonDimension dimension,
      DimColumnExecutorFilterInfo dimColumnExecutorInfo, CarbonMeasure measures,
      MeasureColumnExecutorFilterInfo msrColumnExecutorInfo) {
    if (null != measures) {
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
      msrColumnExecutorInfo.setFilterKeys(keysBasedOnFilter, filterColumnDataType);
    } else {
      if (filterValues == null) {
        dimColumnExecutorInfo.setFilterKeys(new byte[0][]);
      } else {
        byte[][] keysBasedOnFilter =
            getKeyArray(filterValues, dimension, segmentProperties, false, false);
        if (!filterValues.isIncludeFilter() || filterValues.isOptimized()) {
          dimColumnExecutorInfo.setExcludeFilterKeys(
              getKeyArray(filterValues, dimension, segmentProperties, true, false));
        }
        dimColumnExecutorInfo.setFilterKeys(keysBasedOnFilter);
      }
    }
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

  }

  /**
   * This method will create bit set group for particular raw blocklet column chunk.
   * Applicable for restructure scenarios.
   *
   * @param rawBlockletColumnChunks
   * @param defaultValue
   * @return
   */
  public static BitSetGroup createBitSetGroupWithColumnChunk(RawBlockletColumnChunks
      rawBlockletColumnChunks, boolean defaultValue) {
    int pageCount = rawBlockletColumnChunks.getDataBlock().numberOfPages();
    BitSetGroup bitSetGroup = new BitSetGroup(pageCount);
    for (int i = 0; i < pageCount; i++) {
      int pageRowCount = rawBlockletColumnChunks.getDataBlock().getPageRowCount(i);
      BitSet bitSet = new BitSet(pageRowCount);
      bitSet.set(0, pageRowCount, defaultValue);
      bitSetGroup.setBitSet(bitSet, i);
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
      byte[] defaultValue) {
    if (!bitSet.isEmpty()) {
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

  public static void updateIndexOfColumnExpression(Expression exp, int dimOrdinalMax) {
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
          ce.setColIndex(dimOrdinalMax + column.getOrdinal());
        }
      }
    } else {
      if (exp.getChildren().size() > 0) {
        List<Expression> children = exp.getChildren();
        for (int i = 0; i < children.size(); i++) {
          updateIndexOfColumnExpression(children.get(i), dimOrdinalMax);
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
   * This method will check if ImplicitFilter is present or not
   * if it is present then return that ImplicitFilterExpression
   *
   * @param expression
   * @return
   */
  public static Expression getImplicitFilterExpression(Expression expression) {
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
            // if 1st children is implicit column positionID then 2nd children will be
            // implicit filter list
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
    java.util.Comparator<byte[]> filterNoDictValueComparator =
        ByteUtil.UnsafeComparer.INSTANCE::compareTo;
    encodedFilters.sort(filterNoDictValueComparator);
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
        Boolean result = expression.evaluate(row).getBoolean();
        if (null != result) {
          if (result) {
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
      CarbonDictionary carbonDictionary, boolean useExclude) {
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX });
    List<byte[]> encodedFilterValues = new ArrayList<>();
    int[] dummy = new int[1];
    if (!useExclude) {
      for (int i = includeDictValues.nextSetBit(0);
           i >= 0; i = includeDictValues.nextSetBit(i + 1)) {
        dummy[0] = i;
        encodedFilterValues.add(keyGenerator.generateKey(dummy));
      }
      return encodedFilterValues.toArray(new byte[encodedFilterValues.size()][]);
    } else {
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
   * when local dictionary is present for in blocklet
   * @param rawColumnChunk
   * raw column chunk
   * @param exp
   * filter expression
   * @param isNaturalSorted
   * is data was already sorted
   * @return
   */
  public static FilterExecutor getFilterExecutorForRangeFilters(
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
    FilterExecutor filterExecutor;
    if (!isExclude) {
      filterExecutor = new IncludeFilterExecutorImpl(encodedFilterValues, isNaturalSorted);
    } else {
      filterExecutor = new ExcludeFilterExecutorImpl(encodedFilterValues, isNaturalSorted);
    }
    return filterExecutor;
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
      CarbonDimension carbonDimension, boolean isMin) {
    DataType dataType = carbonDimension.getDataType();
    if (DataTypeUtil.isPrimitiveColumn(dataType) &&
        dataType != DataTypes.DATE) {
      Object value =
          DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(minMaxBytes, dataType);
      // filter value should be in range of max and min value i.e
      // max>filterValue>min
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
    return ByteUtil.convertIntToBytes(key);
  }

  public static Expression prepareEqualToExpression(String columnName, String dataType,
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
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

}
