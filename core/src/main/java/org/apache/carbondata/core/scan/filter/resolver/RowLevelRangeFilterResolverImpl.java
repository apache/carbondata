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

package org.apache.carbondata.core.scan.filter.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.BinaryLogicalExpression;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;

public class RowLevelRangeFilterResolverImpl extends ConditionalFilterResolverImpl {

  /**
   *
   */
  private static final long serialVersionUID = 6629319265336666789L;
  private List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList;
  private List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList;
  private AbsoluteTableIdentifier tableIdentifier;

  public RowLevelRangeFilterResolverImpl(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter, AbsoluteTableIdentifier tableIdentifier) {
    super(exp, isExpressionResolve, isIncludeFilter, false);
    dimColEvaluatorInfoList =
        new ArrayList<DimColumnResolvedFilterInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    msrColEvalutorInfoList = new ArrayList<MeasureColumnResolvedFilterInfo>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.tableIdentifier = tableIdentifier;
  }

  /**
   * This method will return the filter values which is present in the range level
   * conditional expressions.
   *
   * @return
   */
  public byte[][] getFilterRangeValues(SegmentProperties segmentProperties) {

    if (dimColEvaluatorInfoList.size() > 0 && null != dimColEvaluatorInfoList.get(0)
        .getFilterValues() && !dimColEvaluatorInfoList.get(0).getDimension()
        .hasEncoding(Encoding.DICTIONARY)) {
      List<byte[]> noDictFilterValuesList =
          dimColEvaluatorInfoList.get(0).getFilterValues().getNoDictionaryFilterValuesList();
      return noDictFilterValuesList.toArray((new byte[noDictFilterValuesList.size()][]));
    } else if (dimColEvaluatorInfoList.size() > 0 && null != dimColEvaluatorInfoList.get(0)
        .getFilterValues() && dimColEvaluatorInfoList.get(0).getDimension()
        .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      CarbonDimension dimensionFromCurrentBlock = segmentProperties
          .getDimensionFromCurrentBlock(this.dimColEvaluatorInfoList.get(0).getDimension());
      if (null != dimensionFromCurrentBlock) {
        return FilterUtil.getKeyArray(this.dimColEvaluatorInfoList.get(0).getFilterValues(),
            dimensionFromCurrentBlock, segmentProperties, false);
      }
    }
    return null;
  }

  public Object[] getMeasureFilterRangeValues() {
    if (msrColEvalutorInfoList.size() > 0 && null != msrColEvalutorInfoList.get(0)
        .getFilterValues()) {
      List<Object> measureFilterValuesList =
          msrColEvalutorInfoList.get(0).getFilterValues().getMeasuresFilterValuesList();
      return measureFilterValuesList.toArray((new Object[measureFilterValuesList.size()]));
    }
    return null;
  }


  /**
   * method will get the start key based on the filter surrogates
   *
   * @return start IndexKey
   */
  public void getStartKey(SegmentProperties segmentProperties, long[] startKey,
      SortedMap<Integer, byte[]> noDictStartKeys, List<long[]> startKeyList) {
    switch (exp.getFilterExpressionType()) {
      case GREATERTHAN:
      case GREATERTHAN_EQUALTO:
        if (dimColEvaluatorInfoList.size() > 0) {
          FilterUtil
              .getStartKey(dimColEvaluatorInfoList.get(0).getDimensionResolvedFilterInstance(),
                  segmentProperties, startKey, startKeyList);
          FilterUtil.getStartKeyForNoDictionaryDimension(dimColEvaluatorInfoList.get(0),
              segmentProperties, noDictStartKeys);
        }
        break;
      default:
        //do nothing
    }
  }

  /**
   * method will get the start key based on the filter surrogates
   *
   * @return end IndexKey
   */
  @Override public void getEndKey(SegmentProperties segmentProperties, long[] endKeys,
      SortedMap<Integer, byte[]> noDicEndKeys, List<long[]> endKeyList) {
    switch (exp.getFilterExpressionType()) {
      case LESSTHAN:
      case LESSTHAN_EQUALTO:
        if (dimColEvaluatorInfoList.size() > 0) {
          FilterUtil.getEndKey(dimColEvaluatorInfoList.get(0).getDimensionResolvedFilterInstance(),
              endKeys, segmentProperties, endKeyList);
          FilterUtil
              .getEndKeyForNoDictionaryDimension(dimColEvaluatorInfoList.get(0), segmentProperties,
                  noDicEndKeys);
        }
        break;
      default:
        //do nothing
    }
  }

  private List<byte[]> getNoDictionaryRangeValues() {
    List<ExpressionResult> listOfExpressionResults = new ArrayList<ExpressionResult>(20);
    if (this.getFilterExpression() instanceof BinaryConditionalExpression) {
      listOfExpressionResults =
          ((BinaryConditionalExpression) this.getFilterExpression()).getLiterals();
    }
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    boolean invalidRowsPresent = false;
    String timeFormat = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    for (ExpressionResult result : listOfExpressionResults) {
      try {
        if (result.getString() == null) {
          if (result.getDataType() == DataTypes.STRING) {
            filterValuesList.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
          } else {
            filterValuesList.add(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
          }
          continue;
        }
        filterValuesList.add(DataTypeUtil
            .getBytesBasedOnDataTypeForNoDictionaryColumn(result.getString(), result.getDataType(),
                timeFormat));
      } catch (FilterIllegalMemberException e) {
        // Any invalid member while evaluation shall be ignored, system will log the
        // error only once since all rows the evaluation happens so inorder to avoid
        // too much log inforation only once the log will be printed.
        FilterUtil.logError(e, invalidRowsPresent);
      }
    }
    Comparator<byte[]> filterNoDictValueComaparator = new Comparator<byte[]>() {
      @Override public int compare(byte[] filterMember1, byte[] filterMember2) {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterMember1, filterMember2);
      }

    };
    Collections.sort(filterValuesList, filterNoDictValueComaparator);
    return filterValuesList;
  }

  private List<Object> getMeasureRangeValues(CarbonMeasure carbonMeasure) {
    List<ExpressionResult> listOfExpressionResults = new ArrayList<ExpressionResult>(20);
    if (this.getFilterExpression() instanceof BinaryConditionalExpression) {
      listOfExpressionResults =
          ((BinaryConditionalExpression) this.getFilterExpression()).getLiterals();
    }
    List<Object> filterValuesList = new ArrayList<>(20);
    boolean invalidRowsPresent = false;
    for (ExpressionResult result : listOfExpressionResults) {
      try {
        if (result.getString() == null) {
          filterValuesList.add(null);
          continue;
        }
        filterValuesList.add(DataTypeUtil.getMeasureValueBasedOnDataType(result.getString(),
            result.getDataType(), carbonMeasure));
      } catch (FilterIllegalMemberException e) {
        // Any invalid member while evaluation shall be ignored, system will log the
        // error only once since all rows the evaluation happens so inorder to avoid
        // too much log inforation only once the log will be printed.
        FilterUtil.logError(e, invalidRowsPresent);
      }
    }
    Collections.sort(filterValuesList, org.apache.carbondata.core.util.comparator.Comparator
        .getComparatorByDataTypeForMeasure(carbonMeasure.getDataType()));
    return filterValuesList;
  }

  /**
   * Method which will resolve the filter expression by converting the filter
   * member to its assigned dictionary values.
   */
  public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider)
      throws FilterUnsupportedException {
    DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = null;
    MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo = null;
    int index = 0;
    if (exp instanceof BinaryLogicalExpression) {
      BinaryLogicalExpression conditionalExpression = (BinaryLogicalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      for (ColumnExpression columnExpression : columnList) {
        if (columnExpression.isDimension()) {
          dimColumnEvaluatorInfo = new DimColumnResolvedFilterInfo();
          ColumnFilterInfo filterInfo = new ColumnFilterInfo();
          dimColumnEvaluatorInfo.setColumnIndex(columnExpression.getCarbonColumn().getOrdinal());
          dimColumnEvaluatorInfo.setRowIndex(index++);
          dimColumnEvaluatorInfo.setDimension(columnExpression.getDimension());
          dimColumnEvaluatorInfo.setDimensionExistsInCurrentSilce(false);
          if (columnExpression.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
            if (!isIncludeFilter) {
              filterInfo.setExcludeFilterList(getDirectSurrogateValues(columnExpression));
            } else {
              filterInfo.setFilterList(getDirectSurrogateValues(columnExpression));
            }
          } else {
            filterInfo.setFilterListForNoDictionaryCols(getNoDictionaryRangeValues());
          }
          filterInfo.setIncludeFilter(isIncludeFilter);
          dimColumnEvaluatorInfo.setFilterValues(filterInfo);
          dimColumnEvaluatorInfo
              .addDimensionResolvedFilterInstance(columnExpression.getDimension(), filterInfo);
          dimColEvaluatorInfoList.add(dimColumnEvaluatorInfo);
        } else {
          msrColumnEvalutorInfo = new MeasureColumnResolvedFilterInfo();
          ColumnFilterInfo filterInfo = new ColumnFilterInfo();
          msrColumnEvalutorInfo.setMeasure(columnExpression.getMeasure());
          msrColumnEvalutorInfo.setRowIndex(index++);
          msrColumnEvalutorInfo.setCarbonColumn(columnExpression.getCarbonColumn());
          msrColumnEvalutorInfo.setColumnIndex(columnExpression.getCarbonColumn().getOrdinal());
          msrColumnEvalutorInfo.setType(columnExpression.getCarbonColumn().getDataType());
          msrColumnEvalutorInfo.setMeasureExistsInCurrentSilce(false);
          filterInfo
              .setMeasuresFilterValuesList(getMeasureRangeValues(columnExpression.getMeasure()));
          filterInfo.setIncludeFilter(isIncludeFilter);
          msrColumnEvalutorInfo.setFilterValues(filterInfo);
          msrColumnEvalutorInfo
              .addMeasureResolvedFilterInstance(columnExpression.getMeasure(), filterInfo);
          msrColEvalutorInfoList.add(msrColumnEvalutorInfo);
        }
      }
    }
  }

  private List<Integer> getDirectSurrogateValues(ColumnExpression columnExpression)
      throws FilterUnsupportedException {
    List<ExpressionResult> listOfExpressionResults = new ArrayList<ExpressionResult>(20);
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(columnExpression.getDimension().getDataType());

    if (this.getFilterExpression() instanceof BinaryConditionalExpression) {
      listOfExpressionResults =
          ((BinaryConditionalExpression) this.getFilterExpression()).getLiterals();
    }
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    try {
      // if any filter member provided by user is invalid throw error else
      // system can display inconsistent result.
      for (ExpressionResult result : listOfExpressionResults) {
        filterValuesList.add(directDictionaryGenerator
            .generateDirectSurrogateKey(result.getString(),
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
      }
    } catch (FilterIllegalMemberException e) {
      throw new FilterUnsupportedException(e);
    }
    return filterValuesList;
  }

  /**
   * Method will return the DimColumnResolvedFilterInfo instance which consists
   * the mapping of the respective dimension and its surrogates involved in
   * filter expression.
   *
   * @return DimColumnResolvedFilterInfo
   */
  public List<DimColumnResolvedFilterInfo> getDimColEvaluatorInfoList() {
    return dimColEvaluatorInfoList;
  }

  /**
   * Method will return the DimColumnResolvedFilterInfo instance which containts
   * measure level details.
   *
   * @return MeasureColumnResolvedFilterInfo
   */
  public List<MeasureColumnResolvedFilterInfo> getMsrColEvalutorInfoList() {
    return msrColEvalutorInfoList;
  }

  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public Expression getFilterExpression() {
    return this.exp;
  }

  /**
   * This method will provide the executer type to the callee inorder to identify
   * the executer type for the filter resolution, Row level filter executer is a
   * special executer since it get all the rows of the specified filter dimension
   * and will be send to the spark for processing
   */
  public FilterExecuterType getFilterExecuterType() {
    switch (exp.getFilterExpressionType()) {
      case GREATERTHAN:
        return FilterExecuterType.ROWLEVEL_GREATERTHAN;
      case GREATERTHAN_EQUALTO:
        return FilterExecuterType.ROWLEVEL_GREATERTHAN_EQUALTO;
      case LESSTHAN:
        return FilterExecuterType.ROWLEVEL_LESSTHAN;
      case LESSTHAN_EQUALTO:
        return FilterExecuterType.ROWLEVEL_LESSTHAN_EQUALTO;

      default:
        return FilterExecuterType.ROWLEVEL;
    }
  }
}
