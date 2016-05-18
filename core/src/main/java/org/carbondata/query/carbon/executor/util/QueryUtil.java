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
package org.carbondata.query.carbon.executor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.aggregator.dimension.impl.ColumnGroupDimensionsAggregator;
import org.carbondata.query.carbon.aggregator.dimension.impl.FixedLengthDimensionAggregator;
import org.carbondata.query.carbon.aggregator.dimension.impl.VariableLengthDimensionAggregator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.model.CustomAggregateExpression;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Utility class for query execution
 */
public class QueryUtil {

  /**
   * Below method will be used to get the masked byte range based on the query
   * dimension. It will give the range in the mdkey. This will be used to get
   * the actual key array from masked mdkey
   *
   * @param queryDimensions query dimension selected in query
   * @param keyGenerator    key generator
   * @return masked key
   */
  public static int[] getMaskedByteRange(List<QueryDimension> queryDimensions,
      KeyGenerator keyGenerator) {
    Set<Integer> byteRangeSet = new TreeSet<Integer>();
    int[] byteRange = null;
    for (int i = 0; i < queryDimensions.size(); i++) {

      // as no dictionary column and complex type columns
      // are not selected in the mdkey
      // so we will not select the those dimension for calculating the
      // range
      if (queryDimensions.get(i).getDimension().getKeyOrdinal() == -1) {
        continue;
      }
      // get the offset of the dimension in the mdkey
      byteRange =
          keyGenerator.getKeyByteOffsets(queryDimensions.get(i).getDimension().getKeyOrdinal());
      for (int j = byteRange[0]; j <= byteRange[1]; j++) {
        byteRangeSet.add(j);
      }
    }
    int[] maksedByteRange = new int[byteRangeSet.size()];
    int index = 0;
    Iterator<Integer> iterator = byteRangeSet.iterator();
    // add the masked byte range
    while (iterator.hasNext()) {
      maksedByteRange[index++] = iterator.next();
    }
    return maksedByteRange;
  }

  public static int[] getMaskedByteRangeBasedOrdinal(List<Integer> ordinals,
      KeyGenerator keyGenerator) {
    Set<Integer> byteRangeSet = new TreeSet<Integer>();
    int[] byteRange = null;
    for (int i = 0; i < ordinals.size(); i++) {

      // get the offset of the dimension in the mdkey
      byteRange = keyGenerator.getKeyByteOffsets(ordinals.get(i));
      for (int j = byteRange[0]; j <= byteRange[1]; j++) {
        byteRangeSet.add(j);
      }
    }
    int[] maksedByteRange = new int[byteRangeSet.size()];
    int index = 0;
    Iterator<Integer> iterator = byteRangeSet.iterator();
    // add the masked byte range
    while (iterator.hasNext()) {
      maksedByteRange[index++] = iterator.next();
    }
    return maksedByteRange;
  }

  /**
   * Below method will return the max key based on the dimension ordinal
   *
   * @param keyOrdinalList
   * @param generator
   * @return
   * @throws KeyGenException
   */
  public static byte[] getMaxKeyBasedOnOrinal(List<Integer> keyOrdinalList, KeyGenerator generator)
      throws KeyGenException {
    long[] max = new long[generator.getDimCount()];
    Arrays.fill(max, 0L);

    for (int i = 0; i < keyOrdinalList.size(); i++) {
      // adding for dimension which is selected in query
      max[keyOrdinalList.get(i)] = Long.MAX_VALUE;
    }
    return generator.generateKey(max);
  }

  /**
   * To get the max key based on dimensions. i.e. all other dimensions will be
   * set to 0 bits and the required query dimension will be masked with all
   * LONG.MAX so that we can mask key and then compare while aggregating This
   * can be useful during filter query when only few dimensions were selected
   * out of row group
   *
   * @param queryDimensions dimension selected in query
   * @param generator       key generator
   * @param allDimension    all dimension present in the table
   * @return max key for dimension
   * @throws KeyGenException if any problem while generating the key
   */
  public static byte[] getMaxKeyBasedOnDimensions(List<QueryDimension> queryDimensions,
      KeyGenerator generator) throws KeyGenException {
    long[] max = new long[generator.getDimCount()];
    Arrays.fill(max, 0L);

    for (int i = 0; i < queryDimensions.size(); i++) {
      // as no dictionary column and complex type columns
      // are not selected in the mdkey
      // so we will not select the those dimension for calculating the
      // range
      if (queryDimensions.get(i).getDimension().getKeyOrdinal() == -1) {
        continue;
      }
      // adding for dimension which is selected in query
      max[queryDimensions.get(i).getDimension().getKeyOrdinal()] = Long.MAX_VALUE;
    }

    return generator.generateKey(max);
  }

  /**
   * Below method will be used to get the masked key for query
   *
   * @param keySize         size of the masked key
   * @param maskedKeyRanges masked byte range
   * @return masked bytes
   */
  public static int[] getMaskedByte(int keySize, int[] maskedKeyRanges) {
    int[] maskedKey = new int[keySize];
    // all the non selected dimension will be filled with -1
    Arrays.fill(maskedKey, -1);
    for (int i = 0; i < maskedKeyRanges.length; i++) {
      maskedKey[maskedKeyRanges[i]] = i;
    }
    return maskedKey;
  }

  /**
   * Below method will be used to extract the dimension and measure from the
   * expression
   *
   * @param expressions aggregate expression
   * @param dims        extracted dimensions
   * @param msrs        extracted measures
   */
  public static void extractDimensionsAndMeasuresFromExpression(
      List<CustomAggregateExpression> expressions, List<CarbonDimension> dims,
      List<CarbonMeasure> msrs) {
    for (CustomAggregateExpression expression : expressions) {
      List<CarbonColumn> dimsFromExpr = expression.getReferredColumns();
      for (CarbonColumn dimFromExpr : dimsFromExpr) {
        if (!dimFromExpr.isDimesion()) {
          msrs.add((CarbonMeasure) dimFromExpr);
        } else {
          dims.add((CarbonDimension) dimFromExpr);
        }
      }
    }
  }

  /**
   * Below method will be used to get the dimension block index in file based
   * on query dimension
   *
   * @param queryDimensions                query dimension
   * @param dimensionOrdinalToBlockMapping mapping of dimension block in file to query dimension
   * @return block index of file
   */
  public static int[] getDimensionsBlockIndexes(List<QueryDimension> queryDimensions,
      Map<Integer, Integer> dimensionOrdinalToBlockMapping,
      List<DimensionAggregatorInfo> dimAggInfo, List<CarbonDimension> customAggregationDimension) {
    // using set as in row group columns will point to same block
    Set<Integer> dimensionBlockIndex = new HashSet<Integer>();
    for (int i = 0; i < queryDimensions.size(); i++) {
      dimensionBlockIndex.add(
          dimensionOrdinalToBlockMapping.get(queryDimensions.get(i).getDimension().getOrdinal()));
    }
    for (int i = 0; i < dimAggInfo.size(); i++) {
      dimensionBlockIndex
          .add(dimensionOrdinalToBlockMapping.get(dimAggInfo.get(i).getDim().getOrdinal()));
    }
    for (int i = 0; i < customAggregationDimension.size(); i++) {
      dimensionBlockIndex
          .add(dimensionOrdinalToBlockMapping.get(customAggregationDimension.get(i).getOrdinal()));
    }
    return ArrayUtils
        .toPrimitive(dimensionBlockIndex.toArray(new Integer[dimensionBlockIndex.size()]));
  }

  /**
   * Below method will be used to get the dictionary mapping for all the
   * dictionary encoded dimension present in the query
   *
   * @param queryDimensions            query dimension present in the query this will be used to
   *                                   convert the result from surrogate key to actual data
   * @param dimAggInfo                 dimension present in the dimension aggregation
   *                                   dictionary will be used to convert to actual data
   *                                   for aggregation
   * @param customAggregationDimension dimension which is present in the expression for aggregation
   *                                   we need dictionary data
   * @param absoluteTableIdentifier    absolute table identifier
   * @return dimension unique id to its dictionary map
   * @throws QueryExecutionException
   */
  public static Map<String, Dictionary> getDimensionDictionaryDetail(
      List<QueryDimension> queryDimensions, List<DimensionAggregatorInfo> dimAggInfo,
      List<CustomAggregateExpression> customAggExpression,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws QueryExecutionException {
    // to store dimension unique column id list, this is required as
    // dimension can be present in
    // query dimension, as well as some aggregation function will be applied
    // in the same dimension
    // so we need to get only one instance of dictionary
    Set<String> dictionaryDimensionFromQuery = new HashSet<String>();
    for (int i = 0; i < queryDimensions.size(); i++) {
      if (queryDimensions.get(i).getDimension().getEncoder().contains(Encoding.DICTIONARY)) {
        dictionaryDimensionFromQuery.add(queryDimensions.get(i).getDimension().getColumnId());
      }
    }
    for (int i = 0; i < dimAggInfo.size(); i++) {
      if (dimAggInfo.get(i).getDim().getEncoder().contains(Encoding.DICTIONARY)) {
        dictionaryDimensionFromQuery.add(dimAggInfo.get(i).getDim().getColumnId());
      }
    }
    for (int i = 0; i < customAggExpression.size(); i++) {
      List<CarbonColumn> referredColumns = customAggExpression.get(i).getReferredColumns();
      for (CarbonColumn column : referredColumns) {
        if (CarbonUtil.hasEncoding(column.getEncoder(), Encoding.DICTIONARY)) {
          dictionaryDimensionFromQuery.add(column.getColumnId());
        }
      }

    }
    // converting to list as api exposed needed list which i think
    // is not correct
    List<String> dictionaryColumnIdList =
        new ArrayList<String>(dictionaryDimensionFromQuery.size());
    dictionaryColumnIdList.addAll(dictionaryDimensionFromQuery);
    return getDictionaryMap(dictionaryColumnIdList, absoluteTableIdentifier);
  }

  /**
   * Below method will be used to get the column id to its dictionary mapping
   *
   * @param dictionaryColumnIdList  dictionary column list
   * @param absoluteTableIdentifier absolute table identifier
   * @return dictionary mapping
   * @throws QueryExecutionException
   */
  private static Map<String, Dictionary> getDictionaryMap(List<String> dictionaryColumnIdList,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws QueryExecutionException {
    // this for dictionary unique identifier
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers =
        getDictionaryColumnUniqueIdentifierList(dictionaryColumnIdList,
            absoluteTableIdentifier.getCarbonTableIdentifier());
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache forwardDictionaryCache = cacheProvider
        .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath());
    List<Dictionary> columnDictionaryList = null;
    try {
      columnDictionaryList = forwardDictionaryCache.getAll(dictionaryColumnUniqueIdentifiers);
    } catch (CarbonUtilException e) {
      throw new QueryExecutionException(e);
    }
    Map<String, Dictionary> columnDictionaryMap = new HashMap<>(columnDictionaryList.size());
    for (int i = 0; i < dictionaryColumnUniqueIdentifiers.size(); i++) {
      // TODO: null check for column dictionary, if cache size is less it
      // might return null here, in that case throw exception
      columnDictionaryMap.put(dictionaryColumnIdList.get(i), columnDictionaryList.get(i));
    }
    return columnDictionaryMap;
  }

  /**
   * Below method will be used to get the dictionary column unique identifier
   *
   * @param dictionaryColumnIdList dictionary
   * @param carbonTableIdentifier
   * @return
   */
  private static List<DictionaryColumnUniqueIdentifier> getDictionaryColumnUniqueIdentifierList(
      List<String> dictionaryColumnIdList, CarbonTableIdentifier carbonTableIdentifier) {
    CarbonTable carbonTable =
        CarbonMetadata.getInstance().getCarbonTable(carbonTableIdentifier.getTableUniqueName());
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers =
        new ArrayList<>(dictionaryColumnIdList.size());
    for (String columnIdentifier : dictionaryColumnIdList) {
      CarbonDimension dimension = CarbonMetadata.getInstance()
          .getCarbonDimensionBasedOnColIdentifier(carbonTable, columnIdentifier);
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
          new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnIdentifier,
              dimension.getDataType());
      dictionaryColumnUniqueIdentifiers.add(dictionaryColumnUniqueIdentifier);
    }
    return dictionaryColumnUniqueIdentifiers;
  }

  /**
   * Below method will used to get the method will be used to get the measure
   * block indexes to be read from the file
   *
   * @param queryMeasures              query measure
   * @param expressionMeasure          measure present in the expression
   * @param ordinalToBlockIndexMapping measure ordinal to block mapping
   * @return block indexes
   */
  public static int[] getMeasureBlockIndexes(List<QueryMeasure> queryMeasures,
      List<CarbonMeasure> expressionMeasure, Map<Integer, Integer> ordinalToBlockIndexMapping) {
    Set<Integer> measureBlockIndex = new HashSet<Integer>();
    for (int i = 0; i < queryMeasures.size(); i++) {
      measureBlockIndex
          .add(ordinalToBlockIndexMapping.get(queryMeasures.get(i).getMeasure().getOrdinal()));
    }
    for (int i = 0; i < expressionMeasure.size(); i++) {
      measureBlockIndex.add(ordinalToBlockIndexMapping.get(expressionMeasure.get(i).getOrdinal()));
    }
    return ArrayUtils.toPrimitive(measureBlockIndex.toArray(new Integer[measureBlockIndex.size()]));
  }

  /**
   * Below method will be used to get the masked byte range for dimension
   * which is present in order by
   *
   * @param orderByDimensions order by dimension
   * @param generator         key generator
   * @param maskedRanges      masked byte range for dimension
   * @return range of masked byte for order by dimension
   */
  public static int[][] getMaskedByteRangeForSorting(List<QueryDimension> orderByDimensions,
      KeyGenerator generator, int[] maskedRanges) {
    int[][] dimensionCompareIndex = new int[orderByDimensions.size()][];
    int index = 0;
    for (int i = 0; i < dimensionCompareIndex.length; i++) {
      Set<Integer> integers = new TreeSet<Integer>();
      if (!orderByDimensions.get(i).getDimension().getEncoder().contains(Encoding.DICTIONARY)
          || orderByDimensions.get(i).getDimension().numberOfChild() > 0) {
        continue;
      }
      int[] range =
          generator.getKeyByteOffsets(orderByDimensions.get(i).getDimension().getKeyOrdinal());
      for (int j = range[0]; j <= range[1]; j++) {
        integers.add(j);
      }
      dimensionCompareIndex[index] = new int[integers.size()];
      int j = 0;
      for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
        Integer integer = (Integer) iterator.next();
        dimensionCompareIndex[index][j++] = integer.intValue();
      }
      index++;
    }
    for (int i = 0; i < dimensionCompareIndex.length; i++) {
      if (null == dimensionCompareIndex[i]) {
        continue;
      }
      int[] range = dimensionCompareIndex[i];
      if (null != range) {
        for (int j = 0; j < range.length; j++) {
          for (int k = 0; k < maskedRanges.length; k++) {
            if (range[j] == maskedRanges[k]) {
              range[j] = k;
              break;
            }
          }
        }
      }

    }
    return dimensionCompareIndex;
  }

  /**
   * Below method will be used to get the masked key for sorting
   *
   * @param orderDimensions           query dimension
   * @param generator                 key generator
   * @param maskedByteRangeForSorting masked byte range for sorting
   * @param maskedRanges              masked range
   * @return masked byte range
   * @throws QueryExecutionException
   */
  public static byte[][] getMaksedKeyForSorting(List<QueryDimension> orderDimensions,
      KeyGenerator generator, int[][] maskedByteRangeForSorting, int[] maskedRanges)
      throws QueryExecutionException {
    byte[][] maskedKey = new byte[orderDimensions.size()][];
    byte[] mdKey = null;
    long[] key = null;
    byte[] maskedMdKey = null;
    try {
      if (null != maskedByteRangeForSorting) {
        for (int i = 0; i < maskedByteRangeForSorting.length; i++) {
          if (null == maskedByteRangeForSorting[i]) {
            continue;
          }
          key = new long[generator.getDimCount()];
          maskedKey[i] = new byte[maskedByteRangeForSorting[i].length];
          key[orderDimensions.get(i).getDimension().getKeyOrdinal()] = Long.MAX_VALUE;
          mdKey = generator.generateKey(key);
          maskedMdKey = new byte[maskedRanges.length];
          for (int k = 0; k < maskedMdKey.length; k++) { // CHECKSTYLE:OFF
            // Approval
            // No:Approval-V1R2C10_001
            maskedMdKey[k] = mdKey[maskedRanges[k]];
          }
          for (int j = 0; j < maskedByteRangeForSorting[i].length; j++) {
            maskedKey[i][j] = maskedMdKey[maskedByteRangeForSorting[i][j]];
          }// CHECKSTYLE:ON

        }
      }
    } catch (KeyGenException e) {
      throw new QueryExecutionException(e);
    }
    return maskedKey;
  }

  /**
   * Below method will be used to get mapping whether dimension is present in
   * order by or not
   *
   * @param sortedDimensions sort dimension present in order by query
   * @param queryDimensions  query dimension
   * @return sort dimension indexes
   */
  public static byte[] getSortDimensionIndexes(List<QueryDimension> sortedDimensions,
      List<QueryDimension> queryDimensions) {
    byte[] sortedDims = new byte[queryDimensions.size()];
    int indexOf = 0;
    for (int i = 0; i < sortedDims.length; i++) {
      indexOf = sortedDimensions.indexOf(queryDimensions.get(i));
      if (indexOf > -1) {
        sortedDims[i] = 1;
      }
    }
    return sortedDims;
  }

  /**
   * Below method will be used to get the mapping of block index and its
   * restructuring info
   *
   * @param queryDimensions   query dimension from query model
   * @param segmentProperties segment properties
   * @return map of block index to its restructuring info
   * @throws KeyGenException if problem while key generation
   */
  public static Map<Integer, KeyStructureInfo> getColumnGroupKeyStructureInfo(
      List<QueryDimension> queryDimensions, SegmentProperties segmentProperties)
      throws KeyGenException {
    Map<Integer, KeyStructureInfo> rowGroupToItsRSInfo = new HashMap<Integer, KeyStructureInfo>();
    // get column group id and its ordinal mapping of column group
    Map<Integer, List<Integer>> columnGroupAndItsOrdinalMappingForQuery =
        getColumnGroupAndItsOrdinalMapping(queryDimensions);
    Map<Integer, KeyGenerator> columnGroupAndItsKeygenartor =
        segmentProperties.getColumnGroupAndItsKeygenartor();

    Iterator<Entry<Integer, List<Integer>>> iterator =
        columnGroupAndItsOrdinalMappingForQuery.entrySet().iterator();
    KeyStructureInfo restructureInfos = null;
    while (iterator.hasNext()) {
      Entry<Integer, List<Integer>> next = iterator.next();
      KeyGenerator keyGenerator = columnGroupAndItsKeygenartor.get(next.getKey());
      restructureInfos = new KeyStructureInfo();
      // sort the ordinal
      List<Integer> ordinal = next.getValue();
      Collections.sort(ordinal);
      // get the masked byte range for column group
      int[] maskByteRanges = getMaskedByteRangeBasedOrdinal(ordinal, keyGenerator);
      // max key for column group
      byte[] maxKey = getMaxKeyBasedOnOrinal(ordinal, keyGenerator);
      // get masked key for column group
      int[] maksedByte = getMaskedByte(keyGenerator.getKeySizeInBytes(), maskByteRanges);
      restructureInfos.setKeyGenerator(keyGenerator);
      restructureInfos.setMaskByteRanges(maskByteRanges);
      restructureInfos.setMaxKey(maxKey);
      restructureInfos.setMaskedBytes(maksedByte);
      rowGroupToItsRSInfo
          .put(segmentProperties.getDimensionOrdinalToBlockMapping().get(ordinal.get(0)),
              restructureInfos);
    }
    return rowGroupToItsRSInfo;
  }

  /**
   * Below method will be used to create a mapping of column group columns
   * this mapping will have column group id to all the dimension ordinal
   * present in the column group This mapping will be used during query
   * execution, to create a mask key for the column group dimension which will
   * be used in aggregation and filter query as column group dimension will be
   * stored in bit level
   */
  private static Map<Integer, List<Integer>> getColumnGroupAndItsOrdinalMapping(
      List<QueryDimension> dimensions) {
    // list of row groups this will store all the row group column
    Map<Integer, List<Integer>> columnGroupAndItsOrdinalsMapping =
        new HashMap<Integer, List<Integer>>();
    // to store a column group
    List<Integer> currentColumnGroup = null;
    // current index
    int index = 0;
    // previous column group to check all the column of row id has bee
    // selected
    int prvColumnGroupId = -1;
    while (index < dimensions.size()) {
      // if dimension group id is not zero and it is same as the previous
      // column group id
      // then we need to add ordinal of that column as it belongs to same
      // column group
      if (!dimensions.get(index).getDimension().isColumnar()
          && dimensions.get(index).getDimension().columnGroupId() == prvColumnGroupId) {
        currentColumnGroup.add(dimensions.get(index).getDimension().getColumnGroupOrdinal());
      }

      // if dimension is not a columnar then it is column group column
      else if (!dimensions.get(index).getDimension().isColumnar()) {
        currentColumnGroup = new ArrayList<Integer>();
        columnGroupAndItsOrdinalsMapping
            .put(dimensions.get(index).getDimension().columnGroupId(), currentColumnGroup);
        currentColumnGroup.add(dimensions.get(index).getDimension().getColumnGroupOrdinal());
      }
      // update the row id every time,this is required to group the
      // columns
      // of the same row group
      prvColumnGroupId = dimensions.get(index).getDimension().columnGroupId();
      index++;
    }
    return columnGroupAndItsOrdinalsMapping;
  }

  /**
   * Below method will be used to get masked byte
   *
   * @param data           actual data
   * @param maxKey         max key
   * @param maskByteRanges mask byte range
   * @param byteCount
   * @return masked byte
   */
  public static byte[] getMaskedKey(byte[] data, byte[] maxKey, int[] maskByteRanges,
      int byteCount) {
    byte[] maskedKey = new byte[byteCount];
    int counter = 0;
    int byteRange = 0;
    for (int i = 0; i < byteCount; i++) {
      byteRange = maskByteRanges[i];
      if (byteRange != -1) {
        maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
      }
    }
    return maskedKey;
  }

  /**
   * Below method will be used to get the dimension data aggregator list,
   * which will be used to aggregate the dimension data, aggregate function
   * like count(dimension) or any other function on dimension will be handled
   *
   * @param dimensionAggInfoList           dimension aggregation from query model
   * @param dimensionToBlockIndexMapping   dimension block to its index mapping this will be
   *                                       used to read  the block from file
   * @param columnGroupIdToKeyGeneratorMap column group it to its key generator which will
   *                                       be used to unpack the row group columns
   * @param columnUniqueIdToDictionaryMap  this will dictionary column to its dictionary mapping
   * @return list dimension data aggregator objects
   */

  public static List<DimensionDataAggregator> getDimensionDataAggregatorList1(
      List<DimensionAggregatorInfo> dimensionAggInfoList,
      Map<Integer, Integer> dimensionToBlockIndexMapping,
      Map<Integer, KeyGenerator> columnGroupIdToKeyGeneratorMap,
      Map<String, Dictionary> columnUniqueIdToDictionaryMap) {

    Map<Integer, List<DimensionAggregatorInfo>> arrangeDimensionAggregationInfo =
        arrangeDimensionAggregationInfo(dimensionAggInfoList, dimensionToBlockIndexMapping);
    List<DimensionDataAggregator> dimensionDataAggregators =
        new ArrayList<DimensionDataAggregator>();
    int aggregatorStartIndex = 0;

    for (Entry<Integer, List<DimensionAggregatorInfo>> entry : arrangeDimensionAggregationInfo
        .entrySet()) {
      // if number of dimension aggregation is info is more than 2 than it
      // is a column group dimension
      // so only one aggregator instance will be created to handle the
      // aggregation
      // as in case of column group unpacking of the bit packed mdkey will
      // be done
      // only once
      if (entry.getValue().size() > 1) {
        // how many aggregator will be used for column group
        int numberOfAggregatorForColumnGroup = 0;
        List<Dictionary> dictionaryList = new ArrayList<Dictionary>();
        // below code is to create a dictionary list of all the column
        // group dimension present in the query
        for (DimensionAggregatorInfo dimensionAggregatorInfo : entry.getValue()) {
          dictionaryList.add(
              columnUniqueIdToDictionaryMap.get(dimensionAggregatorInfo.getDim().getColumnId()));
          numberOfAggregatorForColumnGroup += dimensionAggregatorInfo.getAggList().size();
        }
        dimensionDataAggregators.add(new ColumnGroupDimensionsAggregator(entry.getValue(),
            columnGroupIdToKeyGeneratorMap.get(entry.getValue().get(0).getDim().getColumnId()),
            dimensionToBlockIndexMapping.get(entry.getValue().get(0)), dictionaryList,
            aggregatorStartIndex));
        aggregatorStartIndex += numberOfAggregatorForColumnGroup;
        continue;
      } else {
        // if it is a dictionary column than create a fixed length
        // aggeragtor
        if (CarbonUtil
            .hasEncoding(entry.getValue().get(0).getDim().getEncoder(), Encoding.DICTIONARY)) {
          dimensionDataAggregators.add(
              new FixedLengthDimensionAggregator(entry.getValue().get(0), null,
                  columnUniqueIdToDictionaryMap.get(entry.getValue().get(0).getDim().getColumnId()),
                  aggregatorStartIndex,
                  dimensionToBlockIndexMapping.get(entry.getValue().get(0).getDim().getOrdinal())));
        } else {
          // else for not dictionary column create a
          // variable length aggregator
          dimensionDataAggregators.add(
              new VariableLengthDimensionAggregator(entry.getValue().get(0), null,
                  aggregatorStartIndex,
                  dimensionToBlockIndexMapping.get(entry.getValue().get(0).getDim().getOrdinal())));
        }
        aggregatorStartIndex += entry.getValue().get(0).getAggList().size();
      }
    }
    return dimensionDataAggregators;
  }

  /**
   * Below method will be used to group the dimension aggregation infos This
   * grouping will be based on block index of the file to dimension mapping
   * Basically it will group all the dimension aggregation of same column
   * group dimension This is done to avoid the column group dimension mdkey
   * unpacking multiple times. If all the dimension of column group is handled
   * separately then unpacking of mdkey for same column group will be done
   * multiple times and it will impact the query performance, so to avoid this
   * if we group the dimension together and they will point same block in the
   * physical file so reading will be done only once and unpacking of each row
   * will be also done only once
   *
   * @param queryDimensionAggregationInfos      query dimension aggregation infos
   * @param dimensionOrdinaltoBlockIndexMapping dimension to file block index mapping
   * @return block index in file to list of dimension pointing to that block
   * mapping
   */
  private static Map<Integer, List<DimensionAggregatorInfo>> arrangeDimensionAggregationInfo(
      List<DimensionAggregatorInfo> queryDimensionAggregationInfos,
      Map<Integer, Integer> dimensionOrdinaltoBlockIndexMapping) {
    Map<Integer, List<DimensionAggregatorInfo>> groupedDimensionAggregationInfo =
        new LinkedHashMap<Integer, List<DimensionAggregatorInfo>>();
    for (DimensionAggregatorInfo queryDimensionAggregatorInfo : queryDimensionAggregationInfos) {
      List<DimensionAggregatorInfo> list = groupedDimensionAggregationInfo.get(
          dimensionOrdinaltoBlockIndexMapping
              .get(queryDimensionAggregatorInfo.getDim().getOrdinal()));

      if (null == list) {
        list =
            new ArrayList<DimensionAggregatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        groupedDimensionAggregationInfo.put(dimensionOrdinaltoBlockIndexMapping
            .get(queryDimensionAggregatorInfo.getDim().getOrdinal()), list);
      }
      list.add(queryDimensionAggregatorInfo);
    }
    return groupedDimensionAggregationInfo;
  }

  /**
   * Below method will be used to fill block indexes of the query dimension
   * which will be used in creating a output row Here is method we are passing
   * two list which store the indexes one for dictionary column other for not
   * dictionary column. This is done for specific purpose so that in one
   * iteration we will be able to fill both type dimension block indexes
   *
   * @param queryDimensions                  dimension present in the query
   * @param columnOrdinalToBlockIndexMapping column ordinal to block index mapping
   * @param dictionaryDimensionBlockIndex    list to store dictionary column block indexes
   * @param noDictionaryDimensionBlockIndex  list to store no dictionary block indexes
   */
  public static void fillQueryDimensionsBlockIndexes(List<QueryDimension> queryDimensions,
      Map<Integer, Integer> columnOrdinalToBlockIndexMapping,
      List<Integer> dictionaryDimensionBlockIndex, List<Integer> noDictionaryDimensionBlockIndex) {
    for (QueryDimension queryDimension : queryDimensions) {
      if (CarbonUtil.hasEncoding(queryDimension.getDimension().getEncoder(), Encoding.DICTIONARY)) {
        dictionaryDimensionBlockIndex
            .add(columnOrdinalToBlockIndexMapping.get(queryDimension.getDimension().getOrdinal()));
      } else {
        noDictionaryDimensionBlockIndex
            .add(columnOrdinalToBlockIndexMapping.get(queryDimension.getDimension().getOrdinal()));
      }
    }
  }

  /**
   * Below method will be used to resolve the query model
   * resolve will be setting the actual dimension and measure object
   * as from driver only column name will be passes to avoid the heavy object
   * serialization
   *
   * @param queryModel query model
   */
  public static void resolveQueryModel(QueryModel queryModel) {
    CarbonMetadata.getInstance().addCarbonTable(queryModel.getTable());
    // TODO need to load the table from table identifier
    CarbonTable carbonTable = queryModel.getTable();
    String tableName =
        queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName();
    // resolve query dimension
    for (QueryDimension queryDimension : queryModel.getQueryDimension()) {
      queryDimension
          .setDimension(carbonTable.getDimensionByName(tableName, queryDimension.getColumnName()));
    }
    // resolve sort dimension
    for (QueryDimension sortDimension : queryModel.getSortDimension()) {
      sortDimension
          .setDimension(carbonTable.getDimensionByName(tableName, sortDimension.getColumnName()));
    }
    // resolve query measure
    for (QueryMeasure queryMeasure : queryModel.getQueryMeasures()) {
      // in case of count start column name will  be count * so
      // for count start add first measure if measure is not present
      // than add first dimension as a measure
      if (queryMeasure.getColumnName().equals("count(*)")) {
        if (carbonTable.getMeasureByTableName(tableName).size() > 0) {
          queryMeasure.setMeasure(carbonTable.getMeasureByTableName(tableName).get(0));
        } else {
          CarbonMeasure dummyMeasure = new CarbonMeasure(
              carbonTable.getDimensionByTableName(tableName).get(0).getColumnSchema(), 0);
          queryMeasure.setMeasure(dummyMeasure);
        }
      } else {
        queryMeasure
            .setMeasure(carbonTable.getMeasureByName(tableName, queryMeasure.getColumnName()));
      }
    }
    // resolve dimension aggregation info
    for (DimensionAggregatorInfo dimAggInfo : queryModel.getDimAggregationInfo()) {
      dimAggInfo.setDim(carbonTable.getDimensionByName(tableName, dimAggInfo.getColumnName()));
    }
    //TODO need to handle expression
  }

  /**
   * Below method will be used to get the index of number type aggregator
   *
   * @param aggType
   * @return index in aggregator
   */
  public static int[] getNumberTypeIndex(List<String> aggType) {
    List<Integer> indexList = new ArrayList<Integer>();
    for (int i = 0; i < aggType.size(); i++) {
      if (CarbonCommonConstants.SUM.equals(aggType.get(i)) || CarbonCommonConstants.AVERAGE
          .equals(aggType.get(i))) {
        indexList.add(i);
      }
    }
    return ArrayUtils.toPrimitive(indexList.toArray(new Integer[indexList.size()]));
  }

  /**
   * below method will be used to get the actual type aggregator
   *
   * @param aggType
   * @return index in aggrgetor
   */
  public static int[] getActualTypeIndex(List<String> aggType) {
    List<Integer> indexList = new ArrayList<Integer>();
    for (int i = 0; i < aggType.size(); i++) {
      if (!CarbonCommonConstants.SUM.equals(aggType.get(i)) && !CarbonCommonConstants.AVERAGE
          .equals(aggType.get(i))) {
        indexList.add(i);
      }
    }
    return ArrayUtils.toPrimitive(indexList.toArray(new Integer[indexList.size()]));
  }
}
