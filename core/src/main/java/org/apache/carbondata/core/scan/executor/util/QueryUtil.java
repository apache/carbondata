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
package org.apache.carbondata.core.scan.executor.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.core.scan.complextypes.PrimitiveQueryType;
import org.apache.carbondata.core.scan.complextypes.StructQueryType;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

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
  public static int[] getMaskedByteRange(List<ProjectionDimension> queryDimensions,
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
   * @return max key for dimension
   * @throws KeyGenException if any problem while generating the key
   */
  public static byte[] getMaxKeyBasedOnDimensions(List<ProjectionDimension> queryDimensions,
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
   * Below method will be used to get the dimension block index in file based
   * on query dimension
   *
   * @param queryDimensions                query dimension
   * @param dimensionOrdinalToChunkMapping mapping of dimension block in file to query dimension
   * @return block index of file
   */
  public static int[] getDimensionChunkIndexes(List<ProjectionDimension> queryDimensions,
      Map<Integer, Integer> dimensionOrdinalToChunkMapping,
      List<CarbonDimension> customAggregationDimension, Set<CarbonDimension> filterDimensions,
      Set<Integer> allProjectionListDimensionIndexes) {
    // using set as in row group columns will point to same block
    Set<Integer> dimensionChunkIndex = new HashSet<Integer>();
    Set<Integer> filterDimensionOrdinal = getFilterDimensionOrdinal(filterDimensions);
    int chunkIndex = 0;
    for (int i = 0; i < queryDimensions.size(); i++) {
      if (queryDimensions.get(i).getDimension().hasEncoding(Encoding.IMPLICIT)) {
        continue;
      }

      Integer dimensionOrdinal = queryDimensions.get(i).getDimension().getOrdinal();
      allProjectionListDimensionIndexes.add(dimensionOrdinalToChunkMapping.get(dimensionOrdinal));
      if (queryDimensions.get(i).getDimension().getNumberOfChild() > 0) {
        addChildrenBlockIndex(allProjectionListDimensionIndexes,
            queryDimensions.get(i).getDimension());
      }

      if (!filterDimensionOrdinal.contains(dimensionOrdinal)) {
        chunkIndex = dimensionOrdinalToChunkMapping.get(dimensionOrdinal);
        dimensionChunkIndex.add(chunkIndex);
        if (queryDimensions.get(i).getDimension().getNumberOfChild() > 0) {
          addChildrenBlockIndex(dimensionChunkIndex, queryDimensions.get(i).getDimension());
        }
      }
    }
    for (int i = 0; i < customAggregationDimension.size(); i++) {
      chunkIndex =
          dimensionOrdinalToChunkMapping.get(customAggregationDimension.get(i).getOrdinal());
      // not adding the children dimension as dimension aggregation
      // is not push down in case of complex dimension
      dimensionChunkIndex.add(chunkIndex);
    }
    int[] dimensionIndex = ArrayUtils
        .toPrimitive(dimensionChunkIndex.toArray(new Integer[dimensionChunkIndex.size()]));
    Arrays.sort(dimensionIndex);
    return dimensionIndex;
  }

  /**
   * Below method will be used to add the children block index
   * this will be basically for complex dimension which will have children
   *
   * @param blockIndexes block indexes
   * @param dimension    parent dimension
   */
  private static void addChildrenBlockIndex(Set<Integer> blockIndexes, CarbonDimension dimension) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      addChildrenBlockIndex(blockIndexes, dimension.getListOfChildDimensions().get(i));
      blockIndexes.add(dimension.getListOfChildDimensions().get(i).getOrdinal());
    }
  }

  /**
   * Below method will be used to get the dictionary mapping for all the
   * dictionary encoded dimension present in the query
   *
   * @param queryDimensions         query dimension present in the query this will be used to
   *                                convert the result from surrogate key to actual data
   * @param absoluteTableIdentifier absolute table identifier
   * @return dimension unique id to its dictionary map
   * @throws IOException
   */
  public static Map<String, Dictionary> getDimensionDictionaryDetail(
      List<ProjectionDimension> queryDimensions, Set<CarbonDimension> filterComplexDimensions,
      AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider)
      throws IOException {
    // to store complex dimension and its child id unique column id list, this is required as
    // dimension can be present in  projection and filter
    // so we need to get only one instance of dictionary
    // direct dictionary skip is done only for the dictionary lookup
    Set<String> dictionaryDimensionFromQuery = new HashSet<String>();
    for (int i = 0; i < queryDimensions.size(); i++) {
      List<Encoding> encodingList = queryDimensions.get(i).getDimension().getEncoder();
      // TODO need to remove the data type check for parent column in complex type no need to
      // write encoding dictionary
      if (CarbonUtil.hasEncoding(encodingList, Encoding.DICTIONARY) && !CarbonUtil
          .hasEncoding(encodingList, Encoding.DIRECT_DICTIONARY) && !CarbonUtil
          .hasEncoding(encodingList, Encoding.IMPLICIT)
          && queryDimensions.get(i).getDimension().getNumberOfChild() > 0) {
        getChildDimensionDictionaryDetail(queryDimensions.get(i).getDimension(),
            dictionaryDimensionFromQuery);
      }
    }
    Iterator<CarbonDimension> iterator = filterComplexDimensions.iterator();
    while (iterator.hasNext()) {
      CarbonDimension filterDim = iterator.next();
      // only to add complex dimension
      if (filterDim.getNumberOfChild() > 0) {
        getChildDimensionDictionaryDetail(filterDim, dictionaryDimensionFromQuery);
      }
    }
    // converting to list as api exposed needed list which i think
    // is not correct
    List<String> dictionaryColumnIdList =
        new ArrayList<String>(dictionaryDimensionFromQuery.size());
    dictionaryColumnIdList.addAll(dictionaryDimensionFromQuery);
    return getDictionaryMap(dictionaryColumnIdList, absoluteTableIdentifier, tableProvider);
  }

  /**
   * Below method will be used to fill the children dimension column id
   *
   * @param queryDimensions              query dimension
   * @param dictionaryDimensionFromQuery dictionary dimension for query
   */
  private static void getChildDimensionDictionaryDetail(CarbonDimension queryDimensions,
      Set<String> dictionaryDimensionFromQuery) {
    for (int j = 0; j < queryDimensions.getNumberOfChild(); j++) {
      List<Encoding> encodingList = queryDimensions.getListOfChildDimensions().get(j).getEncoder();
      if (queryDimensions.getListOfChildDimensions().get(j).getNumberOfChild() > 0) {
        getChildDimensionDictionaryDetail(queryDimensions.getListOfChildDimensions().get(j),
            dictionaryDimensionFromQuery);
      } else if (!CarbonUtil.hasEncoding(encodingList, Encoding.DIRECT_DICTIONARY)) {
        dictionaryDimensionFromQuery
            .add(queryDimensions.getListOfChildDimensions().get(j).getColumnId());
      }
    }
  }

  /**
   * Below method will be used to get the column id to its dictionary mapping
   *
   * @param dictionaryColumnIdList  dictionary column list
   * @param absoluteTableIdentifier absolute table identifier
   * @return dictionary mapping
   * @throws IOException
   */
  private static Map<String, Dictionary> getDictionaryMap(List<String> dictionaryColumnIdList,
      AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider)
      throws IOException {
    // if any complex dimension not present in query then return the empty map
    if (dictionaryColumnIdList.size() == 0) {
      return new HashMap<>();
    }
    // this for dictionary unique identifier
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers =
        getDictionaryColumnUniqueIdentifierList(dictionaryColumnIdList,
            absoluteTableIdentifier.getCarbonTableIdentifier(), tableProvider);
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
        .createCache(CacheType.FORWARD_DICTIONARY);
    List<Dictionary> columnDictionaryList =
        forwardDictionaryCache.getAll(dictionaryColumnUniqueIdentifiers);
    Map<String, Dictionary> columnDictionaryMap = new HashMap<>(columnDictionaryList.size());
    for (int i = 0; i < dictionaryColumnUniqueIdentifiers.size(); i++) {
      // TODO: null check for column dictionary, if cache size is less it
      // might return null here, in that case throw exception
      columnDictionaryMap
          .put(dictionaryColumnUniqueIdentifiers.get(i).getColumnIdentifier().getColumnId(),
              columnDictionaryList.get(i));
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
      List<String> dictionaryColumnIdList, CarbonTableIdentifier carbonTableIdentifier,
      TableProvider tableProvider) throws IOException {
    CarbonTable carbonTable = tableProvider.getCarbonTable(carbonTableIdentifier);
    List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers =
        new ArrayList<>(dictionaryColumnIdList.size());
    for (String columnId : dictionaryColumnIdList) {
      CarbonDimension dimension = CarbonMetadata.getInstance()
          .getCarbonDimensionBasedOnColIdentifier(carbonTable, columnId);
      if (dimension != null) {
        AbsoluteTableIdentifier dictionarySourceAbsoluteTableIdentifier;
        ColumnIdentifier columnIdentifier;
        if (null != dimension.getColumnSchema().getParentColumnTableRelations() && !dimension
            .getColumnSchema().getParentColumnTableRelations().isEmpty()) {
          dictionarySourceAbsoluteTableIdentifier = getTableIdentifierForColumn(dimension,
              carbonTable.getAbsoluteTableIdentifier());
          columnIdentifier = new ColumnIdentifier(
              dimension.getColumnSchema().getParentColumnTableRelations().get(0).getColumnId(),
              dimension.getColumnProperties(), dimension.getDataType());
        } else {
          dictionarySourceAbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
          columnIdentifier = dimension.getColumnIdentifier();
        }
        String dictionaryPath = carbonTable.getTableInfo().getFactTable().getTableProperties()
            .get(CarbonCommonConstants.DICTIONARY_PATH);
        dictionaryColumnUniqueIdentifiers.add(
            new DictionaryColumnUniqueIdentifier(dictionarySourceAbsoluteTableIdentifier,
                columnIdentifier, dimension.getDataType(), dictionaryPath));
      }
    }
    return dictionaryColumnUniqueIdentifiers;
  }

  public static AbsoluteTableIdentifier getTableIdentifierForColumn(CarbonDimension carbonDimension,
      AbsoluteTableIdentifier identifier) {
    RelationIdentifier relation = carbonDimension.getColumnSchema()
        .getParentColumnTableRelations()
        .get(0)
        .getRelationIdentifier();
    String parentTableName = relation.getTableName();
    String parentDatabaseName = relation.getDatabaseName();
    String parentTableId = relation.getTableId();
    String newTablePath =
        CarbonTablePath.getNewTablePath(identifier.getTablePath(), parentTableName);
    return AbsoluteTableIdentifier.from(newTablePath, parentDatabaseName, parentTableName,
        parentTableId);
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
  public static int[] getMeasureChunkIndexes(List<ProjectionMeasure> queryMeasures,
      List<CarbonMeasure> expressionMeasure, Map<Integer, Integer> ordinalToBlockIndexMapping,
      Set<CarbonMeasure> filterMeasures, List<Integer> allProjectionListMeasureIdexes) {
    Set<Integer> measureChunkIndex = new HashSet<Integer>();
    Set<Integer> filterMeasureOrdinal = getFilterMeasureOrdinal(filterMeasures);
    for (int i = 0; i < queryMeasures.size(); i++) {
      Integer measureOrdinal = queryMeasures.get(i).getMeasure().getOrdinal();
      allProjectionListMeasureIdexes.add(measureOrdinal);
      if (!filterMeasureOrdinal.contains(measureOrdinal)) {
        measureChunkIndex.add(ordinalToBlockIndexMapping.get(measureOrdinal));
      }
    }
    for (int i = 0; i < expressionMeasure.size(); i++) {
      measureChunkIndex.add(ordinalToBlockIndexMapping.get(expressionMeasure.get(i).getOrdinal()));
    }
    int[] measureIndexes =
        ArrayUtils.toPrimitive(measureChunkIndex.toArray(new Integer[measureChunkIndex.size()]));
    Arrays.sort(measureIndexes);
    return measureIndexes;
  }

  /**
   * Below method will be used to get mapping whether dimension is present in
   * order by or not
   *
   * @param sortedDimensions sort dimension present in order by query
   * @param queryDimensions  query dimension
   * @return sort dimension indexes
   */
  public static byte[] getSortDimensionIndexes(List<ProjectionDimension> sortedDimensions,
      List<ProjectionDimension> queryDimensions) {
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
      List<ProjectionDimension> queryDimensions, SegmentProperties segmentProperties)
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
      List<Integer> mdKeyOrdinal = new ArrayList<Integer>();
      //Un sorted
      List<Integer> mdKeyOrdinalForQuery = new ArrayList<Integer>();
      for (Integer ord : ordinal) {
        mdKeyOrdinal.add(segmentProperties.getColumnGroupMdKeyOrdinal(next.getKey(), ord));
        mdKeyOrdinalForQuery.add(segmentProperties.getColumnGroupMdKeyOrdinal(next.getKey(), ord));
      }
      Collections.sort(mdKeyOrdinal);
      // get the masked byte range for column group
      int[] maskByteRanges = getMaskedByteRangeBasedOrdinal(mdKeyOrdinal, keyGenerator);
      // max key for column group
      byte[] maxKey = getMaxKeyBasedOnOrinal(mdKeyOrdinal, keyGenerator);
      restructureInfos.setKeyGenerator(keyGenerator);
      restructureInfos.setMaskByteRanges(maskByteRanges);
      restructureInfos.setMaxKey(maxKey);
      restructureInfos.setMdkeyQueryDimensionOrdinal(ArrayUtils
          .toPrimitive(mdKeyOrdinalForQuery.toArray(new Integer[mdKeyOrdinalForQuery.size()])));
      rowGroupToItsRSInfo
          .put(segmentProperties.getDimensionOrdinalToChunkMapping().get(ordinal.get(0)),
              restructureInfos);
    }
    return rowGroupToItsRSInfo;
  }

  /**
   * return true if given key is found in array
   *
   * @param data
   * @param key
   * @return
   */
  public static boolean searchInArray(int[] data, int key) {
    for (int i = 0; i < data.length; i++) {
      if (key == data[i]) {
        return true;
      }
    }
    return false;
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
      List<ProjectionDimension> origDimensions) {

    List<ProjectionDimension> dimensions = new ArrayList<>(origDimensions.size());
    dimensions.addAll(origDimensions);
    /*
     * sort based on column group id
     */
    Collections.sort(dimensions, new Comparator<ProjectionDimension>() {

      @Override public int compare(ProjectionDimension o1, ProjectionDimension o2) {
        return Integer
            .compare(o1.getDimension().columnGroupId(), o2.getDimension().columnGroupId());
      }
    });
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
      if (dimensions.get(index).getDimension().hasEncoding(Encoding.IMPLICIT)) {
        index++;
        continue;
      } else if (!dimensions.get(index).getDimension().isColumnar()
          && dimensions.get(index).getDimension().columnGroupId() == prvColumnGroupId
          && null != currentColumnGroup) {
        currentColumnGroup.add(dimensions.get(index).getDimension().getOrdinal());
      }

      // if dimension is not a columnar then it is column group column
      else if (!dimensions.get(index).getDimension().isColumnar()) {
        currentColumnGroup = new ArrayList<Integer>();
        columnGroupAndItsOrdinalsMapping
            .put(dimensions.get(index).getDimension().columnGroupId(), currentColumnGroup);
        currentColumnGroup.add(dimensions.get(index).getDimension().getOrdinal());
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
   * Below method will be used to fill block indexes of the query dimension
   * which will be used in creating a output row Here is method we are passing
   * two list which store the indexes one for dictionary column other for not
   * dictionary column. This is done for specific purpose so that in one
   * iteration we will be able to fill both type dimension block indexes
   *
   * @param projectDimensions                  dimension present in the query
   * @param columnOrdinalToChunkIndexMapping column ordinal to block index mapping
   * @param dictionaryDimensionChunkIndex    list to store dictionary column block indexes
   * @param noDictionaryDimensionChunkIndex  list to store no dictionary block indexes
   */
  public static void fillQueryDimensionChunkIndexes(
      List<ProjectionDimension> projectDimensions,
      Map<Integer, Integer> columnOrdinalToChunkIndexMapping,
      Set<Integer> dictionaryDimensionChunkIndex,
      List<Integer> noDictionaryDimensionChunkIndex) {
    for (ProjectionDimension queryDimension : projectDimensions) {
      if (CarbonUtil.hasEncoding(queryDimension.getDimension().getEncoder(), Encoding.DICTIONARY)
          && queryDimension.getDimension().getNumberOfChild() == 0) {
        dictionaryDimensionChunkIndex
            .add(columnOrdinalToChunkIndexMapping.get(queryDimension.getDimension().getOrdinal()));
      } else if (
          !CarbonUtil.hasEncoding(queryDimension.getDimension().getEncoder(), Encoding.IMPLICIT)
              && queryDimension.getDimension().getNumberOfChild() == 0) {
        noDictionaryDimensionChunkIndex
            .add(columnOrdinalToChunkIndexMapping.get(queryDimension.getDimension().getOrdinal()));
      }
    }
  }

  /**
   * Below method will be used to get the key structure for the column group
   *
   * @param segmentProperties      segment properties
   * @param dimColumnEvaluatorInfo dimension evaluator info
   * @return key structure info for column group dimension
   * @throws KeyGenException
   */
  public static KeyStructureInfo getKeyStructureInfo(SegmentProperties segmentProperties,
      DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) throws KeyGenException {
    int colGrpId = getColumnGroupId(segmentProperties, dimColumnEvaluatorInfo.getColumnIndex());
    KeyGenerator keyGenerator = segmentProperties.getColumnGroupAndItsKeygenartor().get(colGrpId);
    List<Integer> mdKeyOrdinal = new ArrayList<Integer>();

    mdKeyOrdinal.add(segmentProperties
        .getColumnGroupMdKeyOrdinal(colGrpId, dimColumnEvaluatorInfo.getColumnIndex()));
    int[] maskByteRanges = QueryUtil.getMaskedByteRangeBasedOrdinal(mdKeyOrdinal, keyGenerator);
    byte[] maxKey = QueryUtil.getMaxKeyBasedOnOrinal(mdKeyOrdinal, keyGenerator);
    KeyStructureInfo restructureInfos = new KeyStructureInfo();
    restructureInfos.setKeyGenerator(keyGenerator);
    restructureInfos.setMaskByteRanges(maskByteRanges);
    restructureInfos.setMaxKey(maxKey);
    return restructureInfos;
  }

  /**
   * Below method will be used to get the column group id based on the ordinal
   *
   * @param segmentProperties segment properties
   * @param ordinal           ordinal to be searched
   * @return column group id
   */
  public static int getColumnGroupId(SegmentProperties segmentProperties, int ordinal) {
    int[][] columnGroups = segmentProperties.getColumnGroups();
    int colGrpId = -1;
    for (int i = 0; i < columnGroups.length; i++) {
      if (columnGroups[i].length > 1) {
        colGrpId++;
        if (QueryUtil.searchInArray(columnGroups[i], ordinal)) {
          break;
        }
      }
    }
    return colGrpId;
  }

  /**
   * Below method will be used to get the map of for complex dimension and its type
   * which will be used to during query execution to
   *
   * @param queryDimensions          complex dimension in query
   * @param dimensionToBlockIndexMap dimension to block index in file map
   * @return complex dimension and query type
   */
  public static Map<Integer, GenericQueryType> getComplexDimensionsMap(
      List<ProjectionDimension> queryDimensions, Map<Integer, Integer> dimensionToBlockIndexMap,
      int[] eachComplexColumnValueSize, Map<String, Dictionary> columnIdToDictionaryMap,
      Set<CarbonDimension> filterDimensions) {
    Map<Integer, GenericQueryType> complexTypeMap = new HashMap<Integer, GenericQueryType>();
    for (ProjectionDimension dimension : queryDimensions) {
      CarbonDimension actualDimension = dimension.getDimension();
      if (actualDimension.getNumberOfChild() == 0) {
        continue;
      }
      fillParentDetails(dimensionToBlockIndexMap, actualDimension, complexTypeMap,
          eachComplexColumnValueSize, columnIdToDictionaryMap);
    }
    if (null != filterDimensions) {
      for (CarbonDimension filterDimension : filterDimensions) {
        // do not fill nay details for implicit dimension type
        if (filterDimension.hasEncoding(Encoding.IMPLICIT)
            || filterDimension.getNumberOfChild() == 0) {
          continue;
        }
        fillParentDetails(dimensionToBlockIndexMap, filterDimension, complexTypeMap,
            eachComplexColumnValueSize, columnIdToDictionaryMap);
      }
    }
    return complexTypeMap;
  }

  private static void fillParentDetails(Map<Integer, Integer> dimensionToBlockIndexMap,
      CarbonDimension dimension, Map<Integer, GenericQueryType> complexTypeMap,
      int[] eachComplexColumnValueSize, Map<String, Dictionary> columnIdToDictionaryMap) {
    int parentBlockIndex = dimensionToBlockIndexMap.get(dimension.getOrdinal());
    GenericQueryType parentQueryType;
    if (DataTypes.isArrayType(dimension.getDataType())) {
      parentQueryType =
          new ArrayQueryType(dimension.getColName(), dimension.getColName(), parentBlockIndex);
    } else if (DataTypes.isStructType(dimension.getDataType())) {
      parentQueryType =
          new StructQueryType(dimension.getColName(), dimension.getColName(),
              dimensionToBlockIndexMap.get(dimension.getOrdinal()));
    } else {
      throw new UnsupportedOperationException(dimension.getDataType().getName() +
          " is not supported");
    }
    complexTypeMap.put(dimension.getOrdinal(), parentQueryType);
    fillChildrenDetails(eachComplexColumnValueSize, columnIdToDictionaryMap, parentBlockIndex,
            dimension, parentQueryType);
  }

  private static int fillChildrenDetails(int[] eachComplexColumnValueSize,
      Map<String, Dictionary> columnIdToDictionaryMap, int parentBlockIndex,
      CarbonDimension dimension, GenericQueryType parentQueryType) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      DataType dataType = dimension.getListOfChildDimensions().get(i).getDataType();
      if (DataTypes.isArrayType(dataType)) {
        parentQueryType.addChildren(
            new ArrayQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
                dimension.getColName(), ++parentBlockIndex));
      } else if (DataTypes.isStructType(dataType)) {
        parentQueryType.addChildren(
            new StructQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
                dimension.getColName(), ++parentBlockIndex));
      } else {
        boolean isDirectDictionary = CarbonUtil
            .hasEncoding(dimension.getListOfChildDimensions().get(i).getEncoder(),
                Encoding.DIRECT_DICTIONARY);
        parentQueryType.addChildren(
            new PrimitiveQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
                dimension.getColName(), ++parentBlockIndex,
                dimension.getListOfChildDimensions().get(i).getDataType(),
                eachComplexColumnValueSize[dimension.getListOfChildDimensions().get(i)
                    .getComplexTypeOrdinal()], columnIdToDictionaryMap
                .get(dimension.getListOfChildDimensions().get(i).getColumnId()),
                isDirectDictionary));
      }
      if (dimension.getListOfChildDimensions().get(i).getNumberOfChild() > 0) {
        parentBlockIndex = fillChildrenDetails(eachComplexColumnValueSize, columnIdToDictionaryMap,
            parentBlockIndex, dimension.getListOfChildDimensions().get(i), parentQueryType);
      }
    }
    return parentBlockIndex;
  }

  public static void getAllFilterDimensions(FilterResolverIntf filterResolverTree,
      Set<CarbonDimension> filterDimensions, Set<CarbonMeasure> filterMeasure) {
    if (null == filterResolverTree) {
      return;
    }
    List<ColumnExpression> dimensionResolvedInfos = new ArrayList<ColumnExpression>();
    Expression filterExpression = filterResolverTree.getFilterExpression();
    addColumnDimensions(filterExpression, filterDimensions, filterMeasure);
    for (ColumnExpression info : dimensionResolvedInfos) {
      if (info.isDimension() && info.getDimension().getNumberOfChild() > 0) {
        filterDimensions.add(info.getDimension());
      }
    }
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively and add the dimension instance to the set which holds the dimension
   * instances of the complex filter expressions.
   */
  private static void addColumnDimensions(Expression expression,
      Set<CarbonDimension> filterDimensions, Set<CarbonMeasure> filterMeasure) {
    if (null != expression && expression instanceof ColumnExpression) {
      if (((ColumnExpression) expression).isDimension()) {
        filterDimensions.add(((ColumnExpression) expression).getDimension());
      } else {
        filterMeasure.add((CarbonMeasure) ((ColumnExpression) expression).getCarbonColumn());
      }
      return;
    } else if (null != expression) {
      for (Expression child : expression.getChildren()) {
        addColumnDimensions(child, filterDimensions, filterMeasure);
      }
    }
  }

  private static Set<Integer> getFilterMeasureOrdinal(Set<CarbonMeasure> filterMeasures) {
    Set<Integer> filterMeasuresOrdinal = new HashSet<>();
    for (CarbonMeasure filterMeasure : filterMeasures) {
      filterMeasuresOrdinal.add(filterMeasure.getOrdinal());
    }
    return filterMeasuresOrdinal;
  }

  private static Set<Integer> getFilterDimensionOrdinal(Set<CarbonDimension> filterDimensions) {
    Set<Integer> filterDimensionsOrdinal = new HashSet<>();
    for (CarbonDimension filterDimension : filterDimensions) {
      filterDimensionsOrdinal.add(filterDimension.getOrdinal());
      getChildDimensionOrdinal(filterDimension, filterDimensionsOrdinal);
    }
    return filterDimensionsOrdinal;
  }

  /**
   * Below method will be used to fill the children dimension column id
   */
  private static void getChildDimensionOrdinal(CarbonDimension queryDimensions,
      Set<Integer> filterDimensionsOrdinal) {
    for (int j = 0; j < queryDimensions.getNumberOfChild(); j++) {
      List<Encoding> encodingList = queryDimensions.getListOfChildDimensions().get(j).getEncoder();
      if (queryDimensions.getListOfChildDimensions().get(j).getNumberOfChild() > 0) {
        getChildDimensionOrdinal(queryDimensions.getListOfChildDimensions().get(j),
            filterDimensionsOrdinal);
      } else if (!CarbonUtil.hasEncoding(encodingList, Encoding.DIRECT_DICTIONARY)) {
        filterDimensionsOrdinal.add(queryDimensions.getListOfChildDimensions().get(j).getOrdinal());
      }
    }
  }
}
