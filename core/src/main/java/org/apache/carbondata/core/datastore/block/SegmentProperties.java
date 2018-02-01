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
package org.apache.carbondata.core.datastore.block;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.ColumnGroupModel;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.apache.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * This class contains all the details about the restructuring information of
 * the block. This will be used during query execution to handle restructure
 * information
 */
public class SegmentProperties {

  /**
   * key generator of the block which was used to generate the mdkey for
   * normal dimension. this will be required to
   */
  private KeyGenerator dimensionKeyGenerator;

  /**
   * key generator which was used to generate the mdkey for dimensions in SORT_COLUMNS
   * if SORT_COLUMNS contains all dimensions, it is same with dimensionKeyGenerator
   * otherwise, it is different with dimensionKeyGenerator, the number of its dimensions is less
   * than dimensionKeyGenerator.
   */
  private KeyGenerator sortColumnsGenerator;

  /**
   * list of dimension present in the block
   */
  private List<CarbonDimension> dimensions;

  /**
   * list of dimension present in the block
   */
  private List<CarbonDimension> complexDimensions;

  /**
   * list of measure present in the block
   */
  private List<CarbonMeasure> measures;

  /**
   * cardinality of dimension columns participated in key generator
   */
  private int[] dimColumnsCardinality;

  /**
   * partition index of each dictionary column
   */
  private int[] dimensionPartitions;

  /**
   * cardinality of complex dimension
   */
  private int[] complexDimColumnCardinality;

  /**
   * mapping of dimension ordinal in schema to column chunk index in the data file
   */
  private Map<Integer, Integer> dimensionOrdinalToChunkMapping;

  /**
   * a block can have multiple columns. This will have block index as key
   * and all dimension participated in that block as values
   */
  private Map<Integer, Set<Integer>> blockTodimensionOrdinalMapping;

  /**
   * mapping of measure ordinal in schema to column chunk index in the data file
   */
  private Map<Integer, Integer> measuresOrdinalToChunkMapping;

  /**
   * size of the each dimension column value in a block this can be used when
   * we need to do copy a cell value to create a tuple.for no dictionary
   * column this value will be -1. for dictionary column we size of the value
   * will be fixed.
   */
  private int[] eachDimColumnValueSize;

  /**
   * size of the each dimension column value in a block this can be used when
   * we need to do copy a cell value to create a tuple.for no dictionary
   * column this value will be -1. for dictionary column we size of the value
   * will be fixed.
   */
  private int[] eachComplexDimColumnValueSize;

  /**
   * below mapping will have mapping of the column group to dimensions ordinal
   * for example if 3 dimension present in the columngroupid 0 and its ordinal in
   * 2,3,4 then map will contain 0,{2,3,4}
   */
  private Map<Integer, KeyGenerator> columnGroupAndItsKeygenartor;

  /**
   * column group key generator dimension index will not be same as dimension ordinal
   * This will have mapping with ordinal and keygenerator or mdkey index
   */
  private Map<Integer, Map<Integer, Integer>> columnGroupOrdinalToMdkeymapping;

  /**
   * this will be used to split the fixed length key
   * this will all the information about how key was created
   * and how to split the key based on group
   */
  private ColumnarSplitter fixedLengthKeySplitter;

  /**
   * to store the number of no dictionary dimension
   * this will be used during query execution for creating
   * start and end key. Purpose of storing this value here is
   * so during query execution no need to calculate every time
   */
  private int numberOfNoDictionaryDimension;

  /**
   * column group model
   */
  private ColumnGroupModel colGroupModel;

  private int numberOfSortColumns = 0;

  private int numberOfNoDictSortColumns = 0;

  private int lastDimensionColOrdinal;

  public SegmentProperties(List<ColumnSchema> columnsInTable, int[] columnCardinality) {
    dimensions = new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    complexDimensions =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    measures = new ArrayList<CarbonMeasure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    fillDimensionAndMeasureDetails(columnsInTable, columnCardinality);
    dimensionOrdinalToChunkMapping =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    blockTodimensionOrdinalMapping =
        new HashMap<Integer, Set<Integer>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    measuresOrdinalToChunkMapping =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    intialiseColGroups();
    fillOrdinalToBlockMappingForDimension();
    fillOrdinalToChunkIndexMappingForMeasureColumns();
    fillColumnGroupAndItsCardinality(columnCardinality);
    fillKeyGeneratorDetails();
  }

  /**
   * it fills column groups
   * e.g {{1},{2,3,4},{5},{6},{7,8,9}}
   *
   */
  private void intialiseColGroups() {
    List<List<Integer>> colGrpList = new ArrayList<List<Integer>>();
    List<Integer> group = new ArrayList<Integer>();
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (!dimension.hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      group.add(dimension.getOrdinal());
      if (i < dimensions.size() - 1) {
        int currGroupOrdinal = dimension.columnGroupId();
        int nextGroupOrdinal = dimensions.get(i + 1).columnGroupId();
        if (!(currGroupOrdinal == nextGroupOrdinal && currGroupOrdinal != -1)) {
          colGrpList.add(group);
          group = new ArrayList<Integer>();
        }
      } else {
        colGrpList.add(group);
      }

    }
    int[][] colGroups = new int[colGrpList.size()][];
    for (int i = 0; i < colGroups.length; i++) {
      colGroups[i] = new int[colGrpList.get(i).size()];
      for (int j = 0; j < colGroups[i].length; j++) {
        colGroups[i][j] = colGrpList.get(i).get(j);
      }
    }
    this.colGroupModel = CarbonUtil.getColGroupModel(colGroups);
  }

  /**
   * below method is to fill the dimension and its mapping to file blocks all
   * the column will point to same column group
   */
  private void fillOrdinalToBlockMappingForDimension() {
    int blockOrdinal = -1;
    CarbonDimension dimension = null;
    int index = 0;
    int prvcolumnGroupId = -1;
    while (index < dimensions.size()) {
      dimension = dimensions.get(index);
      // if column id is same as previous one then block index will be
      // same
      if (dimension.isColumnar() || dimension.columnGroupId() != prvcolumnGroupId) {
        blockOrdinal++;
      }
      dimensionOrdinalToChunkMapping.put(dimension.getOrdinal(), blockOrdinal);
      prvcolumnGroupId = dimension.columnGroupId();
      index++;
    }
    index = 0;
    // complex dimension will be stored at last
    while (index < complexDimensions.size()) {
      dimension = complexDimensions.get(index);
      dimensionOrdinalToChunkMapping.put(dimension.getOrdinal(), ++blockOrdinal);
      blockOrdinal = fillComplexDimensionChildBlockIndex(blockOrdinal, dimension);
      index++;
    }
    fillBlockToDimensionOrdinalMapping();
  }

  /**
   *
   */
  private void fillBlockToDimensionOrdinalMapping() {
    Set<Entry<Integer, Integer>> blocks = dimensionOrdinalToChunkMapping.entrySet();
    Iterator<Entry<Integer, Integer>> blockItr = blocks.iterator();
    while (blockItr.hasNext()) {
      Entry<Integer, Integer> block = blockItr.next();
      Set<Integer> dimensionOrdinals = blockTodimensionOrdinalMapping.get(block.getValue());
      if (dimensionOrdinals == null) {
        dimensionOrdinals = new HashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        blockTodimensionOrdinalMapping.put(block.getValue(), dimensionOrdinals);
      }
      dimensionOrdinals.add(block.getKey());
    }
  }

  /**
   * Below method will be used to add the complex dimension child
   * block index.It is a recursive method which will be get the children
   * add the block index
   *
   * @param blockOrdinal start block ordinal
   * @param dimension    parent dimension
   * @return last block index
   */
  private int fillComplexDimensionChildBlockIndex(int blockOrdinal, CarbonDimension dimension) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      dimensionOrdinalToChunkMapping
          .put(dimension.getListOfChildDimensions().get(i).getOrdinal(), ++blockOrdinal);
      if (dimension.getListOfChildDimensions().get(i).getNumberOfChild() > 0) {
        blockOrdinal = fillComplexDimensionChildBlockIndex(blockOrdinal,
            dimension.getListOfChildDimensions().get(i));
      }
    }
    return blockOrdinal;
  }

  /**
   * Below method will be used to fill the mapping
   * of measure ordinal to its block index mapping in
   * file
   */
  private void fillOrdinalToChunkIndexMappingForMeasureColumns() {
    int blockOrdinal = 0;
    int index = 0;
    while (index < measures.size()) {
      measuresOrdinalToChunkMapping.put(measures.get(index).getOrdinal(), blockOrdinal);
      blockOrdinal++;
      index++;
    }
  }

  /**
   * below method will fill dimension and measure detail of the block.
   *
   * @param columnsInTable
   * @param columnCardinality
   */
  private void fillDimensionAndMeasureDetails(List<ColumnSchema> columnsInTable,
      int[] columnCardinality) {
    ColumnSchema columnSchema = null;
    // ordinal will be required to read the data from file block
    int dimensonOrdinal = 0;
    int measureOrdinal = -1;
    // table ordinal is actually a schema ordinal this is required as
    // cardinality array
    // which is stored in segment info contains -1 if that particular column
    // is n
    int tableOrdinal = -1;
    // creating a list as we do not know how many dimension not participated
    // in the mdkey
    List<Integer> cardinalityIndexForNormalDimensionColumn =
        new ArrayList<Integer>(columnsInTable.size());
    // creating a list as we do not know how many dimension not participated
    // in the mdkey
    List<Integer> cardinalityIndexForComplexDimensionColumn =
        new ArrayList<Integer>(columnsInTable.size());
    boolean isComplexDimensionStarted = false;
    CarbonDimension carbonDimension = null;
    // to store the position of dimension in surrogate key array which is
    // participating in mdkey
    int keyOrdinal = 0;
    int previousColumnGroup = -1;
    // to store the ordinal of the column group ordinal
    int columnGroupOrdinal = 0;
    int counter = 0;
    int complexTypeOrdinal = -1;
    while (counter < columnsInTable.size()) {
      columnSchema = columnsInTable.get(counter);
      if (columnSchema.isDimensionColumn()) {
        tableOrdinal++;
        // not adding the cardinality of the non dictionary
        // column as it was not the part of mdkey
        if (CarbonUtil.hasEncoding(columnSchema.getEncodingList(), Encoding.DICTIONARY)
            && !isComplexDimensionStarted && columnSchema.getNumberOfChild() == 0) {
          cardinalityIndexForNormalDimensionColumn.add(tableOrdinal);
          if (columnSchema.isSortColumn()) {
            this.numberOfSortColumns++;
          }
          if (columnSchema.isColumnar()) {
            // if it is a columnar dimension participated in mdkey then added
            // key ordinal and dimension ordinal
            carbonDimension =
                new CarbonDimension(columnSchema, dimensonOrdinal++, keyOrdinal++, -1, -1);
          } else {
            // if not columnnar then it is a column group dimension

            // below code to handle first dimension of the column group
            // in this case ordinal of the column group will be 0
            if (previousColumnGroup != columnSchema.getColumnGroupId()) {
              columnGroupOrdinal = 0;
              carbonDimension = new CarbonDimension(columnSchema, dimensonOrdinal++, keyOrdinal++,
                  columnGroupOrdinal++, -1);
            }
            // if previous dimension  column group id is same as current then
            // then its belongs to same row group
            else {
              carbonDimension = new CarbonDimension(columnSchema, dimensonOrdinal++, keyOrdinal++,
                  columnGroupOrdinal++, -1);
            }
            previousColumnGroup = columnSchema.getColumnGroupId();
          }
        }
        // as complex type will be stored at last so once complex type started all the dimension
        // will be added to complex type
        else if (isComplexDimensionStarted || columnSchema.getDataType().isComplexType()) {
          cardinalityIndexForComplexDimensionColumn.add(tableOrdinal);
          carbonDimension =
              new CarbonDimension(columnSchema, dimensonOrdinal++, -1, -1, ++complexTypeOrdinal);
          carbonDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          complexDimensions.add(carbonDimension);
          isComplexDimensionStarted = true;
          int previouseOrdinal = dimensonOrdinal;
          dimensonOrdinal =
              readAllComplexTypeChildren(dimensonOrdinal, columnSchema.getNumberOfChild(),
                  columnsInTable, carbonDimension, complexTypeOrdinal);
          int numberOfChildrenDimensionAdded = dimensonOrdinal - previouseOrdinal;
          for (int i = 0; i < numberOfChildrenDimensionAdded; i++) {
            cardinalityIndexForComplexDimensionColumn.add(++tableOrdinal);
          }
          counter = dimensonOrdinal;
          complexTypeOrdinal = assignComplexOrdinal(carbonDimension, complexTypeOrdinal);
          continue;
        } else {
          // for no dictionary dimension
          carbonDimension = new CarbonDimension(columnSchema, dimensonOrdinal++, -1, -1, -1);
          numberOfNoDictionaryDimension++;
          if (columnSchema.isSortColumn()) {
            this.numberOfSortColumns++;
            this.numberOfNoDictSortColumns++;
          }
        }
        dimensions.add(carbonDimension);
      } else {
        measures.add(new CarbonMeasure(columnSchema, ++measureOrdinal));
      }
      counter++;
    }
    lastDimensionColOrdinal = dimensonOrdinal;
    dimColumnsCardinality = new int[cardinalityIndexForNormalDimensionColumn.size()];
    complexDimColumnCardinality = new int[cardinalityIndexForComplexDimensionColumn.size()];
    int index = 0;
    // filling the cardinality of the dimension column to create the key
    // generator
    for (Integer cardinalityArrayIndex : cardinalityIndexForNormalDimensionColumn) {
      dimColumnsCardinality[index++] = columnCardinality[cardinalityArrayIndex];
    }
    index = 0;
    // filling the cardinality of the complex dimension column to create the
    // key generator
    for (Integer cardinalityArrayIndex : cardinalityIndexForComplexDimensionColumn) {
      complexDimColumnCardinality[index++] = columnCardinality[cardinalityArrayIndex];
    }
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   *
   * @param dimensionOrdinal
   * @param childCount
   * @param listOfColumns
   * @param parentDimension
   * @return
   */
  private int readAllComplexTypeChildren(int dimensionOrdinal, int childCount,
      List<ColumnSchema> listOfColumns, CarbonDimension parentDimension,
      int complexDimensionOrdinal) {
    for (int i = 0; i < childCount; i++) {
      ColumnSchema columnSchema = listOfColumns.get(dimensionOrdinal);
      if (columnSchema.isDimensionColumn()) {
        if (columnSchema.getNumberOfChild() > 0) {
          CarbonDimension complexDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1,
                  complexDimensionOrdinal++);
          complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          parentDimension.getListOfChildDimensions().add(complexDimension);
          dimensionOrdinal =
              readAllComplexTypeChildren(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  listOfColumns, complexDimension, complexDimensionOrdinal);
        } else {
          parentDimension.getListOfChildDimensions().add(
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1,
                  complexDimensionOrdinal++));
        }
      }
    }
    return dimensionOrdinal;
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   */
  private int assignComplexOrdinal(CarbonDimension parentDimension, int complexDimensionOrdinal) {
    for (int i = 0; i < parentDimension.getNumberOfChild(); i++) {
      CarbonDimension dimension = parentDimension.getListOfChildDimensions().get(i);
      if (dimension.getNumberOfChild() > 0) {
        dimension.setComplexTypeOridnal(++complexDimensionOrdinal);
        complexDimensionOrdinal = assignComplexOrdinal(dimension, complexDimensionOrdinal);
      } else {
        parentDimension.getListOfChildDimensions().get(i)
            .setComplexTypeOridnal(++complexDimensionOrdinal);
      }
    }
    return complexDimensionOrdinal;
  }

  /**
   * Below method will fill the key generator detail of both the type of key
   * generator. This will be required for during both query execution and data
   * loading.
   */
  private void fillKeyGeneratorDetails() {
    // create a dimension partitioner list
    // this list will contain information about how dimension value are
    // stored
    // it is stored in group or individually
    List<Integer> dimensionPartitionList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<Boolean> isDictionaryColumn =
        new ArrayList<Boolean>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    int prvcolumnGroupId = -1;
    int counter = 0;
    while (counter < dimensions.size()) {
      CarbonDimension carbonDimension = dimensions.get(counter);
      // if dimension is not a part of mdkey then no need to add
      if (!carbonDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        isDictionaryColumn.add(false);
        counter++;
        continue;
      }
      // columnar column is stored individually
      // so add one
      if (carbonDimension.isColumnar()) {
        dimensionPartitionList.add(1);
        isDictionaryColumn.add(true);
      }
      // if in a group then need to add how many columns a selected in
      // group
      if (!carbonDimension.isColumnar() && carbonDimension.columnGroupId() == prvcolumnGroupId) {
        // incrementing the previous value of the list as it is in same column group
        dimensionPartitionList.set(dimensionPartitionList.size() - 1,
            dimensionPartitionList.get(dimensionPartitionList.size() - 1) + 1);
      } else if (!carbonDimension.isColumnar()) {
        dimensionPartitionList.add(1);
        isDictionaryColumn.add(true);
      }
      prvcolumnGroupId = carbonDimension.columnGroupId();
      counter++;
    }
    // get the partitioner
    dimensionPartitions = ArrayUtils
        .toPrimitive(dimensionPartitionList.toArray(new Integer[dimensionPartitionList.size()]));
    // get the bit length of each column
    int[] bitLength = CarbonUtil.getDimensionBitLength(dimColumnsCardinality, dimensionPartitions);
    // create a key generator
    this.dimensionKeyGenerator = new MultiDimKeyVarLengthGenerator(bitLength);
    if (this.getNumberOfDictSortColumns() == bitLength.length) {
      this.sortColumnsGenerator = this.dimensionKeyGenerator;
    } else {
      int numberOfDictSortColumns = this.getNumberOfDictSortColumns();
      int [] sortColumnBitLength = new int[numberOfDictSortColumns];
      System.arraycopy(bitLength, 0, sortColumnBitLength, 0, numberOfDictSortColumns);
      this.sortColumnsGenerator = new MultiDimKeyVarLengthGenerator(sortColumnBitLength);
    }
    this.fixedLengthKeySplitter =
        new MultiDimKeyVarLengthVariableSplitGenerator(bitLength, dimensionPartitions);
    // get the size of each value in file block
    int[] dictionayDimColumnValueSize = fixedLengthKeySplitter.getBlockKeySize();
    int index = -1;
    this.eachDimColumnValueSize = new int[isDictionaryColumn.size()];
    for (int i = 0; i < eachDimColumnValueSize.length; i++) {
      if (!isDictionaryColumn.get(i)) {
        eachDimColumnValueSize[i] = -1;
        continue;
      }
      eachDimColumnValueSize[i] = dictionayDimColumnValueSize[++index];
    }
    if (complexDimensions.size() > 0) {
      int[] complexDimesionParition = new int[complexDimColumnCardinality.length];
      // as complex dimension will be stored in column format add one
      Arrays.fill(complexDimesionParition, 1);
      bitLength =
          CarbonUtil.getDimensionBitLength(complexDimColumnCardinality, complexDimesionParition);
      for (int i = 0; i < bitLength.length; i++) {
        if (complexDimColumnCardinality[i] == 0) {
          bitLength[i] = 64;
        }
      }
      ColumnarSplitter keySplitter =
          new MultiDimKeyVarLengthVariableSplitGenerator(bitLength, complexDimesionParition);
      eachComplexDimColumnValueSize = keySplitter.getBlockKeySize();
    } else {
      eachComplexDimColumnValueSize = new int[0];
    }
  }

  /**
   * Below method will be used to create a mapping of column group and its column cardinality this
   * mapping will have column group id to cardinality of the dimension present in
   * the column group.This mapping will be used during query execution, to create
   * a mask key for the column group dimension which will be used in aggregation
   * and filter query as column group dimension will be stored at the bit level
   */
  private void fillColumnGroupAndItsCardinality(int[] cardinality) {
    // mapping of the column group and its ordinal
    Map<Integer, List<Integer>> columnGroupAndOrdinalMapping =
        new HashMap<Integer, List<Integer>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // to store a column group
    List<Integer> currentColumnGroup = null;
    // current index
    int index = 0;
    // previous column group to check all the column of column id has bee selected
    int prvColumnGroupId = -1;
    while (index < dimensions.size()) {
      // if dimension group id is not zero and it is same as the previous
      // column id
      // then we need to add ordinal of that column as it belongs to same
      // column group
      if (!dimensions.get(index).isColumnar()
          && dimensions.get(index).columnGroupId() == prvColumnGroupId
          && null != currentColumnGroup) {
        currentColumnGroup.add(index);
      }
      // if column is not a columnar then new column group has come
      // so we need to create a list of new column id group and add the
      // ordinal
      else if (!dimensions.get(index).isColumnar()) {
        currentColumnGroup = new ArrayList<Integer>();
        columnGroupAndOrdinalMapping.put(dimensions.get(index).columnGroupId(), currentColumnGroup);
        currentColumnGroup.add(index);
      }
      // update the column id every time,this is required to group the
      // columns
      // of the same column group
      prvColumnGroupId = dimensions.get(index).columnGroupId();
      index++;
    }
    // Initializing the map
    this.columnGroupAndItsKeygenartor =
        new HashMap<Integer, KeyGenerator>(columnGroupAndOrdinalMapping.size());
    this.columnGroupOrdinalToMdkeymapping = new HashMap<>(columnGroupAndOrdinalMapping.size());
    int[] columnGroupCardinality = null;
    index = 0;
    Iterator<Entry<Integer, List<Integer>>> iterator =
        columnGroupAndOrdinalMapping.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Integer, List<Integer>> next = iterator.next();
      List<Integer> currentGroupOrdinal = next.getValue();
      Map<Integer, Integer> colGrpOrdinalMdkeyMapping = new HashMap<>(currentGroupOrdinal.size());
      // create the cardinality array
      columnGroupCardinality = new int[currentGroupOrdinal.size()];
      for (int i = 0; i < columnGroupCardinality.length; i++) {
        // fill the cardinality
        columnGroupCardinality[i] = cardinality[currentGroupOrdinal.get(i)];
        colGrpOrdinalMdkeyMapping.put(currentGroupOrdinal.get(i), i);
      }
      this.columnGroupAndItsKeygenartor.put(next.getKey(), new MultiDimKeyVarLengthGenerator(
          CarbonUtil.getDimensionBitLength(columnGroupCardinality,
              new int[] { columnGroupCardinality.length })));
      this.columnGroupOrdinalToMdkeymapping.put(next.getKey(), colGrpOrdinalMdkeyMapping);
    }
  }

  /**
   * Below method is to get the value of each dimension column. As this method
   * will be used only once so we can merge both the dimension and complex
   * dimension array. Complex dimension will be store at last so first copy
   * the normal dimension the copy the complex dimension size. If we store
   * this value as a class variable unnecessarily we will waste some space
   *
   * @return each dimension value size
   */
  public int[] getDimensionColumnsValueSize() {
    int[] dimensionValueSize =
        new int[eachDimColumnValueSize.length + eachComplexDimColumnValueSize.length];
    System.arraycopy(
        eachDimColumnValueSize, 0, dimensionValueSize, 0, eachDimColumnValueSize.length);
    System.arraycopy(eachComplexDimColumnValueSize, 0, dimensionValueSize,
        eachDimColumnValueSize.length, eachComplexDimColumnValueSize.length);
    return dimensionValueSize;
  }

  public int[] getColumnsValueSize() {
    int[] dimensionValueSize =
        new int[eachDimColumnValueSize.length + eachComplexDimColumnValueSize.length + measures
            .size()];
    System
        .arraycopy(eachDimColumnValueSize, 0, dimensionValueSize, 0, eachDimColumnValueSize.length);
    System.arraycopy(eachComplexDimColumnValueSize, 0, dimensionValueSize,
        eachDimColumnValueSize.length, eachComplexDimColumnValueSize.length);
    int k = eachDimColumnValueSize.length + eachComplexDimColumnValueSize.length;
    for (int i = 0; i < measures.size(); i++) {
      DataType dataType = measures.get(i).getDataType();
      if (DataTypes.isDecimal(dataType)) {
        dimensionValueSize[k++] = -1;
      } else {
        dimensionValueSize[k++] = 8;
      }
    }
    return dimensionValueSize;
  }

  /**
   * @return the dimensionKeyGenerator
   */
  public KeyGenerator getDimensionKeyGenerator() {
    return dimensionKeyGenerator;
  }

  public KeyGenerator getSortColumnsGenerator() {
    return sortColumnsGenerator;
  }

  /**
   * @return the dimensions
   */
  public List<CarbonDimension> getDimensions() {
    return dimensions;
  }

  /**
   * @return the complexDimensions
   */
  public List<CarbonDimension> getComplexDimensions() {
    return complexDimensions;
  }

  /**
   * @return the measures
   */
  public List<CarbonMeasure> getMeasures() {
    return measures;
  }

  /**
   * @return the dimColumnsCardinality
   */
  public int[] getDimColumnsCardinality() {
    return dimColumnsCardinality;
  }

  /**
   * @return
   */
  public int[] getDimensionPartitions() {
    return dimensionPartitions;
  }

  /**
   * @return the complexDimColumnCardinality
   */
  public int[] getComplexDimColumnCardinality() {
    return complexDimColumnCardinality;
  }

  /**
   * @return the dimensionOrdinalToChunkMapping
   */
  public Map<Integer, Integer> getDimensionOrdinalToChunkMapping() {
    return dimensionOrdinalToChunkMapping;
  }

  /**
   * @return the measuresOrdinalToChunkMapping
   */
  public Map<Integer, Integer> getMeasuresOrdinalToChunkMapping() {
    return measuresOrdinalToChunkMapping;
  }

  /**
   * @return the eachDimColumnValueSize
   */
  public int[] getEachDimColumnValueSize() {
    return eachDimColumnValueSize;
  }

  /**
   * @return the eachComplexDimColumnValueSize
   */
  public int[] getEachComplexDimColumnValueSize() {
    return eachComplexDimColumnValueSize;
  }

  /**
   * @return the fixedLengthKeySplitter
   */
  public ColumnarSplitter getFixedLengthKeySplitter() {
    return fixedLengthKeySplitter;
  }

  /**
   * @return the columnGroupAndItsKeygenartor
   */
  public Map<Integer, KeyGenerator> getColumnGroupAndItsKeygenartor() {
    return columnGroupAndItsKeygenartor;
  }

  /**
   * @return the numberOfNoDictionaryDimension
   */
  public int getNumberOfNoDictionaryDimension() {
    return numberOfNoDictionaryDimension;
  }

  /**
   * @return
   */
  public int[][] getColumnGroups() {
    return colGroupModel.getColumnGroup();
  }

  /**
   * @return colGroupModel
   */
  public ColumnGroupModel getColumnGroupModel() {
    return this.colGroupModel;
  }

  /**
   * get mdkey ordinal for given dimension ordinal of given column group
   *
   * @param colGrpId
   * @param ordinal
   * @return mdkeyordinal
   */
  public int getColumnGroupMdKeyOrdinal(int colGrpId, int ordinal) {
    return columnGroupOrdinalToMdkeymapping.get(colGrpId).get(ordinal);
  }

  /**
   * @param blockIndex
   * @return It returns all dimension present in given block index
   */
  public Set<Integer> getDimensionOrdinalForBlock(int blockIndex) {
    return blockTodimensionOrdinalMapping.get(blockIndex);
  }

  /**
   * @return It returns block index to dimension ordinal mapping
   */
  public Map<Integer, Set<Integer>> getBlockTodimensionOrdinalMapping() {
    return blockTodimensionOrdinalMapping;
  }

  /**
   * This method will search a given dimension and return the dimension from current block
   *
   * @param queryDimension
   * @return
   */
  public CarbonDimension getDimensionFromCurrentBlock(CarbonDimension queryDimension) {
    return CarbonUtil.getDimensionFromCurrentBlock(this.dimensions, queryDimension);
  }

  /**
   * This method will search for a given measure in the current block measures list
   *
   * @param columnId
   * @return
   */
  public CarbonMeasure getMeasureFromCurrentBlock(String columnId) {
    return CarbonUtil.getMeasureFromCurrentBlock(this.measures, columnId);
  }

  public int getNumberOfSortColumns() {
    return numberOfSortColumns;
  }

  public int getNumberOfNoDictSortColumns() {
    return numberOfNoDictSortColumns;
  }

  public int getNumberOfDictSortColumns() {
    return this.numberOfSortColumns - this.numberOfNoDictSortColumns;
  }

  public int getLastDimensionColOrdinal() {
    return lastDimensionColOrdinal;
  }
}
