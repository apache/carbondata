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
package org.apache.carbondata.scan.filter;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndex;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BTreeNode;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BTreeNonLeafNode;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.LiteralExpression;
import org.apache.carbondata.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.scan.expression.conditional.InExpression;
import org.apache.carbondata.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.scan.expression.conditional.NotEqualsExpression;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.expression.logical.AndExpression;
import org.apache.carbondata.scan.expression.logical.BinaryLogicalExpression;
import org.apache.carbondata.scan.expression.logical.FalseExpression;
import org.apache.carbondata.scan.expression.logical.OrExpression;
import org.apache.carbondata.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.scan.filter.executer.IncludeFilterExecuterImpl;
import org.apache.carbondata.scan.filter.intf.ExpressionType;
import org.apache.carbondata.scan.filter.resolver.AndFilterResolverImpl;
import org.apache.carbondata.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.el.LessThanOrEqualsOperator;
import org.apache.commons.lang.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class FilterExpressionProcessorTest {

  static FilterExpressionProcessor filterExpressionProcessor;
  static BlockBTreeLeafNode blockBTreeLeafNode;
  static BTreeNode btreeNode;
  static FilterResolverIntf filterResolverIntf;
  static AbstractIndex abstractIndex;
  static AbsoluteTableIdentifier absoluteTableIdentifier;

  @BeforeClass public static void setUp() {
    filterExpressionProcessor = new FilterExpressionProcessor();

    ColumnExpression right = new ColumnExpression("name", DataType.STRING);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("name", DataType.STRING);
    left.setColIndex(0);
    Expression expression = new EqualToExpression(left, right);
    filterResolverIntf = new ConditionalFilterResolverImpl(expression, false, false);

    abstractIndex = new SegmentTaskIndex();

    absoluteTableIdentifier = new AbsoluteTableIdentifier("storepath",
        new CarbonTableIdentifier("databasename", "tablename", "tableid"));
  }

  @Test public void getFilterredBlocksTest() throws QueryExecutionException {
    DataFileFooter dataFileFooter = new DataFileFooter();
    ColumnSchema columnSchema = new ColumnSchema();
    Encoding encoding = Encoding.DICTIONARY;
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(encoding);
    columnSchema.setEncodingList(encodingList);
    columnSchema.setDimensionColumn(true);
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setColumnName("name");
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    dataFileFooter.setColumnInTable(columnSchemas);
    SegmentInfo segmentInfo = new SegmentInfo();
    int[] cardinality = { 2 };
    segmentInfo.setColumnCardinality(cardinality);
    segmentInfo.setNumberOfColumns(1);
    dataFileFooter.setSegmentInfo(segmentInfo);
    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    byte[][] maxVaslues = { { 5 } };
    blockletMinMaxIndex.setMaxValues(maxVaslues);
    byte[][] minValues = { { 1 } };
    blockletMinMaxIndex.setMinValues(minValues);
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);

    byte[] startKey = { 0, 0, 0, 0, 0, 0,0 , 1, 2, 1, 2, 3 };
    byte[] endKey = { 3, 4 };
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex(startKey, endKey);
    blockletIndex.setBtreeIndex(blockletBTreeIndex);
    dataFileFooter.setBlockletIndex(blockletIndex);
    List<DataFileFooter> dataFileFooterList = new ArrayList<>();
    dataFileFooterList.add(dataFileFooter);

    btreeNode = new BTreeNonLeafNode();
    byte[] dictionaryKey = { 1, 2 };
    byte[] noDictionaryKey = { 3, 4 };
    IndexKey indexKey = new IndexKey(dictionaryKey, noDictionaryKey);
    btreeNode.setKey(indexKey);

    BTreeNonLeafNode bTreeNonLeafNode1 = new BTreeNonLeafNode();
    byte[] dictionaryKey2 = { 1, 2 };
    byte[] noDictionaryKey2 = { 3, 4 };
    IndexKey indexKey2 = new IndexKey(dictionaryKey, noDictionaryKey);
    bTreeNonLeafNode1.setKey(indexKey2);
    btreeNode.setNextNode(bTreeNonLeafNode1);

    int[] dataColumnValueSize = { 1, 1 };
    BTreeBuilderInfo bTreeBuilderInfo =
        new BTreeBuilderInfo(dataFileFooterList, dataColumnValueSize);
    BlockBTreeLeafNode blockBTreeLeafNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 0);
    BTreeNode[] bTreeNodes = { blockBTreeLeafNode };
    btreeNode.setChildren(bTreeNodes);

    abstractIndex.buildIndex(
        dataFileFooterList);//to get segmentProperties in line 'tableSegment.getSegmentProperties()'

    new MockUp<FilterUtil>() {

      @SuppressWarnings("unused") @Mock
      public FilterExecuter getFilterExecuterTree(FilterResolverIntf filterExpressionResolverTree,
          SegmentProperties segmentProperties,
          Map<Integer, GenericQueryType> complexDimensionInfoMap) {
        ColumnSchema columnSchema = new ColumnSchema();
        Encoding encoding = Encoding.DICTIONARY;
        List<Encoding> encodingList = new ArrayList<>();
        encodingList.add(encoding);
        columnSchema.setEncodingList(encodingList);
        columnSchema.setDimensionColumn(true);
        columnSchema.setDataType(DataType.ARRAY);
        columnSchema.setColumnName("name");
        List<ColumnSchema> columnSchemaList = new ArrayList<>();
        columnSchemaList.add(columnSchema);
        int[] cardinality = { 1, 2, 3, 4, 5 };
        SegmentProperties segmentProperties1 = new SegmentProperties(columnSchemaList, cardinality);
        List<Integer> integerList = new ArrayList<>();
        integerList.add(new Integer("1"));

        DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
        dimColumnFilterInfo.setFilterList(integerList);
        CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 0, 0, 0, 0);
        DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
        dimColumnResolvedFilterInfo.setDimension(carbonDimension);
        dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);
        dimColumnResolvedFilterInfo.setColumnIndex(0);
        return new IncludeFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties1);
      }
    };

    new MockUp<CarbonDimension>() {
      @SuppressWarnings("unused") @Mock public boolean hasEncoding(Encoding encoding) {
        return true;
      }
    };

    new MockUp<ArrayUtils>() {
      @SuppressWarnings("unused") @Mock public int[] toPrimitive(final Integer[] array) {
        int[] primitiveValue = { 0, 1 };
        return primitiveValue;
      }
    };

    List<DataRefNode> dataRefNodeList = filterExpressionProcessor
        .getFilterredBlocks(btreeNode, filterResolverIntf, abstractIndex, absoluteTableIdentifier);
    assertEquals(dataRefNodeList.size(), 1);
    byte[][] expectedMaxValue = { { 5 } };
    assertThat(dataRefNodeList.get(0).getColumnsMaxValue(), is(expectedMaxValue));
    byte[][] expectedMinValue = { { 1 } };
    assertThat(dataRefNodeList.get(0).getColumnsMinValue(), is(expectedMinValue));
  }

  @Test public void getFilterredBlocksTestWithNullStartAndEndKey() throws QueryExecutionException {
    DataFileFooter dataFileFooter = new DataFileFooter();
    ColumnSchema columnSchema = new ColumnSchema();
    Encoding encoding = Encoding.DICTIONARY;
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(encoding);
    columnSchema.setEncodingList(encodingList);
    columnSchema.setDimensionColumn(true);
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setColumnName("name");
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    dataFileFooter.setColumnInTable(columnSchemas);
    SegmentInfo segmentInfo = new SegmentInfo();
    int[] cardinality = { 1 };
    segmentInfo.setColumnCardinality(cardinality);
    segmentInfo.setNumberOfColumns(1);
    dataFileFooter.setSegmentInfo(segmentInfo);
    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    byte[][] maxVaslues = { { 5 } };
    blockletMinMaxIndex.setMaxValues(maxVaslues);
    byte[][] minValues = { { 1 } };
    blockletMinMaxIndex.setMinValues(minValues);
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);

    byte[] startKey = { 0, 0, 0, 0, 0, 0,0 , 1, 2, 1, 2, 3 };
    byte[] endKey = { 3, 4 };
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex(startKey, endKey);
    blockletIndex.setBtreeIndex(blockletBTreeIndex);
    dataFileFooter.setBlockletIndex(blockletIndex);

    List<DataFileFooter> dataFileFooterList = new ArrayList<>();
    dataFileFooterList.add(dataFileFooter);

    int[] dataColumnValueSize = { 1, 1 };
    final BTreeBuilderInfo bTreeBuilderInfo =
        new BTreeBuilderInfo(dataFileFooterList, dataColumnValueSize);

    blockBTreeLeafNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    BlockBTreeLeafNode bTreeNonLeafNode1 = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 0);
    blockBTreeLeafNode.setNextNode(bTreeNonLeafNode1);

    BlockBTreeLeafNode blockBTreeLeafNode1 = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 0);
    BTreeNode[] bTreeNodes = { blockBTreeLeafNode1 };

    abstractIndex.buildIndex(
        dataFileFooterList);//to get segmentProperties in line 'tableSegment.getSegmentProperties()'

    new MockUp<FilterUtil>() {

      @SuppressWarnings("unused") @Mock
      public FilterExecuter getFilterExecuterTree(FilterResolverIntf filterExpressionResolverTree,
          SegmentProperties segmentProperties,
          Map<Integer, GenericQueryType> complexDimensionInfoMap) {
        ColumnSchema columnSchema = new ColumnSchema();
        Encoding encoding = Encoding.DICTIONARY;
        List<Encoding> encodingList = new ArrayList<>();
        encodingList.add(encoding);
        columnSchema.setEncodingList(encodingList);
        columnSchema.setDimensionColumn(true);
        columnSchema.setDataType(DataType.ARRAY);
        columnSchema.setColumnName("name");
        List<ColumnSchema> columnSchemaList = new ArrayList<>();
        columnSchemaList.add(columnSchema);
        int[] cardinality = { 1, 2, 1, 3, 5 };
        SegmentProperties segmentProperties1 = new SegmentProperties(columnSchemaList, cardinality);
        List<Integer> integerList = new ArrayList<>();
        integerList.add(new Integer("1"));

        DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
        dimColumnFilterInfo.setFilterList(integerList);
        CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 0, 0, 0, 0);
        DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
        dimColumnResolvedFilterInfo.setDimension(carbonDimension);
        dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);
        dimColumnResolvedFilterInfo.setColumnIndex(0);
        return new IncludeFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties1);
      }

      @SuppressWarnings("unused") @Mock
      public void traverseResolverTreeAndGetStartAndEndKey(SegmentProperties segmentProperties,
          AbsoluteTableIdentifier tableIdentifier, FilterResolverIntf filterResolver,
          List<IndexKey> listOfStartEndKeys) throws QueryExecutionException {
        listOfStartEndKeys.add(null);
        listOfStartEndKeys.add(null);
      }
    };

    new MockUp<CarbonDimension>() {
      @SuppressWarnings("unused") @Mock public boolean hasEncoding(Encoding encoding) {
        return true;
      }
    };

    new MockUp<ArrayUtils>() {
      @SuppressWarnings("unused") @Mock public int[] toPrimitive(final Integer[] array) {
        int[] primitiveValue = { 0, 1 };
        return primitiveValue;
      }
    };
    new MockUp<BTreeDataRefNodeFinder>() {

      BTreeNode firstDataNode = (BTreeNode) getFirstDataBlock();
      BTreeNode lastDataNode = (BTreeNode) getLastDataNode();

      @Mock public DataRefNode findFirstDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
        System.out.println("Max Value : " + firstDataNode.getColumnsMaxValue()[0][0]);
        firstDataNode.setNextNode(lastDataNode);
        return firstDataNode;
      }

      @Mock public DataRefNode findLastDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
        return lastDataNode;
      }
    };

    new MockUp<IncludeFilterExecuterImpl>() {
      @Mock public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
        BitSet bitSet = new BitSet();
        bitSet.set(0);
        return bitSet;
      }
    };

    List<DataRefNode> dataRefNodeList = filterExpressionProcessor
        .getFilterredBlocks(blockBTreeLeafNode, filterResolverIntf, abstractIndex,
            absoluteTableIdentifier);
    assertEquals(dataRefNodeList.size(), 2);
    byte[][] expectedMaxValue = { { 6 } };
    assertThat(dataRefNodeList.get(0).getColumnsMaxValue(), is(expectedMaxValue));
    byte[][] expectedMinValue = { { 2 } };
    assertThat(dataRefNodeList.get(0).getColumnsMinValue(), is(expectedMinValue));
  }

  @Test
  public void getFilterResolverTestForAnd()throws FilterUnsupportedException {
    LiteralExpression literalExpressionLeft = new LiteralExpression(true,DataType.BOOLEAN);
    LiteralExpression literalExpressionRight = new LiteralExpression(false,DataType.BOOLEAN);
    AndExpression andExpression = new AndExpression(literalExpressionLeft,literalExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    String expectedResult = "And(LiteralExpression(true),LiteralExpression(false))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(andExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  @Test
  public void getFilterResolverTestForOR()throws FilterUnsupportedException {
    LiteralExpression literalExpressionLeft = new LiteralExpression(true,DataType.BOOLEAN);
    LiteralExpression literalExpressionRight = new LiteralExpression(false,DataType.BOOLEAN);
    OrExpression orExpression = new OrExpression(literalExpressionLeft,literalExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    String expectedResult = "Or(LiteralExpression(true),LiteralExpression(false))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(orExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  @Test
  public void getFilterResolverTestForIn()throws FilterUnsupportedException {
    LiteralExpression literalExpressionLeft = new LiteralExpression(true,DataType.BOOLEAN);
    LiteralExpression literalExpressionRight = new LiteralExpression(false,DataType.BOOLEAN);
    InExpression inExpression = new InExpression(literalExpressionLeft,literalExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    String expectedResult = "IN(LiteralExpression(true),LiteralExpression(false))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(inExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  @Test
  public void getFilterResolverTestForLessThanEqualTo()throws FilterUnsupportedException {
    LiteralExpression literalExpressionLeft = new LiteralExpression(15,DataType.INT);
    LiteralExpression literalExpressionRight = new LiteralExpression(25,DataType.INT);
    LessThanEqualToExpression lessThanEqualToExpression = new LessThanEqualToExpression(literalExpressionLeft,literalExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    String expectedResult = "LessThanEqualTo(LiteralExpression(15),LiteralExpression(25))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(lessThanEqualToExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  @Test
  public void getFilterResolverTestForNotEquals()throws FilterUnsupportedException {
    LiteralExpression literalExpressionLeft = new LiteralExpression(15,DataType.INT);
    LiteralExpression literalExpressionRight = new LiteralExpression(25,DataType.INT);
    NotEqualsExpression notEqualsExpression = new NotEqualsExpression(literalExpressionLeft,literalExpressionRight,true);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    String expectedResult = "NotEquals(LiteralExpression(15),LiteralExpression(25))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(notEqualsExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  @Test
  public void getFilterResolverBasedOnExpressionTypeTestForNotEquals()throws FilterUnsupportedException {
    ColumnExpression columnExpressionLet = new ColumnExpression("YourName",DataType.STRING);
    columnExpressionLet.setColIndex(0);
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("yourname");
    CarbonColumn carbonColumn = new CarbonColumn(columnSchema,0,0);
    columnExpressionLet.setCarbonColumn(carbonColumn);
    ColumnExpression columnExpressionRight = new ColumnExpression("YourAddress",DataType.STRING);
    columnExpressionRight.setColIndex(0);
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("yourAddress");
    CarbonColumn carbonColumn1 = new CarbonColumn(columnSchema1,0,0);
    columnExpressionRight.setCarbonColumn(carbonColumn1);
    NotEqualsExpression notEqualsExpression = new NotEqualsExpression(columnExpressionLet,columnExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    new MockUp<BinaryLogicalExpression>(){
      @Mock
      public boolean isSingleDimension() {
        return true;
      }
    };
    new MockUp<ColumnExpression>(){
      @Mock
      public boolean isDimension() {
        return true;
      }
    };
    String expectedResult = "NotEquals(ColumnExpression(YourName),ColumnExpression(YourAddress))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(notEqualsExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  @Test
  public void getFilterResolverBasedOnExpressionTypeTestForEquals()throws FilterUnsupportedException {
    ColumnExpression columnExpressionLet = new ColumnExpression("YourName",DataType.STRING);
    columnExpressionLet.setColIndex(0);
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("yourname");
    CarbonColumn carbonColumn = new CarbonColumn(columnSchema,0,0);
    columnExpressionLet.setCarbonColumn(carbonColumn);
    ColumnExpression columnExpressionRight = new ColumnExpression("YourAddress",DataType.STRING);
    columnExpressionRight.setColIndex(0);
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("yourAddress");
    CarbonColumn carbonColumn1 = new CarbonColumn(columnSchema1,0,0);
    columnExpressionRight.setCarbonColumn(carbonColumn1);
    EqualToExpression equalToExpression = new EqualToExpression(columnExpressionLet,columnExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    new MockUp<BinaryLogicalExpression>(){
      @Mock
      public boolean isSingleDimension() {
        return true;
      }
    };
    new MockUp<ColumnExpression>(){
      @Mock
      public boolean isDimension() {
        return true;
      }
    };
    String expectedResult = "EqualTo(ColumnExpression(YourName),ColumnExpression(YourAddress))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(equalToExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }


  @Test
  public void getFilterResolverTestForFalse()throws FilterUnsupportedException {
    LiteralExpression literalExpression = new LiteralExpression(false,DataType.BOOLEAN);
    FalseExpression falseExpression = new FalseExpression(literalExpression);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(falseExpression,absoluteTableIdentifier);
    assertNotNull(filterResolverIntf);
  }

  @Test
  public void getFilterResolverBasedOnExpressionTypeTestForUnknownFilterExpressionType()throws FilterUnsupportedException {
    ColumnExpression columnExpressionLet = new ColumnExpression("YourName",DataType.STRING);
    columnExpressionLet.setColIndex(0);
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnName("yourname");
    CarbonColumn carbonColumn = new CarbonColumn(columnSchema,0,0);
    columnExpressionLet.setCarbonColumn(carbonColumn);
    ColumnExpression columnExpressionRight = new ColumnExpression("YourAddress",DataType.STRING);
    columnExpressionRight.setColIndex(0);
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("yourAddress");
    CarbonColumn carbonColumn1 = new CarbonColumn(columnSchema1,0,0);
    columnExpressionRight.setCarbonColumn(carbonColumn1);
    LessThanExpression lessThanExpression = new LessThanExpression(columnExpressionLet,columnExpressionRight);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("DatabaseName","TableName","tableId");
    AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("StorePath",carbonTableIdentifier);
    new MockUp<BinaryLogicalExpression>(){
      @Mock
      public boolean isSingleDimension() {
        return true;
      }
    };
    new MockUp<ColumnExpression>(){
      @Mock
      public boolean isDimension() {
        return true;
      }
    };
    new MockUp<LessThanExpression>(){
      @Mock
      public ExpressionType getFilterExpressionType(){
        return ExpressionType.UNKNOWN;
      }
    };
    String expectedResult = "LessThan(ColumnExpression(YourName),ColumnExpression(YourAddress))";
    FilterResolverIntf filterResolverIntf = filterExpressionProcessor.getFilterResolver(lessThanExpression,absoluteTableIdentifier);
    assertThat(filterResolverIntf.getFilterExpression().getString(),is(equalTo(expectedResult)));
  }

  private DataRefNode getFirstDataBlock() {

    BlockBTreeLeafNode btreeNode;
    DataFileFooter dataFileFooter = new DataFileFooter();
    ColumnSchema columnSchema = new ColumnSchema();
    Encoding encoding = Encoding.DICTIONARY;
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(encoding);
    columnSchema.setEncodingList(encodingList);
    columnSchema.setDimensionColumn(true);
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setColumnName("name");
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    dataFileFooter.setColumnInTable(columnSchemas);
    SegmentInfo segmentInfo = new SegmentInfo();
    int[] cardinality = { 1 };
    segmentInfo.setColumnCardinality(cardinality);
    segmentInfo.setNumberOfColumns(1);
    dataFileFooter.setSegmentInfo(segmentInfo);
    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    byte[][] maxVaslues = { { 6 } };
    blockletMinMaxIndex.setMaxValues(maxVaslues);
    byte[][] minValues = { { 2 } };
    blockletMinMaxIndex.setMinValues(minValues);
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);

    byte[] startKey = { 0, 0, 0, 0, 0, 0,0 , 1, 3, 1, 2, 3 };
    byte[] endKey = { 3, 4 };
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex(startKey, endKey);
    blockletIndex.setBtreeIndex(blockletBTreeIndex);
    dataFileFooter.setBlockletIndex(blockletIndex);
    List<DataFileFooter> dataFileFooterList = new ArrayList<>();
    dataFileFooterList.add(dataFileFooter);

    int[] dataColumnValueSize = { 1, 1 };
    final BTreeBuilderInfo bTreeBuilderInfo =
        new BTreeBuilderInfo(dataFileFooterList, dataColumnValueSize);
    btreeNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 0);
    return btreeNode;
  }

  private DataRefNode getLastDataNode() {
    BlockBTreeLeafNode btreeNode;
    DataFileFooter dataFileFooter = new DataFileFooter();
    ColumnSchema columnSchema = new ColumnSchema();
    Encoding encoding = Encoding.DICTIONARY;
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(encoding);
    columnSchema.setEncodingList(encodingList);
    columnSchema.setDimensionColumn(true);
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setColumnName("name");
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    dataFileFooter.setColumnInTable(columnSchemas);
    SegmentInfo segmentInfo = new SegmentInfo();
    int[] cardinality = { 1 };
    segmentInfo.setColumnCardinality(cardinality);
    segmentInfo.setNumberOfColumns(1);
    dataFileFooter.setSegmentInfo(segmentInfo);
    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    byte[][] maxValues = { { 6 } };
    blockletMinMaxIndex.setMaxValues(maxValues);
    byte[][] minValues = { { 2 } };
    blockletMinMaxIndex.setMinValues(minValues);
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);

    byte[] startKey = { 0, 0, 0, 0, 0, 0,0 , 1, 3, 1, 2, 3 };
    byte[] endKey = { 3, 4 };
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex(startKey, endKey);
    blockletIndex.setBtreeIndex(blockletBTreeIndex);
    dataFileFooter.setBlockletIndex(blockletIndex);
    List<DataFileFooter> dataFileFooterList = new ArrayList<>();
    dataFileFooterList.add(dataFileFooter);

    int[] dataColumnValueSize = { 1, 1 };
    final BTreeBuilderInfo bTreeBuilderInfo =
        new BTreeBuilderInfo(dataFileFooterList, dataColumnValueSize);
    btreeNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 0);
    return btreeNode;
  }

}
