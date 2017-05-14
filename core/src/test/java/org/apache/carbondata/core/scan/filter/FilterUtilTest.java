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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.cache.dictionary.AbstractDictionaryCacheTest;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.util.BitSetGroup;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FilterUtilTest extends AbstractDictionaryCacheTest {

  private ColumnSchema columnSchema;

  @Before public void setUp() throws Exception {
    init();
    this.databaseName = props.getProperty("database", "testSchema");
    this.tableName = props.getProperty("tableName", "carbon");
    this.carbonStorePath = props.getProperty("storePath", "carbonStore");
    carbonTableIdentifier =
        new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString());
    this.carbonStorePath = props.getProperty("storePath", "carbonStore");
    columnSchema = new ColumnSchema();
    columnSchema.setColumnar(true);
    columnSchema.setColumnName("IMEI");
    columnSchema.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema.setDataType(DataType.STRING);
    columnSchema.setDimensionColumn(true);
  }

  @Test public void testCheckIfLeftExpressionRequireEvaluation() {
    List<Expression> children = new ArrayList<>();
    ListExpression expression = new ListExpression(children);
    boolean result = FilterUtil.checkIfLeftExpressionRequireEvaluation(expression);
    assertTrue(result);
  }

  @Test
  public void testCheckIfLeftExpressionRequireEvaluationWithExpressionNotInstanceOfColumnExpression() {
    ColumnExpression expression = new ColumnExpression("test", DataType.STRING);
    boolean result = FilterUtil.checkIfLeftExpressionRequireEvaluation(expression);
    assertFalse(result);
  }

  @Test public void testNanSafeEqualsDoublesWithUnEqualValues() {
    Double d1 = new Double(60.67);
    Double d2 = new Double(60.69);
    boolean result = FilterUtil.nanSafeEqualsDoubles(d1, d2);
    assertFalse(result);
  }

  @Test public void testNanSafeEqualsDoublesWithEqualValues() {
    Double d1 = new Double(60.67);
    Double d2 = new Double(60.67);
    boolean result = FilterUtil.nanSafeEqualsDoubles(d1, d2);
    assertTrue(result);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForShortValue() {
    String dictionaryVal = "1";
    String memberVal = "1";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.SHORT);
    int expectedResult = 0;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForIntValue() {
    String dictionaryVal = "1000";
    String memberVal = "1001";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.INT);
    int expectedResult = -1;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForDoubleValue() {
    String dictionaryVal = "1.90";
    String memberVal = "1.89";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.DOUBLE);
    int expectedResult = 1;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForLongValue() {
    String dictionaryVal = "111111111111111";
    String memberVal = "1111111111111111";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.LONG);
    int expectedResult = -1;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForBooleanValue() {
    String dictionaryVal = "true";
    String memberVal = "false";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.BOOLEAN);
    int expectedResult = 1;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForDecimalValue() {
    String dictionaryVal = "1111111";
    String memberVal = "1111";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.DECIMAL);
    int expectedResult = 1;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForDefaultValue() {
    String dictionaryVal = "11.78";
    String memberVal = "1111.90";
    int actualResult =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.FLOAT);
    int expectedResult = -1;
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForTimestamp() {
    String dictionaryVal = "2008-01-01 00:00:01";
    String memberVal = "2008-01-01 00:00:01";
    int actualValue =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.TIMESTAMP);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCompareFilterKeyBasedOnDataTypeForException() throws Exception {
    String dictionaryVal = "test";
    String memberVal = "1";
    int actualValue =
        FilterUtil.compareFilterKeyBasedOnDataType(dictionaryVal, memberVal, DataType.INT);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testCreateIndexKeyFromResolvedFilterVal() throws Exception {
    long[] startOrEndKey = new long[] { 0, 10 };
    byte[] startOrEndKeyForNoDictDimension = { 1, 2 };
    int[] keys = new int[] { 1, 2 };
    MultiDimKeyVarLengthGenerator multiDimKeyVarLengthGenerator =
        new MultiDimKeyVarLengthGenerator(keys);
    assertTrue(FilterUtil
        .createIndexKeyFromResolvedFilterVal(startOrEndKey, multiDimKeyVarLengthGenerator,
            startOrEndKeyForNoDictDimension) != null);

  }

  @Test public void testCheckIfExpressionContainsColumn() {
    String columnName = "IMEI";
    Expression expression = new ColumnExpression(columnName, DataType.STRING);
    boolean result = FilterUtil.checkIfExpressionContainsColumn(expression);
    assertTrue(result);
  }

  @Test
  public void testCheckIfExpressionContainsColumnWithExpressionNotInstanceOfColumnExpression() {
    String columnName = "IMEI";
    Expression expression = new LiteralExpression(columnName, DataType.STRING);
    boolean result = FilterUtil.checkIfExpressionContainsColumn(expression);
    assertFalse(result);
  }

  @Test public void testIsExpressionNeedsToResolved() {
    boolean isIncludeFilter = true;
    Object obj = "test";
    LiteralExpression literalExpression = new LiteralExpression(obj, DataType.STRING);
    boolean result = FilterUtil.isExpressionNeedsToResolved(literalExpression, isIncludeFilter);
    assertFalse(result);
  }

  @Test public void testIsExpressionNeedsToResolvedWithDataTypeNullAndIsIncludeFilterFalse() {
    boolean isIncludeFilter = false;
    Object obj = "test";
    LiteralExpression literalExpression = new LiteralExpression(obj, DataType.NULL);
    boolean result = FilterUtil.isExpressionNeedsToResolved(literalExpression, isIncludeFilter);
    assertTrue(result);
  }

  @Test public void testGetMaskKey() {
    int surrogate = 1;
    int[] keys = new int[] { 1, 2 };
    MultiDimKeyVarLengthGenerator multiDimKeyVarLengthGenerator =
        new MultiDimKeyVarLengthGenerator(keys);
    int ordinal = 1;
    int keyOrdinal = 1;
    int columnGroupOrdinal = 1;
    int complexTypeOrdinal = 1;
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setColumnar(true);
    columnSchema.setColumnName("IMEI");
    columnSchema.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema.setDataType(DataType.STRING);
    columnSchema.setDimensionColumn(true);
    CarbonDimension carbonDimension =
        new CarbonDimension(columnSchema, ordinal, keyOrdinal, columnGroupOrdinal,
            complexTypeOrdinal);
    byte[] expectedResult = new byte[] { 1 };
    byte[] actualResult =
        FilterUtil.getMaskKey(surrogate, carbonDimension, multiDimKeyVarLengthGenerator);
    assertArrayEquals(expectedResult, actualResult);
  }

  @Test public void testGetFilterListForAllMembersRS() throws Exception {
    Expression expression = new ColumnExpression("IMEI", DataType.STRING);
    ColumnExpression columnExpression = new ColumnExpression("IMEI", DataType.STRING);
    String defaultValues = "test";
    int defaultSurrogate = 1;
    boolean isIncludeFilter = true;
    int ordinal = 1;
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataType.STRING);
    dimColumn.setDimensionColumn(true);
    final CarbonColumn carbonColumn = new CarbonColumn(dimColumn, ordinal, -1);
    new MockUp<ColumnExpression>() {
      @Mock public CarbonColumn getCarbonColumn() {
        return carbonColumn;
      }
    };

    new MockUp<RowImpl>() {
      @Mock public Object getVal(int index) {
        return "test";
      }
    };
    assertTrue(FilterUtil
        .getFilterListForAllMembersRS(expression, columnExpression, defaultValues, defaultSurrogate,
            isIncludeFilter) instanceof DimColumnFilterInfo);
  }

  @Test public void testGetFilterListForAllMembersRSWithDefaultValuesEqualsToNull()
      throws Exception {
    Expression expression = new ColumnExpression("IMEI", DataType.STRING);
    ColumnExpression columnExpression = new ColumnExpression("IMEI", DataType.STRING);
    String defaultValues = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
    int defaultSurrogate = 1;
    boolean isIncludeFilter = true;
    int ordinal = 1;
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataType.STRING);
    dimColumn.setDimensionColumn(true);
    final CarbonColumn carbonColumn = new CarbonColumn(dimColumn, ordinal, -1);
    new MockUp<ColumnExpression>() {
      @Mock public CarbonColumn getCarbonColumn() {
        return carbonColumn;
      }
    };

    new MockUp<RowImpl>() {
      @Mock public Object getVal(int index) {
        return "test";
      }
    };
    assertTrue(FilterUtil
        .getFilterListForAllMembersRS(expression, columnExpression, defaultValues, defaultSurrogate,
            isIncludeFilter) instanceof DimColumnFilterInfo);
  }

  @Test public void testgetFilterListForRS() throws Exception {
    Expression expression = new ColumnExpression("IMEI", DataType.STRING);
    ColumnExpression columnExpression = new ColumnExpression("IMEI", DataType.STRING);
    String defaultValues = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
    int defaultSurrogate = 1;
    int ordinal = 1;
    final CarbonColumn carbonColumn = new CarbonColumn(columnSchema, ordinal, -1);
    new MockUp<ColumnExpression>() {
      @Mock public CarbonColumn getCarbonColumn() {
        return carbonColumn;
      }
    };

    new MockUp<RowImpl>() {
      @Mock public Object getVal(int index) {
        return "test";
      }
    };
    assertTrue(FilterUtil.getFilterListForRS(expression, columnExpression, defaultValues,
        defaultSurrogate) instanceof DimColumnFilterInfo);
  }

  @Test public void testCheckIfDataTypeNotTimeStamp() {
    Expression expression = new ColumnExpression("test", DataType.STRING);
    boolean result = FilterUtil.checkIfDataTypeNotTimeStamp(expression);
    assertFalse(result);
  }

  @Test public void testPrepareDefaultEndIndexKey() throws Exception {
    List<ColumnSchema> columnsInTable = new ArrayList<>();
    columnsInTable.add(columnSchema);
    int[] columnCardinality = new int[] { 1, 2 };
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        List<Encoding> encodingList = new ArrayList<>();
        encodingList.add(Encoding.DICTIONARY);
        return encodingList;
      }
    };
    SegmentProperties segmentProperties = new SegmentProperties(columnsInTable, columnCardinality);
    assertTrue(FilterUtil.prepareDefaultEndIndexKey(segmentProperties) instanceof IndexKey);
  }

  @Test public void testCheckIfRightExpressionRequireEvaluation() {
    Expression expression = new ColumnExpression("test", DataType.STRING);
    boolean result = FilterUtil.checkIfRightExpressionRequireEvaluation(expression);
    assertTrue(result);
  }

  @Test
  public void testCheckIfRightExpressionRequireEvaluationWithExpressionIsInstanceOfLiteralExpression() {
    Expression expression = new LiteralExpression("test", DataType.STRING);
    boolean result = FilterUtil.checkIfRightExpressionRequireEvaluation(expression);
    assertFalse(result);
  }

  @Test public void testGetNoDictionaryValKeyMemberForFilter() throws FilterUnsupportedException {
    boolean isIncludeFilter = true;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier(this.carbonStorePath, carbonTableIdentifier);
    ColumnExpression expression = new ColumnExpression("test", DataType.STRING);
    List<String> evaluateResultListFinal = new ArrayList<>();
    evaluateResultListFinal.add("test1");
    evaluateResultListFinal.add("test2");
    assertTrue(FilterUtil
        .getNoDictionaryValKeyMemberForFilter(evaluateResultListFinal, isIncludeFilter,
            DataType.STRING) instanceof DimColumnFilterInfo);
  }

  @Test public void testPrepareDefaultStartIndexKey() throws KeyGenException {
    List<ColumnSchema> columnsInTable = new ArrayList<>();
    columnsInTable.add(columnSchema);
    int[] columnCardinality = new int[] { 1, 2 };
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        List<Encoding> encodingList = new ArrayList<>();
        encodingList.add(Encoding.DICTIONARY);
        return encodingList;
      }
    };
    SegmentProperties segmentProperties = new SegmentProperties(columnsInTable, columnCardinality);
    assertTrue(FilterUtil.prepareDefaultStartIndexKey(segmentProperties) instanceof IndexKey);
  }

  @Test public void testCreateBitSetGroupWithDefaultValue() {
    // test for exactly divisible values
    BitSetGroup bitSetGroupWithDefaultValue =
        FilterUtil.createBitSetGroupWithDefaultValue(14, 448000, true);
    assertTrue(bitSetGroupWithDefaultValue.getNumberOfPages() == 14);
    // test for remainder values
    bitSetGroupWithDefaultValue =
        FilterUtil.createBitSetGroupWithDefaultValue(15, 448200, true);
    assertTrue(bitSetGroupWithDefaultValue.getNumberOfPages() == 15);
  }
}
