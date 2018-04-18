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

package org.apache.carbondata.core.util;

import java.util.Arrays;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.NotEqualsExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.executer.RangeValueFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.intf.FilterOptimizer;
import org.apache.carbondata.core.scan.filter.optimizer.RangeFilterOptmizer;

import mockit.Deencapsulation;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/* Test Cases for Range Filter */
public class RangeFilterProcessorTest {
  @BeforeClass public static void setUp() throws Exception {
  }

  public boolean checkBothTrees(Expression a, Expression b) {

    if (null == a && null == b) {
      return true;
    }

    if (null == a) return false;
    if (null == b) return false;

    if ((a != null) && (b != null)) {
      return ((a.getClass() == b.getClass()) && (
          (a.getChildren().size() == 0 && b.getChildren().size() == 0) || (
              a.getChildren().size() == 2 && b.getChildren().size() == 2 && checkBothTrees(
                  a.getChildren().get(0), b.getChildren().get(0)) && checkBothTrees(
                  a.getChildren().get(1), b.getChildren().get(1)))));
    }
    return false;
  }

  @Test public void createFilterTree() {

    Expression inputFilter;
    boolean result = false;
    ColumnExpression cola = new ColumnExpression("a", DataTypes.STRING);
    cola.setDimension(true);

    ColumnSchema empColumnSchema = new ColumnSchema();
    empColumnSchema.setColumnName("empNameCol");
    empColumnSchema.setColumnUniqueId("empNameCol");
    empColumnSchema.setDimensionColumn(true);
    empColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    empColumnSchema.setDataType(DataTypes.STRING);
    CarbonDimension empDimension = new CarbonDimension(empColumnSchema, 0, 0, 0, 0, 0);
    cola.setDimension(empDimension);

    Expression greaterThan =
        new GreaterThanEqualToExpression(cola, new LiteralExpression("11", DataTypes.STRING));

    ColumnExpression colb = new ColumnExpression("a", DataTypes.STRING);
    colb.setDimension(true);
    colb.setDimension(empDimension);
    Expression lessThan =
        new LessThanEqualToExpression(colb, new LiteralExpression("20", DataTypes.STRING));
    inputFilter = new AndExpression(greaterThan, lessThan);

    Expression output = new AndExpression(new RangeExpression(
        new GreaterThanEqualToExpression(new ColumnExpression("a", DataTypes.STRING),
            new LiteralExpression("11", DataTypes.STRING)),
        new LessThanEqualToExpression(new ColumnExpression("a", DataTypes.STRING),
            new LiteralExpression("20", DataTypes.STRING))), new TrueExpression(null));
    FilterOptimizer rangeFilterOptimizer =
        new RangeFilterOptmizer(inputFilter);
    rangeFilterOptimizer.optimizeFilter();
    result = checkBothTrees(inputFilter, output);
    Assert.assertTrue(result);

  }

  @Test public void createFilterTree_noChange() {

    // Build 2nd Tree no change a < 5 and a > 20

    Expression inputFilter;
    boolean result = false;
    ColumnExpression cola = new ColumnExpression("a", DataTypes.STRING);
    cola.setDimension(true);

    ColumnSchema empColumnSchema = new ColumnSchema();
    empColumnSchema.setColumnName("empNameCol");
    empColumnSchema.setColumnUniqueId("empNameCol");
    empColumnSchema.setDimensionColumn(true);
    empColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    empColumnSchema.setDataType(DataTypes.STRING);
    CarbonDimension empDimension = new CarbonDimension(empColumnSchema, 0, 0, 0, 0, 0);
    cola.setDimension(empDimension);

    Expression greaterThan =
        new GreaterThanEqualToExpression(cola, new LiteralExpression("20", DataTypes.STRING));

    ColumnExpression colb = new ColumnExpression("a", DataTypes.STRING);
    colb.setDimension(true);
    colb.setDimension(empDimension);
    Expression lessThan =
        new LessThanEqualToExpression(colb, new LiteralExpression("05", DataTypes.STRING));
    inputFilter = new AndExpression(greaterThan, lessThan);

    Expression output = new AndExpression(
        new GreaterThanEqualToExpression(new ColumnExpression("a", DataTypes.STRING),
            new LiteralExpression("20", DataTypes.STRING)),
        new LessThanEqualToExpression(new ColumnExpression("a", DataTypes.STRING),
            new LiteralExpression("05", DataTypes.STRING)));
    FilterOptimizer rangeFilterOptimizer =
        new RangeFilterOptmizer(inputFilter);
    rangeFilterOptimizer.optimizeFilter();
    result = checkBothTrees(inputFilter, output);
    // no change
    Assert.assertTrue(result);
  }

  @Test public void createFilterTree_flavor1() {

    // Build 3rd BTree a >= '11' and a > '12' and a <= '20' and a <= '15'

    Expression inputFilter;
    boolean result = false;

    ColumnSchema empColumnSchema = new ColumnSchema();
    empColumnSchema.setColumnName("a");
    empColumnSchema.setColumnUniqueId("a");
    empColumnSchema.setDimensionColumn(true);
    empColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    empColumnSchema.setDataType(DataTypes.STRING);
    CarbonDimension empDimension = new CarbonDimension(empColumnSchema, 0, 0, 0, 0, 0);

    ColumnExpression cola1 = new ColumnExpression("a", DataTypes.STRING);
    cola1.setDimension(true);
    cola1.setDimension(empDimension);

    ColumnExpression cola2 = new ColumnExpression("a", DataTypes.STRING);
    cola2.setDimension(true);
    cola2.setDimension(empDimension);

    ColumnExpression cola3 = new ColumnExpression("a", DataTypes.STRING);
    cola3.setDimension(true);
    cola3.setDimension(empDimension);

    ColumnExpression cola4 = new ColumnExpression("a", DataTypes.STRING);
    cola4.setDimension(true);
    cola4.setDimension(empDimension);

    Expression lessThan1 =
        new LessThanEqualToExpression(cola1, new LiteralExpression("15", DataTypes.STRING));
    Expression lessThan2 =
        new LessThanEqualToExpression(cola2, new LiteralExpression("20", DataTypes.STRING));
    Expression greaterThan1 =
        new GreaterThanExpression(cola3, new LiteralExpression("12", DataTypes.STRING));
    Expression greaterThan2 =
        new GreaterThanEqualToExpression(cola4, new LiteralExpression("11", DataTypes.STRING));

    Expression And1 = new AndExpression(new NotEqualsExpression(null, null), greaterThan2);
    Expression And2 = new AndExpression(And1, greaterThan1);
    Expression And3 = new AndExpression(And2, lessThan2);
    inputFilter = new AndExpression(And3, lessThan1);

    // Build The output

    ColumnExpression colb1 = new ColumnExpression("a", DataTypes.STRING);
    cola1.setDimension(true);
    cola1.setDimension(empDimension);

    ColumnExpression colb2 = new ColumnExpression("a", DataTypes.STRING);
    cola2.setDimension(true);
    cola2.setDimension(empDimension);

    Expression greaterThanb1 =
        new GreaterThanExpression(cola3, new LiteralExpression("12", DataTypes.STRING));

    Expression lessThanb1 =
        new LessThanEqualToExpression(cola1, new LiteralExpression("15", DataTypes.STRING));

    Expression Andb1 =
        new AndExpression(new NotEqualsExpression(null, null), new TrueExpression(null));

    Expression Andb2 = new AndExpression(Andb1, new RangeExpression(greaterThanb1, lessThanb1));
    Expression Andb3 = new AndExpression(Andb2, new TrueExpression(null));

    FilterOptimizer rangeFilterOptimizer =
        new RangeFilterOptmizer(inputFilter);
    rangeFilterOptimizer.optimizeFilter();
    result = checkBothTrees(inputFilter, new AndExpression(Andb3, new TrueExpression(null)));
    // no change
    Assert.assertTrue(result);
  }

  @Test public void createFilterTree_flavor2() {

    // Build 3rd BTree a >= '11' or a > '12' or a <= '20' or a <= '15'

    Expression inputFilter;
    boolean result = false;

    ColumnSchema empColumnSchema = new ColumnSchema();
    empColumnSchema.setColumnName("a");
    empColumnSchema.setColumnUniqueId("a");
    empColumnSchema.setDimensionColumn(true);
    empColumnSchema.setEncodingList(Arrays.asList(Encoding.DICTIONARY));
    empColumnSchema.setDataType(DataTypes.STRING);
    CarbonDimension empDimension = new CarbonDimension(empColumnSchema, 0, 0, 0, 0, 0);

    ColumnExpression cola1 = new ColumnExpression("a", DataTypes.STRING);
    cola1.setDimension(true);
    cola1.setDimension(empDimension);

    ColumnExpression cola2 = new ColumnExpression("a", DataTypes.STRING);
    cola2.setDimension(true);
    cola2.setDimension(empDimension);

    ColumnExpression cola3 = new ColumnExpression("a", DataTypes.STRING);
    cola3.setDimension(true);
    cola3.setDimension(empDimension);

    ColumnExpression cola4 = new ColumnExpression("a", DataTypes.STRING);
    cola4.setDimension(true);
    cola4.setDimension(empDimension);

    Expression lessThan1 =
        new LessThanEqualToExpression(cola1, new LiteralExpression("15", DataTypes.STRING));
    Expression lessThan2 =
        new LessThanEqualToExpression(cola2, new LiteralExpression("20", DataTypes.STRING));
    Expression greaterThan1 =
        new GreaterThanExpression(cola3, new LiteralExpression("12", DataTypes.STRING));
    Expression greaterThan2 =
        new GreaterThanEqualToExpression(cola4, new LiteralExpression("11", DataTypes.STRING));

    Expression Or1 = new OrExpression(new NotEqualsExpression(null, null), greaterThan2);
    Expression Or2 = new OrExpression(Or1, greaterThan1);
    Expression Or3 = new OrExpression(Or2, lessThan2);
    inputFilter = new OrExpression(Or3, lessThan1);

    // Build The output

    ColumnExpression colb1 = new ColumnExpression("a", DataTypes.STRING);
    cola1.setDimension(true);
    cola1.setDimension(empDimension);

    ColumnExpression colb2 = new ColumnExpression("a", DataTypes.STRING);
    cola2.setDimension(true);
    cola2.setDimension(empDimension);

    ColumnExpression colb3 = new ColumnExpression("a", DataTypes.STRING);
    cola3.setDimension(true);
    cola3.setDimension(empDimension);

    ColumnExpression colb4 = new ColumnExpression("a", DataTypes.STRING);
    cola4.setDimension(true);
    cola4.setDimension(empDimension);

    Expression lessThanb1 =
        new LessThanEqualToExpression(colb1, new LiteralExpression("15", DataTypes.STRING));
    Expression lessThanb2 =
        new LessThanEqualToExpression(colb2, new LiteralExpression("20", DataTypes.STRING));
    Expression greaterThanb1 =
        new GreaterThanExpression(colb3, new LiteralExpression("12", DataTypes.STRING));
    Expression greaterThanb2 =
        new GreaterThanEqualToExpression(colb4, new LiteralExpression("11", DataTypes.STRING));

    Expression Orb1 = new OrExpression(new NotEqualsExpression(null, null), greaterThanb2);
    Expression Orb2 = new OrExpression(Orb1, greaterThanb1);
    Expression Orb3 = new OrExpression(Orb2, lessThanb2);

    FilterOptimizer rangeFilterOptimizer =
        new RangeFilterOptmizer(inputFilter);
    rangeFilterOptimizer.optimizeFilter();
    result = checkBothTrees(inputFilter, new OrExpression(Orb3, lessThanb1));
    // no change
    Assert.assertTrue(result);
  }

  @Test public void checkIsScanRequired1() {
    byte[] BlockMin = { 1 };
    byte[] BlockMax = { 2 };
    boolean result;

    byte[][] filterMinMax = { { (byte) 10 }, { (byte) 20 } };

    RangeValueFilterExecuterImpl range = new MockUp<RangeValueFilterExecuterImpl>() {
    }.getMockInstance();
    Deencapsulation.setField(range, "isDimensionPresentInCurrentBlock", true);
    Deencapsulation.setField(range, "lessThanExp", true);
    Deencapsulation.setField(range, "greaterThanExp", true);
    result = range.isScanRequired(BlockMin, BlockMax, filterMinMax);
    Assert.assertFalse(result);
  }

  @Test public void checkIsScanRequired1_1() {
    byte[] BlockMin = { 21 };
    byte[] BlockMax = { 22 };
    boolean result;

    byte[][] filterMinMax = { { (byte) 10 }, { (byte) 20 } };

    RangeValueFilterExecuterImpl range = new MockUp<RangeValueFilterExecuterImpl>() {
    }.getMockInstance();
    Deencapsulation.setField(range, "isDimensionPresentInCurrentBlock", true);
    Deencapsulation.setField(range, "lessThanExp", true);
    Deencapsulation.setField(range, "greaterThanExp", true);
    result = range.isScanRequired(BlockMin, BlockMax, filterMinMax);
    Assert.assertFalse(result);
  }

  @Test public void checkIsScanRequired2() {
    byte[] BlockMin = { 12 };
    byte[] BlockMax = { 16 };
    boolean result;

    byte[][] filterMinMax = { { (byte) 10 }, { (byte) 20 } };

    RangeValueFilterExecuterImpl range = new MockUp<RangeValueFilterExecuterImpl>() {
    }.getMockInstance();
    Deencapsulation.setField(range, "isDimensionPresentInCurrentBlock", true);
    Deencapsulation.setField(range, "lessThanExp", true);
    Deencapsulation.setField(range, "greaterThanExp", true);
    result = range.isScanRequired(BlockMin, BlockMax, filterMinMax);
    Assert.assertTrue(result);
  }

  @Test public void checkIsScanRequired3() {
    byte[] BlockMin = { 12 };
    byte[] BlockMax = { 16 };
    boolean result;
    boolean rangeCovered;

    byte[][] filterMinMax = { { (byte) 10 }, { (byte) 20 } };

    RangeValueFilterExecuterImpl range = new MockUp<RangeValueFilterExecuterImpl>() {
    }.getMockInstance();
    Deencapsulation.setField(range, "isDimensionPresentInCurrentBlock", true);
    Deencapsulation.setField(range, "lessThanExp", true);
    Deencapsulation.setField(range, "greaterThanExp", true);

    result = range.isScanRequired(BlockMin, BlockMax, filterMinMax);
    rangeCovered = Deencapsulation.getField(range, "isRangeFullyCoverBlock");
    Assert.assertTrue(result);
    Assert.assertTrue(rangeCovered);
  }

  @Test public void checkIsScanRequired4() {
    byte[] BlockMin = { 12 };
    byte[] BlockMax = { 30 };
    boolean result;
    boolean startBlockMinIsDefaultStart;

    byte[][] filterMinMax = { { (byte) 10 }, { (byte) 20 } };

    RangeValueFilterExecuterImpl range = new MockUp<RangeValueFilterExecuterImpl>() {
    }.getMockInstance();
    Deencapsulation.setField(range, "isDimensionPresentInCurrentBlock", true);
    Deencapsulation.setField(range, "lessThanExp", true);
    Deencapsulation.setField(range, "greaterThanExp", true);

    result = range.isScanRequired(BlockMin, BlockMax, filterMinMax);
    startBlockMinIsDefaultStart = Deencapsulation.getField(range, "startBlockMinIsDefaultStart");
    Assert.assertTrue(result);
    Assert.assertTrue(startBlockMinIsDefaultStart);
  }

  @Test public void checkIsScanRequired5() {
    byte[] BlockMin = { 10 };
    byte[] BlockMax = { 16 };
    boolean result;
    boolean endBlockMaxisDefaultEnd;

    byte[][] filterMinMax = { { (byte) 15 }, { (byte) 20 } };

    RangeValueFilterExecuterImpl range = new MockUp<RangeValueFilterExecuterImpl>() {
    }.getMockInstance();
    Deencapsulation.setField(range, "isDimensionPresentInCurrentBlock", true);
    Deencapsulation.setField(range, "lessThanExp", true);
    Deencapsulation.setField(range, "greaterThanExp", true);

    result = range.isScanRequired(BlockMin, BlockMax, filterMinMax);
    endBlockMaxisDefaultEnd = Deencapsulation.getField(range, "endBlockMaxisDefaultEnd");
    Assert.assertTrue(result);
    Assert.assertTrue(endBlockMaxisDefaultEnd);
  }

  @AfterClass public static void testcleanUp() {
  }

}

