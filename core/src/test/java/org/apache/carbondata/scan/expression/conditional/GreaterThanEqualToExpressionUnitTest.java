package org.apache.carbondata.scan.expression.conditional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.RowImpl;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;
import org.apache.spark.sql.types.Decimal;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GreaterThanEqualToExpressionUnitTest {

  static GreaterThanEqualToExpression greaterThanEqualToExpression;

  @Test public void testEvaluateWithBothStringISSame()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("left_name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("right_name", DataType.STRING);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, left);
    RowImpl value = new RowImpl();
    String[] row = { "string1" };
    Object objectRow[] = { row, row };

    new MockUp<ExpressionResult>() {
      @Mock public String getString() {
        return "string1";
      }
    };
    value.setValues(objectRow);
    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("id", DataType.SHORT);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());

  }

  @Test public void testEvaluateWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_number", DataType.INT);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_number", DataType.INT);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 140 };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 140;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("right_contact", DataType.DOUBLE);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("left_contact", DataType.DOUBLE);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Double[] row = { 44D };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Double getDouble() {
        return 44D;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.LONG);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataType.LONG);
    left.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Long[] row = { 1234567654321L };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Long getLong() {
        return 1234567654321L;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression right = new ColumnExpression("timestamp", DataType.TIMESTAMP);
      right.setColIndex(0);
      greaterThanEqualToExpression = new GreaterThanEqualToExpression(right, right);
      RowImpl value = new RowImpl();
      DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
      Date date = dateFormat.parse("23/09/2007");
      long time = date.getTime();
      Timestamp[] row = { new Timestamp(time) };
      Object objectRow[] = { row };
      value.setValues(objectRow);

      new MockUp<ExpressionResult>() {
        @Mock public Long getTime() {
          return 18465213000000L;
        }
      };

      ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
      assertTrue(result.getBoolean());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test(expected = FilterUnsupportedException.class) public void testDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.BOOLEAN);
    right.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row };
    value.setValues(objectRow);
    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
  }

  @Test public void testEvaluateWithDecimalDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.DECIMAL);
    right.setColIndex(0);
    ColumnExpression left = new ColumnExpression("contact", DataType.DECIMAL);
    left.setColIndex(1);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    Decimal[] row = new Decimal[] { Decimal.apply(12345.0) };
    Object objectRow[] = { row, row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public BigDecimal getDecimal() {
        return new BigDecimal(12345.0);
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testEvaluateWithIsNullReturnTrue()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public boolean isNull() {
        return true;
      }
    };

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertFalse(result.getBoolean());

  }

  @Test public void testEvaluateWithLeftAndRightDifferentDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression left = new ColumnExpression("name", DataType.STRING);
    left.setColIndex(0);
    ColumnExpression right = new ColumnExpression("number", DataType.INT);
    right.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(left, right);
    RowImpl value = new RowImpl();
    String[] row1 = { "String1" };
    Integer[] row = { 14 };
    Object objectRow[] = { row, row1 };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 14;
      }
    };

    ExpressionResult result = greaterThanEqualToExpression.evaluate(value);
    assertTrue(result.getBoolean());
  }

  @Test public void testGetString() throws Exception {
    ColumnExpression right = new ColumnExpression("name", DataType.STRING);
    right.setColIndex(0);
    greaterThanEqualToExpression = new GreaterThanEqualToExpression(right, right);
    String expected_result = "GreaterThanEqualTo(ColumnExpression(name),ColumnExpression(name))";
    String result = greaterThanEqualToExpression.getString();
    assertEquals(expected_result, result);
  }

}

