package org.apache.carbondata.scan.expression.conditional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.RowImpl;

import mockit.Mock;
import mockit.MockUp;
import org.apache.spark.sql.execution.columnar.BOOLEAN;
import org.apache.spark.sql.types.Decimal;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class EqualToExpressionUnitTest {

  static EqualToExpression equalToExpression;

  @Test public void testGetString() throws Exception {
    ColumnExpression right = new ColumnExpression("name", DataType.STRING);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    String expected_result = "EqualTo(ColumnExpression(name),ColumnExpression(name))";
    String result = equalToExpression.getString();
    assertEquals(expected_result, result);
  }

  @Test public void testEvaluateWithShortDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("id", DataType.SHORT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Short[] row = { 15 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Short getShort() {
        return 15;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assert (!result.isNull());

  }

  @Test public void testEvaluateWithStringDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("name", DataType.STRING);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    String[] row = { "String1" };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public String getString() {
        return "string";
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assert (!result.isNull());

  }

  @Test public void testEvaluateWithIntDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("number", DataType.INT);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Integer[] row = { 14 };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Integer getInt() {
        return 14;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assert (!result.isNull());
  }

  @Test public void testEvaluateWithDoubleDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.DOUBLE);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Double[] row = { 44D };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Double getDouble() {
        return 44D;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assert (!result.isNull());
  }

  @Test public void testEvaluateWithLongDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.LONG);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Long[] row = { 1234567654321L };
    Object objectRow[] = { row };
    value.setValues(objectRow);

    new MockUp<ExpressionResult>() {
      @Mock public Long getLong() {
        return 1234567654321L;
      }
    };

    ExpressionResult result = equalToExpression.evaluate(value);
    assert (!result.isNull());
  }

  @Test public void testEvaluateWithTimestampDataType()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    try {
      ColumnExpression right = new ColumnExpression("timestamp", DataType.TIMESTAMP);
      right.setColIndex(0);
      equalToExpression = new EqualToExpression(right, right);
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

      ExpressionResult result = equalToExpression.evaluate(value);
      assert (!result.isNull());
    } catch (ParseException e) {
      System.out.println("Error while parsing " + e.getMessage());
    }
  }

  @Test(expected = FilterUnsupportedException.class) public void testDefaultCase()
      throws FilterUnsupportedException, FilterIllegalMemberException {
    ColumnExpression right = new ColumnExpression("contact", DataType.BOOLEAN);
    right.setColIndex(0);
    equalToExpression = new EqualToExpression(right, right);
    RowImpl value = new RowImpl();
    Boolean[] row = { true };
    Object objectRow[] = { row };
    value.setValues(objectRow);
    ExpressionResult result = equalToExpression.evaluate(value);
  }

}
