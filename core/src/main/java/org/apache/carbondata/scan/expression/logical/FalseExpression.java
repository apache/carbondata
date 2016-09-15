package org.apache.carbondata.scan.expression.logical;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.ExpressionResult;
import org.apache.carbondata.scan.expression.LiteralExpression;
import org.apache.carbondata.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.intf.ExpressionType;
import org.apache.carbondata.scan.filter.intf.RowIntf;



/**
 * This class will form an expression whose evaluation will be always false.
 */
public class FalseExpression  extends BinaryConditionalExpression {


  private static final long serialVersionUID = -8390184061336799370L;

  public FalseExpression(Expression child1) {
    super(child1, new LiteralExpression(null,null));
  }

  /**
   * This method will always return false, mainly used in the filter expressions
   * which are illogical.
   * eg: columnName NOT IN('Java',NULL)
   * @param value
   * @return
   * @throws FilterUnsupportedException
   * @throws FilterIllegalMemberException
   */
  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    return new ExpressionResult(DataType.BOOLEAN,false);
  }

  /**
   * This method will return the expression types
   * @return
   */
  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.FALSE;
  }
  @Override public String getString() {
    return null;
  }
}
