package org.carbondata.scan.expression;

import java.util.List;

public abstract class UnknownExpression extends Expression {

  public abstract List<ColumnExpression> getAllColumnList();

}
