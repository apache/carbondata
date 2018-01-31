package org.apache.carbondata.core.datamap.dev;

import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.scan.expression.Expression;

public abstract class DataMapFilterNode extends DataMapExpression {
  protected DataMapExpression columnExpression;
  protected DataMapExpression literalExpression;
  protected DataMapFactory dataMapFactory;
  protected Expression filterExpression;
  protected DataMapMeta dataMapMeta;

  public DataMapFilterNode(DataMapExpression columnExpr, DataMapExpression literal,
      Expression expr, DataMapFactory dataMapFactory, DataMapMeta dataMapMeta) {
    this.columnExpression = columnExpr;
    this.literalExpression = literal;
    this.filterExpression = expr;
    this.dataMapFactory = dataMapFactory;
    this.dataMapMeta = dataMapMeta;
  }

  public DataMapExpression getColumnExpression () {
    return columnExpression;
  }

  public DataMapExpression getLiteralExpression() {
    return literalExpression;
  }

  public Expression getFilterExpression () {
    return filterExpression;
  }

  public DataMapFactory getDataMapFactory () { return dataMapFactory; }

  public DataMapMeta getDataMapMeta () { return dataMapMeta; }

}
