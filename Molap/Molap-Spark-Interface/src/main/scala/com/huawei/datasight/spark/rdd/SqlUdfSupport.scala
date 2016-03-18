package com.huawei.datasight.spark.rdd

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, GenericMutableRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.StringType

case class SqlUdf(fn: (InternalRow) => Any, exprs: Expression*) extends Expression with CodegenFallback {
  self: Product =>

  override def children = exprs

  override def dataType = StringType

  override def nullable = true
  
  override def references = AttributeSet(exprs.flatMap(_.references.iterator))

  override def foldable = !children.exists(!_.foldable)

  override def toString = fn.toString + "(" + exprs.foreach(" " + _.toString) + ")"

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    val exprEvals = exprs.map {
      _.eval(input) match {
        case d: MeasureAggregator => d.getValue()
        case others => others
      }
    }
    evalConvert(exprEvals)
  }

  @inline
  def evalConvert(a: Seq[Any]): Any = {
    fn(new GenericMutableRow(a.toArray))
  }

}


trait AddColumnExpression {
  def getExpression: Expression
}

trait GroupbyExpression extends AddColumnExpression {
  def getGroupByColumn: Expression
}


case class Range(udf: Expression, expr: Expression) extends GroupbyExpression {
  override def getExpression: Expression = udf

  override def getGroupByColumn: Expression = expr
}

case class AddColumn(udf: Expression) extends AddColumnExpression {
  override def getExpression: Expression = udf
}
