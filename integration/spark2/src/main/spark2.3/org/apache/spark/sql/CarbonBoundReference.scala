package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, LeafExpression, NamedExpression}
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.core.scan.expression.ColumnExpression

case class CarbonBoundReference(colExp: ColumnExpression, dataType: DataType, nullable: Boolean)
  extends LeafExpression with NamedExpression with CodegenFallback {

  type EvaluatedType = Any

  override def toString: String = s"input[" + colExp.getColIndex + "]"

  override def eval(input: InternalRow): Any = input.get(colExp.getColIndex, dataType)

  override def name: String = colExp.getColumnName

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def exprId: ExprId = throw new UnsupportedOperationException

  override def qualifier: Option[String] = null

  override def newInstance(): NamedExpression = throw new UnsupportedOperationException
}
