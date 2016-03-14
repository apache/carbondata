package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Expression
import com.huawei.unibi.molap.engine.expression.ColumnExpression
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback


case class MolapBoundReference(colExp: ColumnExpression, dataType: DataType, nullable: Boolean)
  extends LeafExpression with NamedExpression with CodegenFallback {

  type EvaluatedType = Any

  override def toString: String = s"input[" + colExp.getColIndex() + "]"

  override def eval(input: InternalRow): Any = input.get(colExp.getColIndex(), dataType)

  override def name: String = colExp.getColumnName()

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def qualifiers: Seq[String] = throw new UnsupportedOperationException

  override def exprId: ExprId = throw new UnsupportedOperationException

}