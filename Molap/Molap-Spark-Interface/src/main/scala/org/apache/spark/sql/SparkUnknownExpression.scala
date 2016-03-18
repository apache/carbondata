package org.apache.spark.sql

import com.huawei.datasight.spark.processors.MolapScalaUtil
import com.huawei.unibi.molap.engine.expression.{ColumnExpression, Expression, ExpressionResult}
import com.huawei.unibi.molap.engine.expression.conditional.ConditionalExpression
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException
import com.huawei.unibi.molap.engine.molapfilterinterface.{ExpressionType, RowIntf}
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, GenericMutableRow}

import scala.collection.JavaConverters._

class SparkUnknownExpression(sparkExp: SparkExpression) extends Expression with ConditionalExpression {

  children.addAll(getColumnList())

  override def evaluate(molapRowInstance: RowIntf): ExpressionResult = {

    val values = molapRowInstance.getValues().toSeq.map { value =>
      value match {
        case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
        case _ => value
      }
    }
    try {
      val sparkRes = sparkExp.eval(
        new GenericMutableRow(values.map(a => a.asInstanceOf[Any]).toArray)
      )

      new ExpressionResult(MolapScalaUtil.convertSparkToMolapDataType(sparkExp.dataType), sparkRes);
    }
    catch {
      case e: Exception => throw new FilterUnsupportedException(e.getMessage());
    }
  }

  override def getFilterExpressionType(): ExpressionType = {
    ExpressionType.UNKNOWN
  }

  override def getString(): String = {
    ???
  }


  def getColumnList(): java.util.List[ColumnExpression] = {

    val lst = new java.util.ArrayList[ColumnExpression]()
    getColumnListFromExpressionTree(sparkExp, lst)
    lst
  }

  def getAllColumnList(): java.util.List[ColumnExpression] = {
    val lst = new java.util.ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    lst
  }

  def isSingleDimension(): Boolean = {
    var lst = new java.util.ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    if (lst.size == 1 && lst.get(0).isDimension) {
      true
    }
    else {
      false
    }
  }

  def getColumnListFromExpressionTree(sparkCurrentExp: SparkExpression,
                                      list: java.util.List[ColumnExpression]): Unit = {
    sparkCurrentExp match {
      case molapBoundRef: MolapBoundReference => {
        val foundExp = list.asScala.find(p => p.getColumnName() == molapBoundRef.colExp.getColumnName())
        if (foundExp.isEmpty) {
          molapBoundRef.colExp.setColIndex(list.size)
          list.add(molapBoundRef.colExp)
        } else {
          molapBoundRef.colExp.setColIndex(foundExp.get.getColIndex())
        }
      }
      case _ => sparkCurrentExp.children.foreach(getColumnListFromExpressionTree(_, list))
    }
  }


  def getAllColumnListFromExpressionTree(sparkCurrentExp: SparkExpression,
                                         list: java.util.List[ColumnExpression]): java.util.List[ColumnExpression] = {
    sparkCurrentExp match {
      case molapBoundRef: MolapBoundReference => list.add(molapBoundRef.colExp)
      case _ => sparkCurrentExp.children.foreach(getColumnListFromExpressionTree(_, list))
    }
    list
  }

}