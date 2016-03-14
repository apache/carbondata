/**
  *
  */
package org.apache.spark.sql

import java.util.HashMap
import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryNode
import scala.collection.mutable.MutableList

//import org.apache.calcite.schema.AggregateFunction
import org.apache.spark.sql.catalyst.InternalRow

/**
  * :: DeveloperApi ::
  * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
  * group.
  *
  * @param partial              if true then aggregation is done partially on local data without shuffling to
  *                             ensure all values where `groupingExpressions` are equal are present.
  * @param groupingExpressions  expressions that are evaluated to determine grouping.
  * @param aggregateExpressions expressions that are computed for each group.
  * @param child                the input data source.
  */
@DeveloperApi
case class MolapAggregate(
                           partial: Boolean,
                           groupingExpressions: Seq[Expression],
                           aggregateExpressions: Seq[NamedExpression],
                           child: SparkPlan)(@transient sqlContext: SQLContext)
  extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def otherCopyArgs = sqlContext :: Nil

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  private[this] val childOutput = child.output

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
    * An aggregate that needs to be computed for each row in a group.
    *
    * @param unbound         Unbound version of this aggregate, used for result substitution.
    * @param aggregate       A bound copy of this aggregate used to create a new aggregation buffer.
    * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
    *                        output.
    */
  case class ComputedAggregate(
                                unbound: AggregateExpression1,
                                aggregate: AggregateExpression1,
                                resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression1 =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, childOutput),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction1] = {
    val buffer = new Array[AggregateFunction1](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
    * A map of substitutions that are used to insert the aggregate expressions and grouping
    * expression into the final result expression.
    */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
    * Substituted version of aggregateExpressions expressions which are used to compute final
    * output rows given a group and the result of all aggregate computations.
    */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  override def doExecute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = 0
        while (i < buffer.length) {
          aggregateResults(i) = buffer(i).eval(EmptyRow)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[InternalRow, Array[AggregateFunction1]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[InternalRow] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): InternalRow = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}

case class MultiProjection(expressions: Seq[Expression], childProjection: MultiProjection) extends (InternalRow => InternalRow) {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute], childProjection: MultiProjection) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)), childProjection)

  private[this] val exprArray = expressions.toArray
  private[this] val mutableRow = new GenericMultiRow(exprArray.size)

  def currentValue: InternalRow = mutableRow

  def apply(input: InternalRow): InternalRow = {
    var i = 0
    while (i < exprArray.length) {
      mutableRow(i) = exprArray(i).eval(input)
      i += 1
    }
    if (childProjection != null)
      mutableRow.setChildRow(childProjection(input))
    mutableRow
  }
}

class GenericMultiRow(size: Int) extends GenericMutableRow(size) {
  /** No-arg constructor for serialization. */
  def this() = this(0)

  def getStringBuilder(ordinal: Int): StringBuilder = ???

  var childRowList = new MutableList[InternalRow]

  def setChildRow(row: InternalRow) = {
    childRowList += row
  }

  def getChildRows() = childRowList
}