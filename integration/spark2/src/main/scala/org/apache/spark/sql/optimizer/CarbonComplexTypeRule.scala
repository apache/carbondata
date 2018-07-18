/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.optimizer

import scala.collection.mutable

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{ArrayType, MetadataBuilder, StructType}

/**
 * Push down complex type attribute reference to CarbonDataRelation
 */
class CarbonComplexTypeRule extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    processPlan(plan)
  }

  private def isFieldOfArrayType(structField: GetStructField): Boolean =
    try {
      structField.childSchema.apply(structField.name.getOrElse(""))
        .dataType.isInstanceOf[ArrayType]
    } catch {
      case e: IllegalArgumentException => true
    }

  private def matchesAttribute(ex: Expression,
    carbonDecoderRelations: Seq[CarbonDecoderRelation]): Option[Attribute] = ex match {
    case exp@GetStructField(child, ordinal, name) if (!isFieldOfArrayType(exp)) =>
      matchesAttribute(child, carbonDecoderRelations)
    case attr: AttributeReference
      if (attr.dataType.isInstanceOf[StructType]
        && !attr.metadata.contains("replaced")
        && carbonDecoderRelations.exists(_.contains(attr))) =>
      Some(attr)
    case _ => None
  }

  object MatchComplexReference {
    def unapply(exp: Expression): Option[Expression] = exp match {
      case e: GetStructField if (!isFieldOfArrayType(e)) => Some(exp)
      case a: AttributeReference if (a.dataType.isInstanceOf[StructType]) => Some(exp)
      case _ => None
    }
  }

  private def processPlan(origPlan: LogicalPlan): LogicalPlan = {
    val carbonDecoderRelations = CarbonOptimizerUtil.collectCarbonRelation(origPlan)
    // Map[oldExpressionId, (newExpressionId, Set(ComplexFieldReferences)]
    val replaceMap = mutable.Map[ExprId, (ExprId, Set[String])]()

    if (carbonDecoderRelations.isEmpty) {
      origPlan
    } else {
      // find the complex type fields which are expected in output
      origPlan.output.foreach(findComplexFieldReferences(carbonDecoderRelations, replaceMap, _))

      // find all complex type references
      var atleastOneReferenceFound = false
      origPlan.foreach(
        dd => dd.expressions.foreach( CarbonOptimizerUtil.foreachUntil(_, {
          case exp@MatchComplexReference(_) =>
            val isReferenceFound = findComplexFieldReferences(carbonDecoderRelations,
                replaceMap,
                exp
              )
            atleastOneReferenceFound = atleastOneReferenceFound || isReferenceFound
            isReferenceFound
          case _ => false
        })))

      // if no complex field references found
      if (replaceMap.isEmpty || !atleastOneReferenceFound) {
        origPlan
      } else {
        val modifiedPlan = replaceComplexFields(origPlan, replaceMap)
        val newplan = replaceCarbonRelationAttributes(modifiedPlan, replaceMap)
        newplan
      }
    }
  }

  private def findComplexFieldReferences(carbonDecoderRelations: Seq[CarbonDecoderRelation],
    replaceMap: mutable.Map[ExprId, (ExprId, Set[String])],
    exp: Expression) : Boolean = {
    val ret = matchesAttribute(exp, carbonDecoderRelations)
    if (ret.isDefined) {
      val matchedAttribute = ret.get
      lazy val newGeneratedExprId = NamedExpression.newExprId

      // add this complex field reference to map
      val value = replaceMap.get(matchedAttribute.exprId)
      // replaces alias name, example: a#10.b converted to a.b
      val expRefString = exp.toString().split("\\.")
        .map(_.replaceAll("#.*", "")).mkString(".")
      val (newExprId, currentFieldReferences) = value.getOrElse((newGeneratedExprId, Set[String]()))
      replaceMap.put(matchedAttribute.exprId, (newExprId, currentFieldReferences + expRefString))
    }
    ret.isDefined
  }

  private def isFullAttributeReferenced(value: Option[(ExprId, Set[String])],
    attrib: Attribute) = value.get._2.contains(attrib.name)

  /**
   * replace complex type field Attribute reference with new ExprId
   */
  private def replaceComplexFields(plan: LogicalPlan,
    replaceMap: mutable.Map[ExprId, (ExprId, Set[String])]): LogicalPlan =
    plan transformAllExpressions {
      case attrib: AttributeReference if (attrib.dataType.isInstanceOf[StructType]) =>
        val value = replaceMap.get(attrib.exprId)
        if (value.isDefined && !isFullAttributeReferenced(value, attrib)) {
          val metadata = new MetadataBuilder().withMetadata(attrib.metadata).
            putBoolean("replaced", true).build()
          AttributeReference(attrib.name, attrib.dataType,
            attrib.nullable, metadata
          )(value.get._1, attrib.qualifier, attrib.isGenerated)
        } else {
          attrib
        }
    }

  private def replaceCarbonRelationAttributes(plan: LogicalPlan,
    replaceAttribMap: mutable.Map[ExprId, (ExprId, Set[String])]) = {
    plan transform {
      case l@LogicalRelation(relation: CarbonDatasourceHadoopRelation, _, catalogTable) =>
        var isReplaceRequired = false;
        val replacedAttribs =
          l.output.map(attrib => {
            val value = replaceAttribMap.get(attrib.exprId)
            // return new attribute reference with newExprId
            value.map {
              case (newExprId, fieldReferences) if (!isFullAttributeReferenced(value, attrib)) =>
                isReplaceRequired = true
                // attribute references stored in metadata of attribute
                val metadata = new MetadataBuilder().withMetadata(attrib.metadata)
                  .putStringArray("attribRef", fieldReferences.toArray)
                  .build()
                AttributeReference(attrib.name, attrib.dataType,
                  attrib.nullable, metadata
                )(newExprId, attrib.qualifier, attrib.isGenerated)
              case _ => attrib
            }.getOrElse(attrib)
          }
          )

        if (isReplaceRequired) {LogicalRelation(relation, Some(replacedAttribs), catalogTable)}
        else {l}
    }
  }
}
