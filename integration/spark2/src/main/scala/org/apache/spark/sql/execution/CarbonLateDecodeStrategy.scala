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

package org.apache.spark.sql.execution

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}
import org.apache.spark.sql.types.IntegerType

///**
// * Carbon strategy for late decode (convert dictionary key to value as late as possible), which
// * can improve the aggregation performance and reduce memory usage
// */
//private[sql] class CarbonLateDecodeStrategy extends SparkStrategy {
//  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
//    plan match {
//      case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
//        CarbonDictionaryDecoder(relations,
//          profile,
//          aliasMap,
//          planLater(child)
//        ) :: Nil
//      case _ => Nil
//    }
//  }
//    /**
//      * Create carbon scan
//     */
//  private def carbonRawScan(projectList: Seq[NamedExpression],
//      predicates: Seq[Expression],
//      logicalRelation: LogicalRelation)(sc: SQLContext): SparkPlan = {
//
//    val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
//    val tableName: String =
//      relation.carbonRelation.metaData.carbonTable.getFactTableName.toLowerCase
//    // Check out any expressions are there in project list. if they are present then we need to
//    // decode them as well.
//    val projectSet = AttributeSet(projectList.flatMap(_.references))
//    val scan = CarbonScan(projectSet.toSeq, relation.carbonRelation, predicates)
//    projectList.map {
//      case attr: AttributeReference =>
//      case Alias(attr: AttributeReference, _) =>
//      case others =>
//        others.references.map{f =>
//          val dictionary = relation.carbonRelation.metaData.dictionaryMap.get(f.name)
//          if (dictionary.isDefined && dictionary.get) {
//            scan.attributesNeedToDecode.add(f.asInstanceOf[AttributeReference])
//          }
//        }
//    }
//    if (scan.attributesNeedToDecode.size() > 0) {
//      val decoder = getCarbonDecoder(logicalRelation,
//        sc,
//        tableName,
//        scan.attributesNeedToDecode.asScala.toSeq,
//        scan)
//      if (scan.unprocessedExprs.nonEmpty) {
//        val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
//        ProjectExec(projectList, filterCondToAdd.map(FilterExec(_, decoder)).getOrElse(decoder))
//      } else {
//        ProjectExec(projectList, decoder)
//      }
//    } else {
//      ProjectExec(projectList, scan)
//    }
//  }
//
//  def getCarbonDecoder(logicalRelation: LogicalRelation,
//      sc: SQLContext,
//      tableName: String,
//      projectExprsNeedToDecode: Seq[Attribute],
//      scan: CarbonScan): CarbonDictionaryDecoder = {
//    val relation = CarbonDecoderRelation(logicalRelation.attributeMap,
//      logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
//    val attrs = projectExprsNeedToDecode.map { attr =>
//      val newAttr = AttributeReference(attr.name,
//        attr.dataType,
//        attr.nullable,
//        attr.metadata)(attr.exprId)
//      relation.addAttribute(newAttr)
//      newAttr
//    }
//    CarbonDictionaryDecoder(Seq(relation), IncludeProfile(attrs),
//      CarbonAliasDecoderRelation(), scan)(sc)
//  }
//
//  def isDictionaryEncoded(projectExprsNeedToDecode: Seq[Attribute],
//      relation: CarbonDatasourceHadoopRelation): Boolean = {
//    var isEncoded = false
//    projectExprsNeedToDecode.foreach { attr =>
//      if (relation.carbonRelation.metaData.dictionaryMap.get(attr.name).getOrElse(false)) {
//        isEncoded = true
//      }
//    }
//    isEncoded
//  }
//
//  def updateDataType(attr: AttributeReference,
//      relation: CarbonDatasourceHadoopRelation,
//      allAttrsNotDecode: java.util.Set[Attribute]): AttributeReference = {
//    if (relation.carbonRelation.metaData.dictionaryMap.get(attr.name).getOrElse(false) &&
//        !allAttrsNotDecode.asScala.exists(p => p.name.equals(attr.name))) {
//      AttributeReference(attr.name,
//        IntegerType,
//        attr.nullable,
//        attr.metadata)(attr.exprId, attr.qualifiers)
//    } else {
//      attr
//    }
//  }
//}
