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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.query.carbon.util.DataTypeUtil

/**
 * It decodes the data.
 *
 */
case class CarbonDictionaryDecoder(
    relations: Seq[CarbonDecoderRelation],
    profile: CarbonProfile,
    aliasMap: CarbonAliasDecoderRelation,
    child: SparkPlan)
  (@transient sqlContext: SQLContext)
  extends UnaryNode {


  override def otherCopyArgs: Seq[AnyRef] = sqlContext :: Nil

  override val output: Seq[Attribute] = {
    child.output.map { a =>
      val attr = aliasMap.getOrElse(a, a)
      val relation = relations.find(p => p.contains(attr))
      if(relation.isDefined) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension = carbonTable
          .getDimensionByName(carbonTable.getFactTableName, attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            canBeDecoded(attr)) {
          val newAttr = AttributeReference(a.name,
            convertCarbonToSparkDataType(carbonDimension.getDataType),
            a.nullable,
            a.metadata)(a.exprId,
            a.qualifiers).asInstanceOf[Attribute]
          newAttr.resolved
          newAttr
        } else {
          a
        }
      } else {
        a
      }
    }
  }


  def canBeDecoded(attr: Attribute): Boolean = {
    profile match {
      case ip: IncludeProfile if ip.attributes.nonEmpty =>
        ip.attributes.exists(a => a.name.equals(attr.name))
      case ep: ExcludeProfile =>
        !ep.attributes.exists(a => a.name.equals(attr.name))
      case _ => true
    }
  }

  def convertCarbonToSparkDataType(dataType: DataType): types.DataType = {
    dataType match {
      case DataType.STRING => StringType
      case DataType.INT => IntegerType
      case DataType.LONG => LongType
      case DataType.DOUBLE => DoubleType
      case DataType.BOOLEAN => BooleanType
      case DataType.DECIMAL => DecimalType.DoubleDecimal
      case DataType.TIMESTAMP => TimestampType
    }
  }

  val getDictionaryColumnIds = {
    val attributes = child.output
    val dictIds: Array[(String, String, DataType)] = attributes.map { a =>
      val attr = aliasMap.getOrElse(a, a)
      val relation = relations.find(p => p.contains(attr))
      if(relation.isDefined) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension =
          carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            canBeDecoded(attr)) {
          (carbonTable.getFactTableName, carbonDimension.getColumnId, carbonDimension.getDataType)
        } else {
          (null, null, null)
        }
      } else {
        (null, null, null)
      }

    }.toArray
    dictIds
  }

  override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val storePath = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog].storePath
      val absoluteTableIdentifiers = relations.map { relation =>
        val carbonTable = relation.carbonRelation.carbonRelation.metaData.carbonTable
        (carbonTable.getFactTableName, carbonTable.getAbsoluteTableIdentifier)
      }.toMap

      if (isRequiredToDecode) {
        val dataTypes = child.output.map { attr => attr.dataType }
        child.execute().mapPartitions { iter =>
          val cacheProvider: CacheProvider = CacheProvider.getInstance
          val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
            cacheProvider
              .createCache(CacheType.FORWARD_DICTIONARY, storePath)
          val dicts: Seq[Dictionary] = getDictionary(absoluteTableIdentifiers,
            forwardDictionaryCache)
          new Iterator[InternalRow] {
            override final def hasNext: Boolean = iter.hasNext

            override final def next(): InternalRow = {
              val row: InternalRow = iter.next()
              val data = row.toSeq(dataTypes).toArray
              for (i <- data.indices) {
                if (dicts(i) != null) {
                  data(i) = toType(DataTypeUtil
                    .getDataBasedOnDataType(dicts(i)
                      .getDictionaryValueForKey(data(i).asInstanceOf[Integer]),
                      getDictionaryColumnIds(i)._3))
                }
              }
              new GenericMutableRow(data)
            }
          }
        }
      } else {
        child.execute()
      }
    }
  }

  private def isRequiredToDecode = {
    getDictionaryColumnIds.find(p => p._1 != null) match {
      case Some(value) => true
      case _ => false
    }
  }

  private def toType(obj: Any): Any = {
    obj match {
      case s: String => UTF8String.fromString(s)
      case _ => obj
    }
  }

  private def getDictionary(atiMap: Map[String, AbsoluteTableIdentifier],
      cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = getDictionaryColumnIds.map { f =>
      if (f._2 != null) {
        cache.get(new DictionaryColumnUniqueIdentifier(
          atiMap.get(f._1).get.getCarbonTableIdentifier,
          f._2, f._3))
      } else {
        null
      }
    }
    dicts
  }
}
