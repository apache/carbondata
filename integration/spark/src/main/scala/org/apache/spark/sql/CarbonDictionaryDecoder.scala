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
import org.apache.spark.sql.hive.{CarbonMetastoreCatalog, CarbonMetastoreTypes}
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants, QueryStatisticsRecorder}
import org.apache.carbondata.core.util.DataTypeUtil

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
      if(relation.isDefined && canBeDecoded(attr)) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension = carbonTable
          .getDimensionByName(carbonTable.getFactTableName, attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          val newAttr = AttributeReference(a.name,
            convertCarbonToSparkDataType(carbonDimension,
              relation.get.carbonRelation.carbonRelation),
            a.nullable,
            a.metadata)(a.exprId,
            a.qualifiers).asInstanceOf[Attribute]
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
        ip.attributes
          .exists(a => a.name.equalsIgnoreCase(attr.name))
      case ep: ExcludeProfile =>
        !ep.attributes
          .exists(a => a.name.equalsIgnoreCase(attr.name))
      case _ => true
    }
  }

  def convertCarbonToSparkDataType(carbonDimension: CarbonDimension,
      relation: CarbonRelation): types.DataType = {
    carbonDimension.getDataType match {
      case DataType.STRING => StringType
      case DataType.SHORT => ShortType
      case DataType.INT => IntegerType
      case DataType.LONG => LongType
      case DataType.DOUBLE => DoubleType
      case DataType.BOOLEAN => BooleanType
      case DataType.DECIMAL =>
        val scale: Int = carbonDimension.getColumnSchema.getScale
        val precision: Int = carbonDimension.getColumnSchema.getPrecision
        if (scale == 0 && precision == 0) {
          DecimalType(18, 2)
        } else {
          DecimalType(precision, scale)
        }
      case DataType.TIMESTAMP => TimestampType
      case DataType.STRUCT =>
        CarbonMetastoreTypes
        .toDataType(s"struct<${ relation.getStructChildren(carbonDimension.getColName) }>")
      case DataType.ARRAY =>
        CarbonMetastoreTypes
        .toDataType(s"array<${ relation.getArrayChildren(carbonDimension.getColName) }>")
    }
  }

  val getDictionaryColumnIds = {
    val attributes = child.output
    val dictIds: Array[(String, ColumnIdentifier, DataType)] = attributes.map { a =>
      val attr = aliasMap.getOrElse(a, a)
      val relation = relations.find(p => p.contains(attr))
      if(relation.isDefined && canBeDecoded(attr)) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension =
          carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          (carbonTable.getFactTableName, carbonDimension.getColumnIdentifier,
              carbonDimension.getDataType)
        } else {
          (null, null, null)
        }
      } else {
        (null, null, null)
      }

    }.toArray
    dictIds
  }

  override def outputsUnsafeRows: Boolean = true

  override def canProcessUnsafeRows: Boolean = true

  override def canProcessSafeRows: Boolean = true



  override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val storePath = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog].storePath
      val queryId = sqlContext.getConf("queryId", System.nanoTime() + "")
      val absoluteTableIdentifiers = relations.map { relation =>
        val carbonTable = relation.carbonRelation.carbonRelation.metaData.carbonTable
        (carbonTable.getFactTableName, carbonTable.getAbsoluteTableIdentifier)
      }.toMap
      val recorder = new QueryStatisticsRecorder(queryId)
      if (isRequiredToDecode) {
        val dataTypes = child.output.map { attr => attr.dataType }
        child.execute().mapPartitions { iter =>
          val cacheProvider: CacheProvider = CacheProvider.getInstance
          val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
            cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, storePath)
          val dicts: Seq[Dictionary] = getDictionary(absoluteTableIdentifiers,
            forwardDictionaryCache)
          val dictIndex = dicts.zipWithIndex.filter(x => x._1 != null).map(x => x._2)
          new Iterator[InternalRow] {
            val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
            var flag = true
            var total = 0L
            override final def hasNext: Boolean = {
              flag = iter.hasNext
              if (false == flag && total > 0) {
                val queryStatistic = new QueryStatistic()
                queryStatistic
                  .addFixedTimeStatistic(QueryStatisticsConstants.PREPARE_RESULT, total)
                recorder.recordStatistics(queryStatistic)
                recorder.logStatistics()
              }
              flag
            }
            override final def next(): InternalRow = {
              val startTime = System.currentTimeMillis()
              val row: InternalRow = iter.next()
              val data = row.toSeq(dataTypes).toArray
              dictIndex.foreach { index =>
                if (data(index) != null) {
                  data(index) = DataTypeUtil.getDataBasedOnDataType(dicts(index)
                      .getDictionaryValueForKey(data(index).asInstanceOf[Int]),
                      getDictionaryColumnIds(index)._3)
                }
              }
              val result = unsafeProjection(new GenericMutableRow(data))
              total += System.currentTimeMillis() - startTime
              result
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

  private def getDictionary(atiMap: Map[String, AbsoluteTableIdentifier],
      cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = getDictionaryColumnIds.map { f =>
      if (f._2 != null) {
        try {
          cache.get(new DictionaryColumnUniqueIdentifier(
            atiMap.get(f._1).get.getCarbonTableIdentifier,
            f._2, f._3))
        } catch {
          case _ => null
        }
      } else {
        null
      }
    }
    dicts
  }
}
