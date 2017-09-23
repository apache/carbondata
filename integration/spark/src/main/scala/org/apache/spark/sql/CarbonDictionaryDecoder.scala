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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.{CarbonMetastore, CarbonMetastoreTypes}
import org.apache.spark.sql.optimizer.CarbonDecoderRelation
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.util.{CarbonTimeStatisticsFactory, DataTypeUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.spark.CarbonAliasDecoderRelation

/**
 * It decodes the dictionary key to value
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
      if (relation.isDefined && canBeDecoded(attr)) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension = carbonTable
          .getDimensionByName(carbonTable.getFactTableName, attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !carbonDimension.isComplex()) {
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
          .exists(a => a.name.equalsIgnoreCase(attr.name) && a.exprId == attr.exprId)
      case ep: ExcludeProfile =>
        !ep.attributes
          .exists(a => a.name.equalsIgnoreCase(attr.name) && a.exprId == attr.exprId)
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
      case DataType.DATE => DateType
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
    val dictIds: Array[(String, ColumnIdentifier, DataType, CarbonDimension)] =
      attributes.map { a =>
        val attr = aliasMap.getOrElse(a, a)
        val relation = relations.find(p => p.contains(attr))
        if (relation.isDefined && canBeDecoded(attr)) {
          val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
          val carbonDimension =
            carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
          if (carbonDimension != null &&
              carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
              !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
              !carbonDimension.isComplex()) {
            (carbonTable.getFactTableName, carbonDimension.getColumnIdentifier,
              carbonDimension.getDataType, carbonDimension)
          } else {
            (null, null, null, null)
          }
        } else {
          (null, null, null, null)
        }

      }.toArray
    dictIds
  }

  override def outputsUnsafeRows: Boolean = true

  override def canProcessUnsafeRows: Boolean = true

  override def canProcessSafeRows: Boolean = true

  override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val storePath = sqlContext.catalog.asInstanceOf[CarbonMetastore].storePath
      val queryId = sqlContext.getConf("queryId", System.nanoTime() + "")
      val absoluteTableIdentifiers = relations.map { relation =>
        val carbonTable = relation.carbonRelation.carbonRelation.metaData.carbonTable
        (carbonTable.getFactTableName, carbonTable.getAbsoluteTableIdentifier)
      }.toMap

      val recorder = CarbonTimeStatisticsFactory.createExecutorRecorder(queryId)
      if (isRequiredToDecode) {
        val dataTypes = child.output.map { attr => attr.dataType }
        val confBytes = SparkUtil.compressConfiguration(sqlContext.sparkContext.hadoopConfiguration)
        child.execute().mapPartitions { iter =>
          val cacheProvider: CacheProvider = CacheProvider.getInstance
          val hadoopConf = SparkUtil.uncompressConfiguration(confBytes)
          val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
            cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, storePath, hadoopConf)
          val dicts: Seq[Dictionary] = getDictionary(absoluteTableIdentifiers,
            forwardDictionaryCache, hadoopConf)
          val dictIndex = dicts.zipWithIndex.filter(x => x._1 != null).map(x => x._2)
          // add a task completion listener to clear dictionary that is a decisive factor for
          // LRU eviction policy
          val dictionaryTaskCleaner = TaskContext.get
          dictionaryTaskCleaner.addTaskCompletionListener(context =>
            dicts.foreach { dictionary =>
              if (null != dictionary) {
                dictionary.clear()
              }
            }
          )
          new Iterator[InternalRow] {
            val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)

            override final def hasNext: Boolean = {
              iter.hasNext
            }

            override final def next(): InternalRow = {
              val row: InternalRow = iter.next()
              val data = row.toSeq(dataTypes).toArray
              dictIndex.foreach { index =>
                if (data(index) != null) {
                  data(index) = DataTypeUtil.getDataBasedOnDataType(dicts(index)
                    .getDictionaryValueForKeyInBytes(data(index).asInstanceOf[Int]),
                    getDictionaryColumnIds(index)._4)
                }
              }
              val result = unsafeProjection(new GenericMutableRow(data))
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
      cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary], hadoopConf: Configuration) = {
    val dictionaryColumnIds = getDictionaryColumnIds.map { dictionaryId =>
      if (dictionaryId._2 != null) {
        new DictionaryColumnUniqueIdentifier(
          atiMap(dictionaryId._1).getCarbonTableIdentifier,
          dictionaryId._2, dictionaryId._3,
          CarbonStorePath.getCarbonTablePath(atiMap(dictionaryId._1), hadoopConf))
      } else {
        null
      }
    }
    try {
      val noDictionaryIndexes = new java.util.ArrayList[Int]()
      dictionaryColumnIds.zipWithIndex.foreach { columnIndex =>
        if (columnIndex._1 == null) {
          noDictionaryIndexes.add(columnIndex._2)
        }
      }
      val dict = cache.getAll(dictionaryColumnIds.filter(_ != null).toSeq.asJava);
      val finalDict = new java.util.ArrayList[Dictionary]()
      var dictIndex: Int = 0
      dictionaryColumnIds.zipWithIndex.foreach { columnIndex =>
        if (!noDictionaryIndexes.contains(columnIndex._2)) {
          finalDict.add(dict.get(dictIndex))
          dictIndex += 1
        } else {
          finalDict.add(null)
        }
      }
      finalDict.asScala
    } catch {
      case t: Throwable => Seq.empty
    }

  }

}
