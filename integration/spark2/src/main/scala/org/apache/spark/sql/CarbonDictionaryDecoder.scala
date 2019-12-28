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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.optimizer.CarbonDecoderRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{SparkSQLUtil, SparkTypeConverter}
import org.apache.spark.util.SerializableConfiguration

import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.ColumnIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.scan.executor.util.QueryUtil
import org.apache.carbondata.core.util.{DataTypeUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.CarbonAliasDecoderRelation
import org.apache.carbondata.spark.rdd.CarbonRDDWithTableInfo

/**
 * It decodes the data.
 *
 */
case class CarbonDictionaryDecoder(
    relations: Seq[CarbonDecoderRelation],
    profile: CarbonProfile,
    aliasMap: CarbonAliasDecoderRelation,
    child: SparkPlan,
    sparkSession: SparkSession)
  extends UnaryExecNode with CodegenSupport {

  override val output: Seq[Attribute] =
    CarbonDictionaryDecoder.convertOutput(child.output, relations, profile, aliasMap)


  override def outputPartitioning: Partitioning = {
    child.outputPartitioning
  }

  val getDictionaryColumnIds: Array[(String, ColumnIdentifier, CarbonDimension)] =
    CarbonDictionaryDecoder.getDictionaryColumnMapping(child.output, relations, profile, aliasMap)

  val broadcastConf = SparkSQLUtil.broadCastHadoopConf(
    sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())

  override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val tableNameToCarbonTableMapping = relations.map { relation =>
        val carbonTable = relation.carbonRelation.carbonRelation.metaData.carbonTable
        (carbonTable.getTableName, carbonTable)
      }.toMap

      if (CarbonDictionaryDecoder.isRequiredToDecode(getDictionaryColumnIds)) {
        val dataTypes = child.output.map { attr => attr.dataType }
        child.execute().mapPartitions { iter =>
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(broadcastConf.value.value)
          val cacheProvider: CacheProvider = CacheProvider.getInstance
          val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
            cacheProvider.createCache(CacheType.FORWARD_DICTIONARY)
          val dicts: Seq[Dictionary] = getDictionary(tableNameToCarbonTableMapping,
            forwardDictionaryCache)
          val dictIndex = dicts.zipWithIndex.filter(x => x._1 != null).map(x => x._2)
          // add a task completion listener to clear dictionary that is a decisive factor for
          // LRU eviction policy
          val dictionaryTaskCleaner = TaskContext.get
          dictionaryTaskCleaner.addTaskCompletionListener(_ =>
            dicts.foreach { dictionary =>
              if (null != dictionary) {
                dictionary.clear()
              }
            }
          )
          new Iterator[InternalRow] {
            val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
            var flag = true
            var total = 0L

            override final def hasNext: Boolean = iter.hasNext

            override final def next(): InternalRow = {
              val row: InternalRow = iter.next()
              val data = row.toSeq(dataTypes).toArray
              dictIndex.foreach { index =>
                if (data(index) != null) {
                  data(index) = DataTypeUtil.getDataBasedOnDataType(dicts(index)
                    .getDictionaryValueForKeyInBytes(data(index).asInstanceOf[Int]),
                    getDictionaryColumnIds(index)._3)
                }
              }
              unsafeProjection(new GenericInternalRow(data))
            }
          }
        }
      } else {
        child.execute()
      }
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    val tableNameToCarbonTableMapping = relations.map { relation =>
      val carbonTable = relation.carbonRelation.carbonRelation.metaData.carbonTable
      (carbonTable.getTableName, carbonTable)
    }.toMap

    if (CarbonDictionaryDecoder.isRequiredToDecode(getDictionaryColumnIds)) {
      val cacheProvider: CacheProvider = CacheProvider.getInstance
      val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY)
      val dicts: Seq[ForwardDictionaryWrapper] = getDictionaryWrapper(tableNameToCarbonTableMapping,
        forwardDictionaryCache, broadcastConf)

      val exprs = child.output.map { exp =>
        ExpressionCanonicalizer.execute(BindReferences.bindReference(exp, child.output))
      }
      ctx.currentVars = input
      val dictTuple = exprs.map(e => new DictTuple(null, false))
      val decodeDictionary = ctx.freshName("deDict")
      ctx.addNewFunction(decodeDictionary,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeDictionary(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           |    throws java.io.IOException {
           |  boolean isNull = false;
           |  byte[] valueIntern = dict.getDictionaryValueForKeyInBytes(surg);
           |  if (valueIntern == null ||
           |    java.util.Arrays.equals(org.apache.carbondata.core.constants
           |  .CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, valueIntern)) {
           |    isNull = true;
           |    valueIntern = org.apache.carbondata.core.constants
           |    .CarbonCommonConstants.ZERO_BYTE_ARRAY;
           |  }
           |  return new org.apache.spark.sql.DictTuple(valueIntern, isNull);
           |}""".stripMargin)

      val decodeDecimal = ctx.freshName("deDictDec")
      ctx.addNewFunction(decodeDecimal,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeDecimal(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(org.apache.spark.sql.types.Decimal.apply(new java.math.BigDecimal(
           |  new String((byte[])tuple.getValue(),
           |  org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET_CLASS))));
           |  return tuple;
           |}""".stripMargin)

      val decodeInt = ctx.freshName("deDictInt")
      ctx.addNewFunction(decodeInt,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeInt(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(Integer.parseInt(new String((byte[])tuple.getValue(),
           |    org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET_CLASS)));
           |  return tuple;
           |}""".stripMargin)
      val decodeShort = ctx.freshName("deDictShort")
      ctx.addNewFunction(decodeShort,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeShort(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(Short.parseShort(new String((byte[])tuple.getValue(),
           |    org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET_CLASS)));
           |  return tuple;
           |}""".stripMargin)
      val decodeDouble = ctx.freshName("deDictDoub")
      ctx.addNewFunction(decodeDouble,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeDouble(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(Double.parseDouble(new String((byte[])tuple.getValue(),
           |    org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET_CLASS)));
           |  return tuple;
           |}""".stripMargin)
      val decodeLong = ctx.freshName("deDictLong")
      ctx.addNewFunction(decodeLong,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeLong(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(Long.parseLong(new String((byte[])tuple.getValue(),
           |    org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET_CLASS)));
           |  return tuple;
           |}""".stripMargin)
      val decodeStr = ctx.freshName("deDictStr")
      ctx.addNewFunction(decodeStr,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeStr(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(UTF8String.fromBytes((byte[])tuple.getValue()));
           |  return tuple;
           |}""".stripMargin)
      val decodeBool = ctx.freshName("deDictBool")
      ctx.addNewFunction(decodeBool,
        s"""
           |private org.apache.spark.sql.DictTuple $decodeBool(
           |  org.apache.spark.sql.ForwardDictionaryWrapper dict, int surg)
           | throws java.io.IOException {
           |  org.apache.spark.sql.DictTuple tuple = $decodeDictionary(dict, surg);
           |  tuple.setValue(Boolean.parseBoolean(new String((byte[])tuple.getValue(),
           |  org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET_CLASS)));
           |  return tuple;
           |}""".stripMargin)


      val resultVars = exprs.zipWithIndex.map { case (expr, index) =>
        if (dicts(index) != null) {
          val ev = expr.genCode(ctx)
          val dictRef = ctx.addReferenceObj("df", dicts(index))
          val value = ctx.freshName("v")
          var code =
            s"""
               |${ev.code}
             """.stripMargin
          if (CarbonDataTypes.isDecimal(getDictionaryColumnIds(index)._3.getDataType)) {
            code +=
            s"""
               |org.apache.spark.sql.DictTuple $value = $decodeDecimal($dictRef, ${ev.value});
                 """.stripMargin
            ExprCode(code, s"$value.getIsNull()",
              s"((org.apache.spark.sql.types.Decimal)$value.getValue())")
          } else {
            getDictionaryColumnIds(index)._3.getDataType match {
              case CarbonDataTypes.INT => code +=
                s"""
                   |org.apache.spark.sql.DictTuple $value = $decodeInt($dictRef, ${ ev.value });
                 """.stripMargin
                ExprCode(code, s"$value.getIsNull()", s"((Integer)$value.getValue())")
              case CarbonDataTypes.SHORT => code +=
                s"""
                   |org.apache.spark.sql.DictTuple $value = $decodeShort($dictRef, ${ ev.value });
                 """.stripMargin
                ExprCode(code, s"$value.getIsNull()", s"((Short)$value.getValue())")
              case CarbonDataTypes.DOUBLE => code +=
                 s"""
                    |org.apache.spark.sql.DictTuple $value = $decodeDouble($dictRef, ${ ev.value });
                 """.stripMargin
                ExprCode(code, s"$value.getIsNull()", s"((Double)$value.getValue())")
              case CarbonDataTypes.LONG => code +=
                 s"""
                    |org.apache.spark.sql.DictTuple $value = $decodeLong($dictRef, ${ ev.value });
                 """.stripMargin
                ExprCode(code, s"$value.getIsNull()", s"((Long)$value.getValue())")
              case CarbonDataTypes.BOOLEAN => code +=
                s"""
                   |org.apache.spark.sql.DictTuple $value = $decodeBool($dictRef, ${ ev.value });
                 """.stripMargin
                ExprCode(code, s"$value.getIsNull()", s"((Boolean)$value.getValue())")
              case _ => code +=
                s"""
                   |org.apache.spark.sql.DictTuple $value = $decodeStr($dictRef, ${ev.value});
                 """.stripMargin
                ExprCode(code, s"$value.getIsNull()", s"((UTF8String)$value.getValue())")

            }
          }

        } else {
          expr.genCode(ctx)
        }
      }
      // Evaluation of non-deterministic expressions can't be deferred.
      s"""
         |${consume(ctx, resultVars)}
     """.stripMargin

    } else {

      val exprs = child.output.map(x =>
        ExpressionCanonicalizer.execute(BindReferences.bindReference(x, child.output)))
      ctx.currentVars = input
      val resultVars = exprs.map(_.genCode(ctx))
      // Evaluation of non-deterministic expressions can't be deferred.
      s"""
         |${consume(ctx, resultVars)}
     """.stripMargin

    }
  }


  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  private def getDictionary(atiMap: Map[String, CarbonTable],
      cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = getDictionaryColumnIds.map { f =>
      if (f._2 != null) {
        try {
          cache.get(new DictionaryColumnUniqueIdentifier(
            atiMap(f._1).getAbsoluteTableIdentifier, f._2, f._3.getDataType))
        } catch {
          case _: Throwable => null
        }
      } else {
        null
      }
    }
    dicts
  }

  private def getDictionaryWrapper(atiMap: Map[String, CarbonTable],
      cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary],
      broadcastConf: Broadcast[SerializableConfiguration]) = {
    val allDictIdentifiers = new ArrayBuffer[DictionaryColumnUniqueIdentifier]()
    val dicts: Seq[ForwardDictionaryWrapper] = getDictionaryColumnIds.map {
      case (tableName, columnIdentifier, carbonDimension) =>
        if (columnIdentifier != null) {
          try {
            val (newAbsoluteTableIdentifier, newColumnIdentifier) =
              if (null != carbonDimension.getColumnSchema.getParentColumnTableRelations &&
                  !carbonDimension
                    .getColumnSchema.getParentColumnTableRelations.isEmpty) {
                (QueryUtil
                  .getTableIdentifierForColumn(carbonDimension),
                  new ColumnIdentifier(carbonDimension.getColumnSchema
                    .getParentColumnTableRelations.get(0).getColumnId,
                    carbonDimension.getColumnProperties,
                    carbonDimension.getDataType))
              } else {
                (atiMap(tableName).getAbsoluteTableIdentifier, columnIdentifier)
              }
            val dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
              newAbsoluteTableIdentifier,
              newColumnIdentifier, carbonDimension.getDataType)
            allDictIdentifiers += dictionaryColumnUniqueIdentifier
            new ForwardDictionaryWrapper(dictionaryColumnUniqueIdentifier, broadcastConf)
          } catch {
            case _: Throwable => null
          }
        } else {
          null
        }
    }
    val dictionaryLoader = new DictionaryLoader(allDictIdentifiers.toList)
    dicts.foreach { dict =>
      if (dict != null) {
        dict.setDictionaryLoader(dictionaryLoader)
      }
    }
    dicts
  }

}

object CarbonDictionaryDecoder {

  /**
   * Converts the datatypes of attributes as per the decoder plan. If a column needs to be decoded
   * here then that datatype is updated to its original datatype from Int type.
   */
  def convertOutput(output: Seq[Attribute],
      relations: Seq[CarbonDecoderRelation],
      profile: CarbonProfile,
      aliasMap: CarbonAliasDecoderRelation): Seq[Attribute] = {
    output.map { a =>
      val attr = aliasMap.getOrElse(a, a)
      val relation = relations.find(p => p.contains(attr))
      if (relation.isDefined && canBeDecoded(attr, profile)) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension = carbonTable.getDimensionByName(attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !carbonDimension.isComplex()) {
          val newAttr = AttributeReference(a.name,
            SparkTypeConverter.convertCarbonToSparkDataType(carbonDimension.getColumnSchema,
              relation.get.carbonRelation.carbonTable),
            a.nullable,
            a.metadata)(a.exprId).asInstanceOf[Attribute]
          newAttr
        } else {
          a
        }
      } else {
        a
      }
    }
  }

  /**
   * Updates all dictionary attributes with integer datatype.
   */
  def updateAttributes(output: Seq[Attribute],
      relations: Seq[CarbonDecoderRelation],
      aliasMap: CarbonAliasDecoderRelation): Seq[Attribute] = {
    output.map { a =>
      val attr = aliasMap.getOrElse(a, a)
      val relation = relations.find(p => p.contains(attr))
      if (relation.isDefined) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension = carbonTable.getDimensionByName(attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !carbonDimension.isComplex()) {
          val newAttr = AttributeReference(a.name,
            IntegerType,
            a.nullable,
            a.metadata)(a.exprId).asInstanceOf[Attribute]
          newAttr
        } else {
          a
        }
      } else {
        a
      }
    }
  }

  /**
   * Whether the attributed requires to decode or not based on the profile.
   */
  def canBeDecoded(attr: Attribute, profile: CarbonProfile): Boolean = {
    profile match {
      case ip: IncludeProfile =>
        ip.attributes
          .exists(a => a.name.equalsIgnoreCase(attr.name) && a.exprId == attr.exprId)
      case ep: ExcludeProfile =>
        !ep.attributes
          .exists(a => a.name.equalsIgnoreCase(attr.name) && a.exprId == attr.exprId)
      case _ => true
    }
  }

  def getDictionaryColumnMapping(output: Seq[Attribute],
      relations: Seq[CarbonDecoderRelation],
      profile: CarbonProfile,
      aliasMap: CarbonAliasDecoderRelation): Array[(String, ColumnIdentifier, CarbonDimension)] = {
    output.map { attribute =>
      val attr = aliasMap.getOrElse(attribute, attribute)
      val relation = relations.find(p => p.contains(attr))
      if (relation.isDefined && CarbonDictionaryDecoder.canBeDecoded(attr, profile)) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension =
          carbonTable.getDimensionByName(attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !carbonDimension.isComplex) {
          (carbonTable.getTableName, carbonDimension.getColumnIdentifier,
            carbonDimension)
        } else {
          (null, null, null)
        }
      } else {
        (null, null, null)
      }
    }.toArray
  }

  def isRequiredToDecode(colIdents: Array[(String, ColumnIdentifier, CarbonDimension)]): Boolean = {
    colIdents.find(p => p._1 != null) match {
      case Some(_) => true
      case _ => false
    }
  }
}


class CarbonDecoderRDD(
    relations: Seq[CarbonDecoderRelation],
    profile: CarbonProfile,
    aliasMap: CarbonAliasDecoderRelation,
    val prev: RDD[InternalRow],
    output: Seq[Attribute],
    serializedTableInfo: Array[Byte])
  extends CarbonRDDWithTableInfo[InternalRow](relations.head.carbonRelation.sparkSession,
    prev,
    serializedTableInfo) {

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

  val getDictionaryColumnIds = {
    val dictIds: Array[(String, ColumnIdentifier, CarbonDimension)] = output.map { a =>
      val attr = aliasMap.getOrElse(a, a)
      val relation = relations.find(p => p.contains(attr))
      if (relation.isDefined && canBeDecoded(attr)) {
        val carbonTable = relation.get.carbonRelation.carbonRelation.metaData.carbonTable
        val carbonDimension =
          carbonTable.getDimensionByName(attr.name)
        if (carbonDimension != null &&
            carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
            !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !carbonDimension.isComplex()) {
          (carbonTable.getTableName, carbonDimension.getColumnIdentifier,
            carbonDimension)
        } else {
          (null, null, null)
        }
      } else {
        (null, null, null)
      }

    }.toArray
    dictIds
  }

  override def internalCompute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tableInfo = getTableInfo
    val tableNameToCarbonTableMapping = relations.map { _ =>
      (tableInfo.getFactTable.getTableName, CarbonTable.buildFromTableInfo(tableInfo))
    }.toMap

    val cacheProvider: CacheProvider = CacheProvider.getInstance
    val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
      cacheProvider.createCache(CacheType.FORWARD_DICTIONARY)
    val dicts: Seq[Dictionary] = getDictionary(tableNameToCarbonTableMapping,
      forwardDictionaryCache)
    val dictIndex = dicts.zipWithIndex.filter(x => x._1 != null).map(x => x._2)
    // add a task completion listener to clear dictionary that is a decisive factor for
    // LRU eviction policy
    val dictionaryTaskCleaner = TaskContext.get
    dictionaryTaskCleaner.addTaskCompletionListener(_ =>
      dicts.foreach { dictionary =>
        if (null != dictionary) {
          dictionary.clear()
        }
      }
    )
    val iter = firstParent[InternalRow].iterator(split, context)
    new Iterator[InternalRow] {
      var flag = true
      var total = 0L
      val dataTypes = output.map { attr => attr.dataType }

      override final def hasNext: Boolean = iter.hasNext

      override final def next(): InternalRow = {
        val row: InternalRow = iter.next()
        val data = row.toSeq(dataTypes).toArray
        dictIndex.foreach { index =>
          if (data(index) != null) {
            data(index) = DataTypeUtil.getDataBasedOnDataType(dicts(index)
                .getDictionaryValueForKeyInBytes(data(index).asInstanceOf[Int]),
              getDictionaryColumnIds(index)._3)
          }
        }
        new GenericInternalRow(data)
      }
    }
  }

  private def getDictionary(atiMap: Map[String, CarbonTable],
      cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = getDictionaryColumnIds.map { f =>
      if (f._2 != null) {
        try {
          cache.get(new DictionaryColumnUniqueIdentifier(
            atiMap(f._1).getAbsoluteTableIdentifier, f._2, f._3.getDataType))
        } catch {
          case _: Throwable => null
        }
      } else {
        null
      }
    }
    dicts
  }

  override protected def internalGetPartitions: Array[Partition] =
    firstParent[InternalRow].partitions
}

/**
 * It is a wrapper around Dictionary, it is a work around to keep the dictionary serializable in
 * case of codegen
 * @param dictIdentifier Dictionary column unique identifier
 * @param broadcastConf hadoop broadcast conf for serialization, that contains carbon conf.
 */
class ForwardDictionaryWrapper(
    dictIdentifier: DictionaryColumnUniqueIdentifier,
    broadcastConf: Broadcast[SerializableConfiguration]) extends Serializable {

  var dictionary: Dictionary = null

  var dictionaryLoader: DictionaryLoader = _

  def getDictionaryValueForKeyInBytes (surrogateKey: Int): Array[Byte] = {
    if (dictionary == null) {
      // Note: from doConsume() codegen, this is the first method called.
      // so setting conf to threadlocal here. If any new method added before calling this method.
      // Need to set this in that method instead of here.
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(broadcastConf.value.value)
      dictionary = dictionaryLoader.getDictionary(dictIdentifier)
    }
    dictionary.getDictionaryValueForKeyInBytes(surrogateKey)
  }

  def setDictionaryLoader(loader: DictionaryLoader): Unit = {
    dictionaryLoader = loader
  }

  def clear(): Unit = {
    if (dictionary == null) {
      dictionary = dictionaryLoader.getDictionary(dictIdentifier)
    }
    dictionary.clear()
  }
}

class DictTuple(var value: AnyRef, var isNull: Boolean) extends Serializable {

  def getValue: AnyRef = value

  def getIsNull: Boolean = isNull

  def setValue(value: AnyRef): Unit = {
    this.value = value
  }

  def setIsNull(isNull: Boolean): Unit = {
    this.isNull = isNull
  }
}

/**
 * It is Dictionary Loader class to load all dictionaries at a time instead of one by one.
 */
class DictionaryLoader(allDictIdentifiers: List[DictionaryColumnUniqueIdentifier])
  extends Serializable {

  var isDictionaryLoaded = false

  var allDicts : java.util.List[Dictionary] = _

  private def loadDictionary(): Unit = {
    if (!isDictionaryLoaded) {
      val cacheProvider: CacheProvider = CacheProvider.getInstance
      val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY)
      allDicts = forwardDictionaryCache.getAll(allDictIdentifiers.asJava)
      isDictionaryLoaded = true
      val dictionaryTaskCleaner = TaskContext.get
      if (dictionaryTaskCleaner != null) {
        dictionaryTaskCleaner.addTaskCompletionListener(_ =>
          allDicts.asScala.foreach { dictionary =>
            if (null != dictionary) {
              dictionary.clear()
            }
          }
        )
      }
    }
  }

  def getDictionary(dictIdent: DictionaryColumnUniqueIdentifier): Dictionary = {
    if (!isDictionaryLoaded) {
      loadDictionary()
    }
    val findValue = allDictIdentifiers.zipWithIndex.find(p => p._1.equals(dictIdent)).get
    allDicts.get(findValue._2)
  }

}
