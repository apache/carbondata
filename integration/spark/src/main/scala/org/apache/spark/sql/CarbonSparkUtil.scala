package org.apache.spark.sql

import org.apache.carbondata.core.carbon.metadata.encoder.Encoding

import scala.collection.JavaConverters._
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.spark.sql.hive.{CarbonMetaData, DictionaryMap}

case class TransformHolder(rdd: Any, mataData: CarbonMetaData)

object CarbonSparkUtil {

  def createSparkMeta(carbonTable: CarbonTable): CarbonMetaData = {
    val dimensionsAttr = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
        .asScala.map(x => x.getColName) // wf : may be problem
    val measureAttr = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
        .asScala.map(x => x.getColName)
    val dictionary =
      carbonTable.getDimensionByTableName(carbonTable.getFactTableName).asScala.map { f =>
        (f.getColName.toLowerCase,
            f.hasEncoding(Encoding.DICTIONARY) && !f.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
                !CarbonUtil.hasComplexDataType(f.getDataType))
      }
    CarbonMetaData(dimensionsAttr, measureAttr, carbonTable, DictionaryMap(dictionary.toMap))
  }
}