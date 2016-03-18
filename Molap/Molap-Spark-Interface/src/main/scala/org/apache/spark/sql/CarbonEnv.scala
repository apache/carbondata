package org.apache.spark.sql

import com.huawei.datasight.molap.load.MolapLoaderUtil
import com.huawei.unibi.molap.metadata.MolapMetadata
import com.huawei.unibi.molap.util.MolapUtil
import org.apache.spark.Logging
import org.apache.spark.sql.hive.{HiveContext, OlapMetastoreCatalog}

/**
  * Carbon Environment for unified context
  */
class CarbonEnv extends Logging {
  var carbonContext: HiveContext = _
  var carbonCatalog: OlapMetastoreCatalog = _
  val FS_DEFAULT_FS = "fs.defaultFS"
  val HDFSURL_PREFIX = "hdfs://"
}

object CarbonEnv {
  val className = classOf[CarbonEnv].getCanonicalName
  var carbonEnv: CarbonEnv = _

  def getInstance(sqlContext: SQLContext): CarbonEnv = {
    if(carbonEnv == null)
    {
      carbonEnv = new CarbonEnv
      carbonEnv.carbonContext = sqlContext.asInstanceOf[OlapContext]
      carbonEnv.carbonCatalog = sqlContext.asInstanceOf[OlapContext].catalog
    }
    carbonEnv
  }

  var isloaded = false

  def loadCarbonCubes(sqlContext: SQLContext, carbonCatalog: OlapMetastoreCatalog): Unit = {
    val cubes = carbonCatalog.getAllCubes()(sqlContext)
    if (null != cubes && isloaded == false) {
      isloaded = true
      cubes.foreach { cube =>
        val schemaName = cube._1
        val cubeName = cube._2
        val cubeInstance = MolapMetadata.getInstance().getCube(
          schemaName + '_' + cubeName)
        val filePath = cubeInstance.getMetaDataFilepath();
        val details = MolapUtil
          .readLoadMetadata(filePath)
        if (null != details) {
          var listOfLoadFolders = MolapLoaderUtil.getListOfValidSlices(details)
          if (null != listOfLoadFolders && listOfLoadFolders.size() > 0 ) {
            var hc: HiveContext = sqlContext.asInstanceOf[HiveContext]
            hc.sql(" select count(*) from " + schemaName + "." + cubeName).collect()
          }
        }
      }
    }
  }
}


