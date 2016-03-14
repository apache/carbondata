/**
  *
  */
package com.huawei.datasight.spark

import java.util.ArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import com.huawei.unibi.molap.constants.DataSetUtil
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.unibi.molap.engine.holders.MolapResultHolder
import com.huawei.datasight.spark.rdd.MolapDataRDDFactory
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.OlapContext
import org.apache.spark.sql.Row
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import com.huawei.unibi.molap.engine.aggregator.impl._
import scala.collection.mutable.HashMap
import com.huawei.datasight.spark.dsl.interpreter.EvaluatorAttributeModel
import com.huawei.datasight.spark.dsl.interpreter.MolapSparkInterpreter
import org.apache.spark.HttpServer
import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.spark.sql.SparkHttpServer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.OlapRelation
import com.huawei.datasight.spark.processors.OlapUtil
import com.huawei.datasight.spark.agg.CountMolap
import com.huawei.unibi.molap.query.MolapQuery
import com.huawei.unibi.molap.engine.directinterface.impl.MolapQueryParseUtil
import com.huawei.datasight.spark.processors._
import com.huawei.unibi.molap.query.impl.MolapQueryImpl
import com.huawei.unibi.molap.query.metadata.DSLTransformation
import com.huawei.unibi.molap.query.metadata.Dataset
import java.util.List
import org.apache.spark.rdd.RDD
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory
import com.huawei.datasight.spark.processors.TransformHolder
import java.lang.Double
import java.lang.Boolean
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import com.huawei.datasight.spark.rdd.SchemaRDDExt

//import org.apache.spark.sql.execution.SparkLogicalPlan
import org.apache.spark.sql.csv.CsvRDD
import com.huawei.unibi.molap.engine.directinterface.impl.MolapQueryModel
import com.huawei.datasight.spark.processors.MolapDerivedInfo
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.processors.AddCount
import java.sql.DriverManager
import java.sql.ResultSet
import org.apache.spark.sql.jdbc.JdbcResultSetRDD
import org.apache.spark.sql.jdbc.JdbcResultSetRDD
import com.huawei.unibi.molap.metadata.MolapMetadata
import com.huawei.unibi.molap.util.MolapProperties
import com.huawei.datasight.molap.load.MolapLoadModel
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import com.huawei.iweb.platform.logging.LogServiceFactory
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent
import com.huawei.unibi.molap.engine.filters.measurefilter.GroupMeasureFilterModel

//import mondrian.olap.MondrianDef
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.util.MolapSchemaParser
import org.apache.spark.sql.catalyst.expressions.GenericRow
import com.huawei.datasight.molap.partition.api.impl.QueryPartitionHelper
import org.apache.spark.sql.OlapMetastoreCatalog
import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap
import it.unimi.dsi.fastutil._
import it.unimi.dsi.fastutil.booleans.BooleanArrays
import it.unimi.dsi.fastutil.objects._
import it.unimi.dsi.fastutil.doubles.AbstractDouble2ObjectMap
import org.apache.spark.sql.DataFrame
import com.huawei.unibi.molap.locks.MetadataLock
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent

/**
  * @author R00900208
  *
  */

object SparkQueryExecutor {

  def convert(result: Row): Array[Object] = {

    val length = result.length

    val bakArray = Array.ofDim[Object](length)

    for (j <- 0 until length) {
      bakArray(j) = typeCast(result.apply(j))
    }

    return bakArray;
  }

  def flattenRow(result: Row): Row = {
    new GenericRow(result.toSeq.map(x => typeCast(x).asInstanceOf[Any]).toArray)
  }

  def typeCast(value: Any): Object = {
    value match {
      case m: MeasureAggregator => m.getValue().asInstanceOf[Object]
      case _ => value.asInstanceOf[Object]
    }
  }

  def convert(result: (MolapKey, MolapValue)): Array[Object] = {

    // val holder = new MolapResultHolder(new ArrayList())

    val keyLength = result._1.getKey().length
    val valLength = result._2.getValues().length

    val bakArray = Array.ofDim[Object](keyLength + valLength)

    val keyArray = result._1.getKey();
    val valArray = result._2.getValues();

    for (j <- 0 until keyLength) {
      bakArray(j) = keyArray(j)
    }

    for (k <- keyLength until keyLength + valLength) {
      bakArray(k) = valArray(k).getValueObject()
    }
    bakArray
    //return holder;

  }
}

class SparkQueryExecutor(sparkUrl: String, sparkBasePath: String, schemaContent: MolapDef.Schema, metadatapath: String) extends Serializable {
  def this(sparkUrl: String, sparkBasePath: String, schemaPath: String, metadatapath: String) {
    this(sparkUrl, sparkBasePath, MolapSchemaParser.loadXML(schemaPath), metadatapath);
  }

  val LOGGER = LogServiceFactory.getLogService(SparkQueryExecutor.getClass().getName())

  //  var sc:SparkContext = null;

  //var olapContext:OlapContext = null;
  @transient var sc: SparkContext = null;

  @transient var olapContext: OlapContext = null;

  @transient val LOGGER1 = LogServiceFactory.getLogService("com.huawei.datasight.spark.SparkQueryExecutor");

  val schemaName = schemaContent.name

  val cubeName = schemaContent.cubes(0).name

  val columinar = MolapProperties.getInstance().getProperty("molap.is.columnar.storage", "false")

  def init(): Unit = init(null)

  def init(externalSC: SparkContext = null) = {
    createContext(externalSC)
    val relation = CarbonEnv.getInstance(olapContext).carbonCatalog.lookupRelation1(Option(schemaName), cubeName, None)(null).asInstanceOf[OlapRelation]
    MolapDataRDDFactory.intializeMolap(sc, relation.cubeMeta.schema,
      relation.cubeMeta.dataPath, cubeName, schemaName, relation.cubeMeta.partitioner, columinar.toBoolean)
  }

  def initLoader(molapLoadModel: MolapLoadModel,
                 externalSC: SparkContext = null,
                 localStorePath: String = null,
                 kettleHomePath: String) = {
    createContext(externalSC)
    Thread.sleep(5000);
    val relation = CarbonEnv.getInstance(olapContext).carbonCatalog.lookupRelation1(Option(schemaName), cubeName, None)(null).asInstanceOf[OlapRelation]
    var localStoreLocation = localStorePath;
    if (relation.cubeMeta.dataPath != null && relation.cubeMeta.dataPath.startsWith("hdfs://")) {
      if (localStoreLocation == null) {
        localStoreLocation = "/tmp/";
      }
    }

    molapLoadModel.setSchema(schemaContent);
    val partition = relation.cubeMeta.partitioner
    val molapLock = new MetadataLock(MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName).getMetaDataFilepath())
    try {
      MolapDataRDDFactory.loadMolapData(null, molapLoadModel, localStoreLocation, relation.cubeMeta.dataPath, kettleHomePath, partition, columinar.toBoolean, false);
    } finally {
      if (molapLock != null) {
        if (molapLock.unlock()) {
          LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
            "Cube MetaData Unlocked Successfully after data load")
        } else {
          LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
            "Unable to unlock Cube MetaData")
        }
      }
    }
    //olapContext = new OlapContext(sc,schemaPath)

  }

  def createContext(externalSC: SparkContext = null) {

    if (sc == null) {
      val d = SparkContext.jarOfClass(this.getClass)
      val ar = new Array[String](d.size)
      var i = 0
      d.foreach {
        p =>
          ar(i) = p;
          i = i + 1
      }

      val classServer = SparkHttpServer.init
      //    sc = new SparkContext(sparkUrl, "Molap Spark Query", sparkBasePath, ar)
      var conf = new SparkConf()
      MolapProperties.getInstance().getAllProperties().filter(_.startsWith("spark")).foreach { f =>
        println(f)
        conf = conf.set(f, MolapProperties.getInstance().getProperty(f))
      }

      conf = conf.setMaster(sparkUrl)
        .setJars(ar)
        .set("spark.repl.class.uri", classServer.uri)
        .set("spark.executor.memory", MolapProperties.getInstance().getProperty("spark.executor.memory", "4000m"))
        .set("spark.ui.port", MolapProperties.getInstance().getProperty("spark.ui.port", "4041"))
        .set("spark.akka.heartbeat.interval", MolapProperties.getInstance().getProperty("spark.akka.heartbeat.interval", "100"))
        .set("spark.cores.max", MolapProperties.getInstance().getProperty("spark.cores.max", "40"))
        .set("spark.yarn.secondary.jars", MolapProperties.getInstance().getProperty("spark.yarn.secondary.jars", "/opt/spark-1.0.0-rc3/lib/sparkmolapjars/*"))
        //.set("spark.eventLog.enabled", MolapProperties.getInstance().getProperty("spark.eventLog.enabled", "false"))
        .setAppName("Molap Spark Query")
        .set("spark.hadoop.dfs.client.domain.socket.data.traffic", "false")
        .set("spark.hadoop.dfs.client.read.shortcircuit", "true")
        .set("spark.hadoop.dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket")
        .set("spark.hadoop.dfs.block.local-path-access.user", "root,hadoop")
      //.set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

      if (MolapProperties.getInstance().getProperty("spark.usekryo.serializer", "true").equals("true")) {
        conf = conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "com.huawei.datasight.spark.MyRegistrator")
          .set("spark.kryoserializer.buffer.mb", MolapProperties.getInstance().getProperty("spark.kryoserializer.buffer", "100m"))
      }
      if (MolapProperties.getInstance().getProperty("spark.yarn.dist.archives") != null) {
        conf = conf.set("spark.yarn.dist.archives", MolapProperties.getInstance().getProperty("spark.yarn.dist.archives"))
      }
      //      .set("spark.sql.shuffle.partitions", "20")
      //      .set("mapred.reduce.tasks", "20")
      //      .setSparkHome(sparkBasePath)

      if (externalSC == null) {
        sc = new SparkContext(conf)
      }
      else {
        sc = externalSC;
        sc.getConf.setAll(conf.getAll);
      }

      // This wait/sleep is required so that all executors show up in spark context object.
      // @see OlapMetastoreCatalog.getNodesList()
      // TODO Check if the bug is fixed in later versions of spark
      val warmUpTime = MolapProperties.getInstance().getProperty("molap.spark.warmUpTime", "30000");
      println("Sleeping for millisecs:" + warmUpTime);
      try {
        Thread.sleep(Integer.parseInt(warmUpTime));
      }
      catch {
        case _ => {
          println("Wrong value for molap.spark.warmUpTime " + warmUpTime + "Using default Value and proceeding"); Thread.sleep(30000);
        }
      }


      val molapjars = MolapProperties.getInstance().getProperty("molap.spark.yarn.user.jars", "/opt/spark-1.0.0-rc3/lib/sparkmolapjars/*")

      println("adding MOLAP JARS..." + molapjars)
      molapjars.split(',').foreach(sc.addJar(_))
      olapContext = new OlapContext(sc, metadatapath)

      olapContext.setConf("spark.sql.shuffle.partitions", MolapProperties.getInstance().getProperty("spark.sql.shuffle.partitions", "40"))
      olapContext.setConf("molap.is.columnar.storage", columinar)
      //  if(!olapContext.cubeExists(schemaName, cubeName)) {
      //  val partition = QueryPartitionHelper.getInstance().getPartitioner(schemaName, cubeName)
      // CarbonEnv.carbonCatalog.loadCube(schemaContent, true, partition)
      //}
    }
  }

  def execute(query: MolapQuery): MolapResultHolder = {
    val startTime = System.currentTimeMillis()
    //    val result = MolapDataRDDFactory.newMolapDataDirectSqlRDD(sc, sql, new Configuration(), schemaPath)//BigDataRDDFactory.newBigDataDirectSqlRDD(sc, sql, conf)

    LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor olapContext time taken (ms) : " + (System.currentTimeMillis() - startTime))
    val context = olapContext
    import context._
    val model = MolapQueryParseUtil.parseMolapQuery(query, schemaName, cubeName)
    LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor MolapQueryParseUtil time taken (ms) : " + (System.currentTimeMillis() - startTime))
    //sample data set
    //var datasetList = Seq(new Dataset(Dataset.DatasetType.REPORT_DATA_EXPORT,"ds1",Nil))

    var datasetList = query.asInstanceOf[MolapQueryImpl].getExtraProperties().get("DATA_SETS").asInstanceOf[List[Dataset]]
    if (datasetList != null) {
      initalizeDataSetRDDs(olapContext, query, datasetList)
      LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor initalizeDataSetRDDs time taken (ms) : " + (System.currentTimeMillis() - startTime))
    }
    var molapDerivedInfo = identifyExtraCols(model)

    var baseRDD: TransformHolder = OlapUtil.createBaseRDD(olapContext, model.getCube())
    baseRDD = AddProjection(olapContext, model, molapDerivedInfo).transform(baseRDD)
    baseRDD = AddPredicate(olapContext, model).transform(baseRDD)
    baseRDD = AddGroupBy(olapContext, model, molapDerivedInfo).transform(baseRDD)
    //Base the RDD
    baseRDD = TransformHolder(baseRDD.rdd.asInstanceOf[SchemaRDD].as('curr), baseRDD.mataData)

    baseRDD = AddDSL(olapContext, model).transform(baseRDD)
    LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor DSL time taken (ms) : " + (System.currentTimeMillis() - startTime))
    baseRDD = AddCount(olapContext, model).transform(baseRDD)

    baseRDD = AddTopN(olapContext, model).transform(baseRDD)
    baseRDD = AddFinalSelectionGroupOrderBy(olapContext, model).transform(baseRDD)
    val sColTime = System.currentTimeMillis()

    LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, baseRDD.rdd.asInstanceOf[DataFrame].schema.toString())

    val data = collect(baseRDD.rdd)
    LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor collection time taken (ms) : " + (System.currentTimeMillis() - sColTime))
    val savePath = query.asInstanceOf[MolapQueryImpl].getExtraProperties().get(MolapQuery.DATA_SET_PATH).asInstanceOf[String]
    val dims = model.getQueryDims().map(_.getName())
    val msrs = model.getMsrs.map(_.getName())
    val calMsrs = model.getCalcMsrs.map(_.getName())
    var alCols = dims
    if (model.getMolapTransformations() != null) {
      val trans = model.getMolapTransformations().filter(_.isAddAsColumn()).map(_.getNewColumnName())
      alCols = alCols ++ trans
    }
    alCols = alCols ++ msrs ++ calMsrs
    if (savePath != null)
      saveAsCSVDataSet(data, savePath, alCols)
    val holder = new MolapResultHolder(new ArrayList())
    holder.setObject(transposeResult(data))
    holder.setColHeaders(alCols)
    LOGGER1.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor Exactly time taken (ms) : " + (System.currentTimeMillis() - startTime))
    holder
  }

  def execute(sqlString: String): Array[Row] = {

    //    val result = MolapDataRDDFactory.newMolapDataDirectSqlRDD(sc, sql, new Configuration(), schemaPath)//BigDataRDDFactory.newBigDataDirectSqlRDD(sc, sql, conf)
    val olapContext = new OlapContext(sc, metadatapath)
    import olapContext._

    //sample data set
    //var datasetList = Seq(new Dataset(Dataset.DatasetType.REPORT_DATA_EXPORT,"ds1",Nil))
    val metadata = MolapMetadata.getInstance();
    val cubeUniqueName = schemaName + '_' + cubeName;
    val cube = metadata.getCube(cubeUniqueName);
    if (cube == null) {
      throw new RuntimeException("Scheme or Cube name does not exist");
    }

    var baseRDD: TransformHolder = OlapUtil.createBaseRDD(olapContext, cube)

    baseRDD = TransformHolder(olapContext.mSql(sqlString), baseRDD.mataData)

    val data = collect(baseRDD.rdd)
    //   val result = transposeResult(data)

    return genericResult(data)
    //   val holder = new MolapResultHolder(new ArrayList())
    //   holder.setObject(transposeResult(data))
    //   holder
  }


  def initalizeDataSetRDDs(olapContext: OlapContext, query: MolapQuery, datasets: List[Dataset]) =
    if (datasets != null) {
      datasets.foreach(createDataSetRDD(olapContext, _))
    }


  def createDataSetRDD(olapContext: OlapContext, dataset: Dataset) = {
    try {
      val plan = olapContext.table(dataset.getTitle())
    } catch {
      case _ =>
        dataset.getType() match {

          case Dataset.DatasetType.REPORT_DATA_EXPORT =>
            val filePath = DataSetUtil.DATA_SET_LOCATION +
              dataset.getTitle() +
              Dataset.DATA_SET_EXTENSION
            val csvSchemaRDD = olapContext.csvFile(filePath, ",", '"', null, true)
            csvSchemaRDD.registerTempTable(dataset.getTitle())

          case Dataset.DatasetType.LOCAL_FILE => Nil
          case Dataset.DatasetType.REPORT_LINK_EXPORT => Nil
          case Dataset.DatasetType.DB_SQL =>
            val url = dataset.getProperty(Dataset.DB_URL).asInstanceOf[String] // "jdbc:mysql://10.18.51.88:3306/test"
          val username = dataset.getProperty(Dataset.DB_USER_NAME).asInstanceOf[String] //  "root"
          val password = dataset.getProperty(Dataset.DB_PASSWORD).asInstanceOf[String] // "password"
          val sql_query = dataset.getProperty("SQL").asInstanceOf[String] //"select name, value  from TerritoryInfo"
          var sqlqueryRDD = olapContext.jdbcResultSet(url, username, password, sql_query, 1, 1, 1)

            sqlqueryRDD.registerTempTable(dataset.getTitle())
        }
    }
  }


  def identifyExtraCols(queryModel: MolapQueryModel): MolapDerivedInfo = {
    var extraDimColNames: Buffer[String] = Buffer()
    var extraMsrColNames: Buffer[String] = Buffer()
    val dims: Set[String] = queryModel.getQueryDims.map(_.getName()).toSet
    val msrs: Set[String] = queryModel.getMsrs.map(_.getName()).toSet

    val molapTransformations: List[DSLTransformation] = queryModel.getMolapTransformations()
    if (molapTransformations != null) {
      molapTransformations.map(x =>
        findcols(x.getDslExpression()).map { colName =>
          var cube = queryModel.getCube()
          //if it is dimension and not already present in base query
          if (MolapQueryUtil.getMolapDimension(cube.getDimensions(cube.getFactTableName()), colName) != null && !dims.contains(colName))
            if (!extraDimColNames.contains(colName))
              extraDimColNames = extraDimColNames :+ colName
            //if it is measure and not already present in base query
            else if (cube.getMeasure(cube.getFactTableName(), colName) != null && !msrs.contains(colName))
              if (!extraMsrColNames.contains(colName))
                extraMsrColNames = extraMsrColNames :+ colName
        }
      )
    }
    MolapDerivedInfo(extraDimColNames, extraMsrColNames)
  }


  def findcols(str: String): (Seq[String]) = {
    var attribs: Seq[String] = Seq[String]();
    var pattern = "\"(curr.([a-zA-Z0-9_]*))\".attr".r
    for (p <- pattern.findAllMatchIn(str)) {
      p match {
        case pattern(p1, attrib) => attribs = attribs :+ (attrib)
      }
    }
    attribs
  }

  private def saveAsCSVDataSet(result: Array[Array[Object]], path: String, alCols: Buffer[String]) {
    if (result.length > 0) {
      val length = result(0).length
      val fileType = FileFactory.getFileType(path)
      val stream = FileFactory.getDataOutputStream(path, fileType)
      val header = new StringBuffer()
      alCols.map(x => header.append(x).append(","))
      header.deleteCharAt(header.length() - 1)
      header.append("\n")
      stream.writeBytes(header.toString())
      result.map { row =>
        val buffer = new StringBuffer()
        row.map(x => buffer.append(x).append(","))
        buffer.deleteCharAt(buffer.length() - 1).append("\n")
        stream.writeBytes(buffer.toString())
      }
      stream.close()
    }
  }

  private def collect(rdd: Any): Array[Array[Object]] = {
    rdd match {
      case sr: SchemaRDD =>
        //        val s = sr.mapPartitions ( iter => {
        //        iter.take(200).map(x=>convert(x)) }
        //        ,true)
        val s = sr.map(x => SparkQueryExecutor.convert(x))
        s.collect
      case r: RDD[Row] =>
        val s = r.map(x => SparkQueryExecutor.convert(x))
        s.collect
    }
  }


  def transposeResult(inputResult: Array[Array[Object]]): Array[Array[Object]] = {
    if (inputResult.size > 0) {

      val columnLength = inputResult(0).length
      val result = Array.ofDim[Object](columnLength, inputResult.size)
      for (i <- 0 until inputResult.size) {
        for (j <- 0 until inputResult(i).size) {
          result(j)(i) = inputResult(i)(j)
        }
      }
      result
    } else {
      null
    }
  }


  def genericResult(inputResult: Array[Array[Object]]): Array[Row] = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.catalyst.expressions.GenericRow
    if (inputResult.size > 0) {

      val columnLength = inputResult.length
      val result2 = Array.ofDim[Row](columnLength)
      for (i <- 0 until inputResult.length) {
        result2(i) = new GenericRow(inputResult(i).asInstanceOf[Array[Any]]) // .create(inputResult(i))
        //        result2(i).set
        //for(j <- 0 until result(i).size) {
        //result2(j)(i) = result(i)(j)
        //}
      }
      return result2;
      //   show(result2, Array("Year","Traffic"))

    }

    return null
  }


  // private def convert(result : Array[Row]) : MolapResultHolder = {
  //    
  //    val holder = new MolapResultHolder(new ArrayList())
  //    
  //    if(result.length > 0)
  //    {
  //    	val length = result(0).length
  //      
  //    	val bakArray = Array.ofDim[Object](length, result.length)
  //    	
  //    	for(i<-0 until result.length)
  //    	{
  //    	  
  //    	  for(j<-0 until length)
  //    	  {
  //    	    bakArray(j)(i) = typeCast(result(i).apply(j))
  //    	  }
  //
  //    	}
  //    	holder.setObject(bakArray)
  //    }
  //    return holder;
  //  }


}


class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[MeasureAggregator])
    kryo.register(classOf[AvgAggregator])
    kryo.register(classOf[CountAggregator])
    kryo.register(classOf[DistinctCountAggregator])
    kryo.register(classOf[DistinctStringCountAggregator])
    kryo.register(classOf[MaxAggregator])
    kryo.register(classOf[MinAggregator])
    kryo.register(classOf[SumAggregator])
    kryo.register(classOf[GroupMeasureFilterModel])
    kryo.register(classOf[Double2ObjectOpenHashMap[Object]])
    kryo.register(classOf[Hash])
    kryo.register(classOf[HashCommon])
    kryo.register(classOf[BooleanArrays])
    kryo.register(classOf[ObjectCollection[Object]])
    kryo.register(classOf[AbstractObjectCollection[Object]])
    kryo.register(classOf[ObjectIterator[Object]])
    kryo.register(classOf[ObjectArrays])
    kryo.register(classOf[AbstractObjectSet[Object]])
    kryo.register(classOf[AbstractDouble2ObjectMap[Object]])
  }
}