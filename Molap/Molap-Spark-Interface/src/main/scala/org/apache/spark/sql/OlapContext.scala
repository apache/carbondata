package org.apache.spark.sql

import java.sql.DriverManager
import java.sql.ResultSet
import scala.language.implicitConversions
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.JdbcRDDExt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.csv.CsvRDD
import org.apache.spark.sql.cubemodel.LoadAggregationTable
import org.apache.spark.sql.cubemodel.LoadCube
import org.apache.spark.sql.cubemodel.PartitionData
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.jdbc.JdbcResultSetRDD
import org.apache.spark.sql.types.StructType
import com.huawei.datasight.spark.rdd.CubeSchemaRDD
import com.huawei.datasight.spark.rdd.SchemaRDDExt
import com.huawei.datasight.spark.agg.FlattenExpr
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import org.apache.spark.sql.execution.joins.HashJoin
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoin
import org.apache.spark.sql.execution.joins.CartesianProduct
import org.apache.spark.sql.hive.CarbonStrategy
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.hive.CompositeMetastoreCatalog
import org.apache.spark.sql.hive.OlapStrategies
import org.apache.spark.sql.cubemodel.MergeCube
import org.apache.spark.sql.hive.HiveStrategies
import org.apache.spark.sql.hive.huawei.HuaweiStrategies
import org.apache.spark.sql.cubemodel.LoadCubeAPI
import com.huawei.datasight.spark.rdd.MolapDataFrameRDD
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.joins.HashJoin
import org.apache.spark.sql.catalyst.InternalRow
import com.huawei.iweb.platform.logging.LogServiceFactory
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent
import com.huawei.unibi.molap.engine.querystats.QueryStatsCollector
import com.huawei.unibi.molap.engine.querystats.QueryDetail

//import org.apache.spark.sql.execution.SparkStrategies.CommandStrategy
//import org.apache.spark.sql.SQLContext.SparkPlanner

/**
  * Created by w00228970 on 2014/5/16.
  */
class OlapContext(val sc: SparkContext, metadataPath: String) extends HiveContext(sc) {
  self =>

  //  @transient
  // lazy val catalog2 = new OlapMetastoreCatalog(this,metadataPath)

  var lastSchemaUpdatedTime = System.currentTimeMillis()

  //CarbonEnv.initCarbonCatalog(this)

  override lazy val catalog = {
    val catalogs = new ArrayBuffer[Catalog]
    //catalogs += CarbonEnv.carbonCatalog
    new CompositeMetastoreCatalog(conf, catalogs, metadataHive, this) with OverrideCatalog
  }

  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, conf)

  @transient
  protected[sql] val sqlDDLParser = new MolapSqlDDLParser()

  @transient
  //protected[sql] val molapSQLParser = new MolapSqlParser(HiveSupport.hiveParseSql(_))
  protected[sql] val molapSQLParser = new MolapSqlParser(getSQLDialect().parse(_))

  //  /* An analyzer that uses the Hive metastore. */
  //  @transient
  //  override protected[sql] lazy val analyzer =
  //    new Analyzer(catalog, EmptyFunctionRegistry, caseSensitive = false)

  override def parseSql(sql: String): LogicalPlan = molapSQLParser.parse(sql)

  def parseSqlDDL(sql: String): LogicalPlan = sqlDDLParser.parse(sql)

  override def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))

  override def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  def loadSchema(schemaPath: String, encrypted: Boolean = true, aggTablesGen: Boolean = true, partitioner: Partitioner = null) {
    OlapContext.updateMolapPorpertiesPath(this)
    CarbonEnv.getInstance(this).carbonCatalog.loadCube(schemaPath, encrypted, aggTablesGen, partitioner)(this)
  }

  def updateSchema(schemaPath: String, encrypted: Boolean = true, aggTablesGen: Boolean = false) {
    CarbonEnv.getInstance(this).carbonCatalog.updateCube(schemaPath, encrypted, aggTablesGen)(this)
  }


  //  def addAggregatesToCube(schemaName: String = null, cubeName: String, aggTableColumns: Seq[String]) {
  //    updateMolapPorpertiesPath
  //    val relation = this.asInstanceOf[OlapContext].catalog2.lookupRelation1(Option(schemaName), cubeName, None).asInstanceOf[OlapRelation]
  //    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
  //    val aggTableName = catalog2.getAggregateTableName(schemaName, cubeName)
  //    catalog2.updateCubeWithAggregates(relation.cubeMeta.schema, schemaName, cubeName, aggTableName, aggTableColumns)
  //    val seqOfTableName = Seq(aggTableName)
  //    this.loadAggregation(schemaName, cubeName, seqOfTableName)
  //
  //  }
  def cubeExists(schemaName: String, cubeName: String): Boolean = {
    CarbonEnv.getInstance(this).carbonCatalog.cubeExists(Seq(schemaName, cubeName))(this)
  }

  def loadData(schemaName: String = null, cubeName: String, dataPath: String, dimFilesPath: String = null) {
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = "default"
    }
    var dimFilesPathLocal = dimFilesPath
    if (dimFilesPath == null) {
      dimFilesPathLocal = dataPath
    }
    OlapContext.updateMolapPorpertiesPath(this)
    LoadCubeAPI(schemaNameLocal, cubeName, dataPath, dimFilesPathLocal, null).run(this)
  }

  def mergeData(schemaName: String = null, cubeName: String, tableName: String) {
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = cubeName
    }
    MergeCube(schemaNameLocal, cubeName, tableName).run(this)
  }


  //  def loadAggregation(schemaName: String = null, cubeName: String, aggTableNames: Seq[String]) {
  //    updateMolapPorpertiesPath
  //    var schemaNameLocal = schemaName
  //    if (schemaNameLocal == null) {
  //      schemaNameLocal = "default"
  //    }
  //    LoadAggregationTable(schemaNameLocal, cubeName, aggTableNames).run(this)
  //  }

  @DeveloperApi
  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getValue()

  /**
    * Loads a CSV file (according to RFC 4180) and returns the result as a [[SchemaRDD]].
    *
    * NOTE: If there are new line characters inside quoted fields this method may fail to
    * parse correctly, because the two lines may be in different partitions. Use
    * [[SQLContext#csvRDD]] to parse such files.
    *
    * @param path      path to input file
    * @param delimiter Optional delimiter (default is comma)
    * @param quote     Optional quote character or string (default is '"')
    * @param schema    optional StructType object to specify schema (field names and types). This will
    *                  override field names if header is used
    * @param header    Optional flag to indicate first line of each file is the header
    *                  (default is false)
    */
  def csvFile(
               path: String,
               delimiter: String = ",",
               quote: Char = '"',
               schema: StructType = null,
               header: Boolean = false): SchemaRDD = {
    val csv = sparkContext.textFile(path)
    csvRDD(csv, delimiter, quote, schema, header)
  }

  /**
    * Parses an RDD of String as a CSV (according to RFC 4180) and returns the result as a
    * [[SchemaRDD]].
    *
    * NOTE: If there are new line characters inside quoted fields, use
    * [[SparkContext#wholeTextFiles]] to read each file into a single partition.
    *
    * @param csv       input RDD
    * @param delimiter Optional delimiter (default is comma)
    * @param quote     Optional quote character of strig (default is '"')
    * @param schema    optional StructType object to specify schema (field names and types). This will
    *                  override field names if header is used
    * @param header    Optional flag to indicate first line of each file is the hader
    *                  (default is false)
    */
  def csvRDD(
              csv: RDD[String],
              delimiter: String = ",",
              quote: Char = '"',
              schema: StructType = null,
              header: Boolean = false): SchemaRDD = {
    new SchemaRDD(this, CsvRDD.inferSchema(csv, delimiter, quote, schema, header)(this))
  }

  /**
    * Creates a SchemaRDD from an RDD of case classes.
    *
    * @group userf
    */

  /*  override implicit def createDataFrame[A <: Product: TypeTag](rdd: RDD[A]) = {
      SparkPlan.currentContext.set(self)
      val attributeSeq = ScalaReflection.attributesFor[A]
      val schema = StructType.fromAttributes(attributeSeq)
      val rowRDD = RDDConversions.productToRowRdd(rdd, schema.map(_.dataType))
      new SchemaRDDExt(this,
          LogicalRDD(attributeSeq, rowRDD)(self))
    }
    override implicit def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
      SparkPlan.currentContext.set(self)
      val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
      val attributeSeq = schema.toAttributes
      val rowRDD = RDDConversions.productToRowRdd(rdd, schema.map(_.dataType))
      DataFrame(self, LogicalRDD(attributeSeq, rowRDD)(self))
    }*/

  /**
    * Creates a SchemaRDD from an RDD of case classes.
    *
    * @group userf
    */
  implicit def createSchemaExtRDD(rdd: SchemaRDD) =
    new SchemaRDDExt(rdd.sqlContext, rdd.logicalPlan)


  def mSql(sql: String): SchemaRDD = {
    OlapContext.updateMolapPorpertiesPath(this)
    val result = {
      if (sql.toUpperCase.startsWith("CREATE ") || sql.toUpperCase.startsWith("LOAD ")) {
        new SchemaRDD(this, parseSqlDDL(sql))
      } else {
        new SchemaRDD(this, parseSql(sql))
      }
    }


    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result.queryExecution.toRdd
    result
  }

  override def sql(sql: String): SchemaRDD = {


    //queryId will be unique for each query, creting query detail holder
    val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
    val queryId: String = System.nanoTime() + ""
    val queryDetail: QueryDetail = new QueryDetail(queryId)
    queryStatsCollector.addQueryStats(queryId, queryDetail)
    this.setConf("queryId", queryId)

    OlapContext.updateMolapPorpertiesPath(this)
    val sqlString = sql.toUpperCase
    val LOGGER = LogServiceFactory.getLogService(OlapContext.getClass().getName())
    LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, s"Query [$sqlString]")
    val logicPlan: LogicalPlan = parseSqlDDL(sql)
    //val result = new SchemaRDD(this,logicPlan)
    val result = new MolapDataFrameRDD(sql: String, this, logicPlan)
    //    {
    //      if(sqlString.startsWith("CREATE ") || sqlString.startsWith("LOAD ")
    //          || sqlString.startsWith("USE ") || sqlString.startsWith("SHOW ")
    //          || sqlString.startsWith("DESCRIBE ") || sqlString.startsWith("DESC ")) {
    //        new SchemaRDD(this, parseSqlDDL(sql))
    //      } else {
    //        new SchemaRDD(this, parseSql(sql))
    //      }
    //    }

    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    //    result.queryExecution.toRdd
    result

    //    flattenRDD(result)
  }

  /**
    * All the measure objects inside SchemaRDD will be flattened
    */

  def flattenRDD(rdd: SchemaRDD) = {
    val fields = rdd.schema.fields.map { f =>
      new Column(FlattenExpr(UnresolvedAttribute(f.name)))
    }
    println(fields)
    rdd.as(Symbol("olap_flatten")).select(fields: _*)
  }

  /** Returns the specified table as a SchemaRDD ,
    * it can be used to test the performance of olapspark and sparksql
    * */
  //  override def table(tableName: String): SchemaRDD =
  //    new SchemaRDD(this, catalog.lookupRelation(None, tableName))

  implicit def dataset(name: String): SchemaRDDExt = {
    table(name).as(Symbol(name))
  }

  /** Returns the specified cube rdd for cube operations
    * */
  def cube(cubeName: String): SchemaRDD =
    new CubeSchemaRDD(this, CarbonEnv.getInstance(this).carbonCatalog.lookupRelation1(Option(""), cubeName)(this))

  /** Caches the specified table in-memory. */
  override def cacheTable(tableName: String): Unit = {
    //todo:
  }

  experimental.extraStrategies = CarbonStrategy.getStrategy(self) :: Nil

  @transient
  val olapPlanner = new SparkPlanner with HuaweiStrategies {
    val olapContext = self
    val hiveContext = self

    //    override val strategies: Seq[Strategy] = Seq(
    //      CommandStrategy(self),
    //      TakeOrdered,
    //      ParquetOperations,
    //      OlapCubeScans,
    //      HashAggregation,
    //      LeftSemiJoin,
    //      HashJoin,
    //      InMemoryScans,
    //      BasicOperators,
    //      CartesianProduct,
    //      BroadcastNestedLoopJoin
    //    )

    experimental.extraStrategies = CarbonStrategy.getStrategy(self) :: Nil
    //    override val strategies: Seq[Strategy] =
    //      experimental.extraStrategies ++ Seq(
    //      DataSourceStrategy,
    //      DummySparkPlanner.getStrategy(self),
    //      HiveCommandStrategy(self),
    //      HiveDDLStrategy,
    //      DDLStrategy,
    //      TakeOrderedAndProject,
    ////      ParquetOperations,
    //      InMemoryScans,
    ////      ParquetConversion, // Must be before HiveTableScans
    //      HiveTableScans,
    //      DataSinks,
    //      Scripts,
    //      HashAggregation,
    //      LeftSemiJoin,
    //      EquiJoinSelection,
    //      BasicOperators,
    //      HuaweiStrategy,
    //      CartesianProduct,
    //      BroadcastNestedLoopJoin)
  }
  //    val strategies1: Seq[Strategy] =
  //      experimental.extraStrategies ++ (
  //      //CommandStrategy(self) ::
  //      DataSourceStrategy ::
  //      OlapCubeScans ::
  //      TakeOrdered ::
  //      HashAggregation ::
  //      LeftSemiJoin ::
  //      HashJoin ::
  //      InMemoryScans ::
  //      ParquetOperations ::
  //      BasicOperators ::
  //      CartesianProduct ::
  //      BroadcastNestedLoopJoin :: Nil)
  //  }

  //  @transient
  //  override protected[sql] val planner  = olapPlanner

  //  abstract class QueryExecution extends super.QueryExecution {
  //  }

  /**
    * Loads from JDBC, returning the ResultSet as a [[SchemaRDD]].
    * It gets MetaData from ResultSet of PreparedStatement to determine the schema.
    *
    * @group userf
    */
  def jdbcResultSet(
                     connectString: String,
                     sql: String): SchemaRDD = {
    jdbcResultSet(connectString, "", "", sql, 0, 0, 1)
  }

  def jdbcResultSet(
                     connectString: String,
                     username: String,
                     password: String,
                     sql: String): SchemaRDD = {
    jdbcResultSet(connectString, username, password, sql, 0, 0, 1)
  }

  def jdbcResultSet(
                     connectString: String,
                     sql: String,
                     lowerBound: Long,
                     upperBound: Long,
                     numPartitions: Int): SchemaRDD = {
    jdbcResultSet(connectString, "", "", sql, lowerBound, upperBound, numPartitions)
  }

  def jdbcResultSet(
                     connectString: String,
                     username: String,
                     password: String,
                     sql: String,
                     lowerBound: Long,
                     upperBound: Long,
                     numPartitions: Int): SchemaRDD = {
    val resultSetRDD = new JdbcRDDExt(
      sparkContext,
      () => {
        DriverManager.getConnection(connectString, username, password)
      },
      sql, lowerBound, upperBound, numPartitions,
      (r: ResultSet) => r
    )
    //new SchemaRDD(this, JdbcResultSetRDD.inferSchema(resultSetRDD))
    val appliedSchema = JdbcResultSetRDD.inferSchema(resultSetRDD)
    val rowRDD = JdbcResultSetRDD.jdbcResultSetToRow(resultSetRDD, appliedSchema)
    applySchema1(rowRDD, appliedSchema)
  }

  def applySchema1(rowRDD: RDD[InternalRow], schema: StructType): DataFrame = {
    // TODO: use MutableProjection when rowRDD is another SchemaRDD and the applied
    // schema differs from the existing schema on any field data type.
    val attributes = schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
    val logicalPlan = LogicalRDD(attributes, rowRDD)(this)
    new DataFrame(this, logicalPlan)
  }


}

object OlapContext {
  /**
    * @param schemaName - Schema Name
    * @param cubeName   - Cube Name
    * @param factPath   - Raw CSV data path
    * @param targetPath - Target path where the file will be split as per partition
    * @param delimiter  - default file delimiter is comma(,)
    * @param quoteChar  - default quote character used in Raw CSV file, Default quote
    *                   character is double quote(")
    * @param fileHeader - Header should be passed if not available in Raw CSV File, else pass null, Header will be read from CSV
    * @param escapeChar - This parameter by default will be null, there wont be any validation if default escape
    *                   character(\) is found on the RawCSV file
    * @param multiLine  - This parameter will be check for end of quote character if escape character & quote character is set.
    *                   if set as false, it will check for end of quote character within the line and skips only 1 line if end of quote not found
    *                   if set as true, By default it will check for 10000 characters in multiple lines for end of quote & skip all lines if end of quote not found.
    */
  final def partitionData(
                           schemaName: String = null,
                           cubeName: String,
                           factPath: String,
                           targetPath: String,
                           delimiter: String = ",",
                           quoteChar: String = "\"",
                           fileHeader: String = null,
                           escapeChar: String = null,
                           multiLine: Boolean = false)(hiveContext: HiveContext): String = {
    updateMolapPorpertiesPath(hiveContext)
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = "default"
    }
    val partitionDataClass = PartitionData(schemaName, cubeName, factPath, targetPath, delimiter, quoteChar, fileHeader, escapeChar, multiLine)
    partitionDataClass.run(hiveContext)
    partitionDataClass.partitionStatus
  }

  final def updateMolapPorpertiesPath(hiveContext: HiveContext) {
    val molapPropertiesFilePath = hiveContext.getConf("molap.properties.filepath", null)
    val systemmolapPropertiesFilePath = System.getProperty("molap.properties.filepath", null);
    if (null != molapPropertiesFilePath && null == systemmolapPropertiesFilePath) {
      System.setProperty("molap.properties.filepath", molapPropertiesFilePath + "/" + "molap.properties")
    }
  }

}
