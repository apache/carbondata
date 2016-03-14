package org.apache.spark.sql

import org.apache.spark.sql.execution.Project
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.UnaryNode
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics

import com.huawei.unibi.molap.engine.querystats.QueryStatsCollector
import com.huawei.unibi.molap.engine.querystats.QueryDetail
import org.apache.spark.sql.catalyst.errors.attachTree
import com.huawei.iweb.platform.logging.LogServiceFactory

/**
  * @author A00902717
  */

/**
  * This plan is used by select queries to collect statistics for each query
  */
case class QueryStatsSparkPlan(child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {

    child.execute()
  }

  override def executeCollect(): Array[Row] = {
    //queryId will be unique for each query, creting query detail holder
    val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
    val queryId: String = System.nanoTime() + ""
    val queryDetail: QueryDetail = new QueryDetail(queryId)
    queryStatsCollector.addQueryStats(queryId, queryDetail)
    sqlContext.setConf("queryId", queryId)

    val startTime = System.currentTimeMillis();
    //executing query
    val rows: Array[Row] = super.executeCollect()

    val endTime = System.currentTimeMillis()

    //log query data
    logQueryDetails(queryId, startTime, endTime, rows)

    //return the result
    rows
  }

  def logQueryDetails(queryId: String, startTime: Long, endTime: Long, rows: Array[Row]) {
    val LOGGER = LogServiceFactory.getLogService(QueryStatsSparkPlan.getClass().getName());
    try {
      val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
      val queryDetail: QueryDetail = queryStatsCollector.getQueryStats(queryId)
      val timeTaken = System.currentTimeMillis() - startTime;

      queryDetail.setQueryStartTime(startTime)
      queryDetail.setTotalExecutionTime(timeTaken)
      queryDetail.setRecordSize(rows.length)
      val partAcc = queryDetail.getPartitionsDetail
      if (null != partAcc && null != partAcc.value) {
        queryDetail.setNoOfNodesScanned(partAcc.value.getNumberOfNodesScanned)
        queryDetail.setNoOfRowsScanned(partAcc.value.getNoOfRowsScanned)
        //log query stats only if there is dimensions included in query
        if (null != queryDetail.getDimOrdinals && queryDetail.getDimOrdinals.length > 0) {
          queryStatsCollector.logQueryStats(queryDetail);
        }
      }

      //remove this query from cache
      queryStatsCollector.removeQueryStats(queryId)
    }
    catch {
      case e: Exception => LOGGER.audit("Error in logging query details" + e.getMessage)
    }


  }

  override protected def doPrepare(): Unit = {
    super.prepare()
  }

}