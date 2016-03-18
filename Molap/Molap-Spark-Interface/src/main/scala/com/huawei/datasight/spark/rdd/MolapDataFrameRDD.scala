package com.huawei.datasight.spark.rdd

import com.huawei.unibi.molap.engine.querystats.{QueryDetail, QueryStatsCollector}
import org.apache.spark.sql.{DataFrame, OlapContext, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class MolapDataFrameRDD(val sql: String, val sc: OlapContext, logicalPlan: LogicalPlan) extends DataFrame(sc, logicalPlan) {

  override def collect(): Array[Row] = {

    //Creating query stats
    val queryId: String = sc.getConf("queryId", System.nanoTime() + "")
    val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
    val queryDetail: QueryDetail = queryStatsCollector.getQueryStats(queryId)
    val startTime = System.currentTimeMillis();

    // executing the query
    val row: Array[Row] = super.collect()


    // if query is for carbon than only log it
    val timeTaken = System.currentTimeMillis() - startTime;
    if (null != queryDetail) {

      queryDetail.setQueryStartTime(startTime)
      queryDetail.setTotalExecutionTime(timeTaken)
      queryDetail.setRecordSize(row.length)
      val partAcc = queryDetail.getPartitionsDetail
      if (null != partAcc && null != partAcc.value) {
        queryDetail.setNoOfNodesScanned(partAcc.value.getNumberOfNodesScanned)
        queryDetail.setNoOfRowsScanned(partAcc.value.getNoOfRowsScanned)
      }


      queryStatsCollector.logQueryStats(queryDetail);
    }
    //remove this query from cache
    queryStatsCollector.removeQueryStats(queryId)

    //result
    row

  }

}