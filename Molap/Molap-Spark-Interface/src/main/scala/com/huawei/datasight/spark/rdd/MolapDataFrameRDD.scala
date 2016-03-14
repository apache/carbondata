package com.huawei.datasight.spark.rdd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.OlapContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import com.huawei.unibi.molap.engine.querystats.QueryStatsCollector
import com.huawei.unibi.molap.engine.querystats.QueryDetail
import com.huawei.unibi.molap.engine.querystats.PartitionDetail
import com.huawei.unibi.molap.engine.querystats.PartitionAccumulator
import com.huawei.unibi.molap.engine.querystats.PartitionStatsCollector
import org.apache.spark.Accumulator
import java.util.ArrayList

/**
  * This class wraps DataFrame
  *
  * @author A00902717
  */
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