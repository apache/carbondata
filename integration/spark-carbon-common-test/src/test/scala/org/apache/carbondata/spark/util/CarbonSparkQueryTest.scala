package org.apache.carbondata.spark.util

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, DataFrame}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest

class CarbonSparkQueryTest extends QueryTest {

  /**
   * check whether the pre-aggregate tables are in DataFrame
   *
   * @param df DataFrame
   * @param exists whether the preAggTableNames exists
   * @param preAggTableNames preAggTableNames
   */
  def checkPreAggTable(df: DataFrame, exists: Boolean, preAggTableNames: String*): Unit = {
    val plan = df.queryExecution.analyzed
    for (preAggTableName <- preAggTableNames) {
      var isValidPlan = false
      plan.transform {
        // first check if any preaTable1 scala function is applied it is present is in plan
        // then call is from create preaTable1regate table class so no need to transform the query plan
        case ca: CarbonRelation =>
          if (ca.isInstanceOf[CarbonDatasourceHadoopRelation]) {
            val relation = ca.asInstanceOf[CarbonDatasourceHadoopRelation]
            if (relation.carbonTable.getTableName.equalsIgnoreCase(preAggTableName)) {
              isValidPlan = true
            }
          }
          ca
        case logicalRelation: LogicalRelation =>
          if (logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
            val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
            if (relation.carbonTable.getTableName.equalsIgnoreCase(preAggTableName)) {
              isValidPlan = true
            }
          }
          logicalRelation
      }

      if (exists != isValidPlan) {
        assert(false)
      } else {
        assert(true)
      }
    }
  }

}
