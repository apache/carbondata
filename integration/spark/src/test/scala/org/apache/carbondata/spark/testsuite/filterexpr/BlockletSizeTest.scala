package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.util.CarbonProperties

class BlockletSizeTest extends QueryTest with BeforeAndAfterAll {
  
  
  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty("carbon.blocklet.size", "50")
     sql("CREATE TABLE all_carbon_blockletsize (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO TABLE all_carbon_blockletsize OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    
    sql("CREATE TABLE all_hives_blockletsize (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO TABLE all_hives_blockletsize");

  }
      test("select empno,empname from alldatatypescubeFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development' for diff blocklet sizes") {
    checkAnswer(
      sql("select empno,empname from all_carbon_blockletsize where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'"),
      sql("select empno,empname from all_hives_blockletsize where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'"))
        }
  override def afterAll {

    sql("drop table all_carbon_blockletsize")
    sql("drop table all_hives_blockletsize")
  }
  
}