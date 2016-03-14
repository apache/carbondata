package org.apache.spark.sql.common.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import com.huawei.unibi.molap.util.MolapProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.datasight.molap.load.MolapLoaderUtil
	
class LocalSQLContext(hdfsCarbonBasePath :String)
  extends HiveContext(new SparkContext(new SparkConf()
  	 .setAppName("CarbonSpark")
  	 .setMaster("local[2]")
     .set("carbon.storelocation", hdfsCarbonBasePath)
     .set("molap.kettle.home", "../../Molap/Molap-Data-Processor/molapplugins/molapplugins")
     .set("molap.is.columnar.storage", "true")
     .set("spark.sql.bigdata.register.dialect", "org.apache.spark.sql.MolapSqlDDLParser")
     .set("spark.sql.bigdata.register.strategyRule", "org.apache.spark.sql.hive.CarbonStrategy")
     .set("spark.sql.bigdata.initFunction", "org.apache.spark.sql.CarbonEnv")
     .set("spark.sql.bigdata.acl.enable", "false")
     .set("molap.tempstore.location", System.getProperty("java.io.tmpdir"))
     .set("spark.sql.bigdata.register.strategy.useFunction", "true")
     .set("hive.security.authorization.enabled","false")
     .set("spark.sql.bigdata.register.analyseRule","org.apache.spark.sql.QueryStatsRule")
     .set("spark.sql.dialect", "hiveql"))) {

}

object CarbonHiveContext extends LocalSQLContext(
    {
		val hadoopConf = new Configuration();
		hadoopConf.addResource(new Path("../core-default.xml"));
		hadoopConf.addResource(new Path("core-site.xml"));
		val hdfsCarbonPath = hadoopConf.get("fs.defaultFS", "./") + "/opt/carbon/test/";
		hdfsCarbonPath
    })
{
  
	{
	    MolapProperties.getInstance().addProperty("molap.kettle.home","../../Molap/Molap-Data-Processor/molapplugins/molapplugins")
	    MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT, "dd-MM-yyyy")
	    MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
	    
	    val hadoopConf = new Configuration();
		hadoopConf.addResource(new Path("../core-default.xml"));
		hadoopConf.addResource(new Path("core-site.xml"));
		val hdfsCarbonPath = hadoopConf.get("fs.defaultFS", "./") + "/opt/carbon/test/";
		
		MolapLoaderUtil.deleteStorePath(hdfsCarbonPath)
//	    //		sql("drop cube timestamptypecube");
	}
}


