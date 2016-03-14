package com.huawei.datasight.testsuite.filterexpr;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.huawei.datasight.common.util.CreateCubeUtils;
import com.huawei.datasight.common.util.DropCubeUtils;
import com.huawei.datasight.common.util.LoadDataUtils;
import com.huawei.datasight.common.util.QueryPlanUtils;
import com.huawei.datasight.common.util.QueryUtils;
import com.huawei.datasight.molap.load.MolapLoadModel;
import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * Test Class for filter expression query on timestamp datatypes
 * @author N00902756
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TimestampDataTypeTestCase {

	/**
	 * Runs before class execution starts
	 * @throws Exception
	 */
	@BeforeClass
    public static void setUpBeforeClass() throws Exception 
	{
		Configuration hadoopConf = new Configuration();
		hadoopConf.addResource(new Path("../core-default.xml"));
		hadoopConf.addResource(new Path("core-site.xml"));
		
		String hdfsCarbonPath = hadoopConf.get("fs.defaultFS", "./") + "/opt/carbon/test/";
		
		MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION_HDFS, hdfsCarbonPath);
		MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION, hdfsCarbonPath);
		MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_BADRECORDS_LOC, hdfsCarbonPath + "/badrecords");
		MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"));
		MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT, "dd-MM-yyyy");
		MolapProperties.getInstance().addProperty("molap.kettle.home", "../../Molap/Molap-Data-Processor/molapplugins/molapplugins");
		MolapProperties.getInstance().addProperty("molap.testdata.path", "./TestData/");
		
		CreateCubeUtils.createCube("default", "timestamptypecube", "empno");
		MolapLoadModel model = LoadDataUtils.prepareLoadModel("default", "timestamptypecube", 0+"");
		LoadDataUtils.loadCube(model, 0);
		QueryPlanUtils.setSchemaCube("default", "timestamptypecube");
    }
	
   @Before
   public void setUp() throws Exception {
	   // If some per condition to be run before each test method, add here
   }
   
   @Test
   public void testSelectDimension() throws Exception {	
	   
	   //select doj from cube
	   
	   //Preparation
	   MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();
	   
	   int queryOrder=0;
       
       QueryPlanUtils.addDimension(plan, "doj", queryOrder);
	   queryOrder = queryOrder + 1;
       
       MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);
       
       //Execution
       MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);
	   
	   //Validation
	   List<String> expectedOutput = new LinkedList<String>();
       expectedOutput.add("17-01-2007");
	   expectedOutput.add("29-05-2008");
       expectedOutput.add("07-07-2009");
       expectedOutput.add("29-12-2010");
       expectedOutput.add("09-11-2011");
       expectedOutput.add("14-10-2012");
       expectedOutput.add("22-09-2013");
       expectedOutput.add("15-08-2014");
       expectedOutput.add("12-05-2015");
       expectedOutput.add("01-12-2015");

       QueryUtils.validateTimestampQueryOutput(rowIterator, expectedOutput);
   }
        
   @After
   public void tearDown() throws Exception {
	// If some post condition to be run after each test method, add here
   }
   
   /**
	 * Runs After class execution Ends
	 * @throws Exception
	 */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  	System.out.println("tearing down");
  	DropCubeUtils.dropCube("default", "timestamptypecube");
  }
}