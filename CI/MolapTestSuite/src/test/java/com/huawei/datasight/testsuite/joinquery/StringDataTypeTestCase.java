package com.huawei.datasight.testsuite.joinquery;

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
 * Test Class for join query on String datatypes
 * @author N00902756
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StringDataTypeTestCase {

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
		
		CreateCubeUtils.createCube("default", "stringtypecube", "empname");
		MolapLoadModel model = LoadDataUtils.prepareLoadModel("default", "stringtypecube", 0+"");
		LoadDataUtils.loadCube(model, 0);
		QueryPlanUtils.setSchemaCube("default", "stringtypecube");
    }
	
   @Before
   public void setUp() throws Exception {
	   // If some per condition to be run before each test method, add here
   }
   
   @Test
   public void testSelectDimension() throws Exception {	
	   
	   //select empname from cube
	   
	   //Preparation
	   MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();
	   
	   int queryOrder=0;
       
       QueryPlanUtils.addDimension(plan, "empname", queryOrder);
	   queryOrder = queryOrder + 1;
       
       MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);
       
       //Execution
       MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);
	   
	   //Validation
	   List<String> expectedOutput = new LinkedList<String>();
       expectedOutput.add("arvind");
       expectedOutput.add("krithin");
       expectedOutput.add("madhan");
       expectedOutput.add("anandh");
       expectedOutput.add("ayushi");
       expectedOutput.add("pramod");
       expectedOutput.add("gawrav");
       expectedOutput.add("sibi");
       expectedOutput.add("shivani");
       expectedOutput.add("bill");
       
       QueryUtils.validateQueryOutput(rowIterator, expectedOutput);
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
  	DropCubeUtils.dropCube("default", "stringtypecube");
  }
}