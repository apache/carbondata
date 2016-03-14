package com.huawei.datasight.testsuite.detailquery;

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
import com.huawei.datasight.molap.query.metadata.MolapMeasure.AggregatorType;
import com.huawei.datasight.molap.query.metadata.SortOrderType;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * Test Class for detailed query on multiple datatypes
 * @author N00902756
 *
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AllDataTypesTestCase {
	
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
		
		CreateCubeUtils.createCube("default", "alldatatypescube", "empname");
		MolapLoadModel model = LoadDataUtils.prepareLoadModel("default", "alldatatypescube", 0+"");
		LoadDataUtils.loadCube(model, 0);
		QueryPlanUtils.setSchemaCube("default", "alldatatypescube");
    }

   @Before
   public void setUp() throws Exception {
	   // If some per condition to be run before each test method, add here
   }
   
   @Test
   public void testFilterAndAggregates() throws Exception {	
	   //select empno,empname,utilization,count(salary),sum(empno) from cube where empname in ('arvind','ayushi') limit 100
	   
	   //Preparation
	   MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();
	   
	   int queryOrder=0;
	   QueryPlanUtils.addDimension(plan, "empno", queryOrder);
       queryOrder = queryOrder + 1;
       
       QueryPlanUtils.addDimensionWithSort(plan, "empname", SortOrderType.DSC, queryOrder);
	   queryOrder = queryOrder + 1;
       
	   QueryPlanUtils.addMeasure(plan, "utilization", queryOrder);
       queryOrder = queryOrder + 1;
       
       QueryPlanUtils.addMeasureWithAgg(plan, "salary", AggregatorType.COUNT, queryOrder);
       queryOrder = queryOrder + 1;
       
       QueryPlanUtils.addAggOnDimension(plan, "empno", "sum", queryOrder);
       queryOrder = queryOrder + 1;
     
       QueryPlanUtils.setQueryLimit(plan, 100);
       
       QueryPlanUtils.isDetailedQuery(plan, false);

       Cube cube = MolapMetadata.getInstance().getCube(plan.getSchemaName()+"_"+plan.getCubeName());
       List<Object> values = new LinkedList<Object>();
       values.add("arvind");
       values.add("ayushi");
       QueryPlanUtils.addInFilter(plan, "empname", values, cube.getDimension("empname"), DataType.StringType);

       MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);
       
       //Execution
       MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);
	   
       //Validation
       List<String> expectedOutput = new LinkedList<String>();
       expectedOutput.add("11,arvind,96.2,1.0,11.0");
       expectedOutput.add("15,ayushi,91.5,1.0,15.0");
       
       QueryUtils.validateQueryOutput(rowIterator, expectedOutput);
   }
   
   @Test
   public void testCountOnDimension() throws Exception { 
	   //select dept,count(dept) from cube group by dept

	   //Preparation
	   MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();
	   
	   int queryOrder=0;
	      
	   QueryPlanUtils.addDimension(plan, "deptname", queryOrder);
	   queryOrder = queryOrder + 1;
	   
	   QueryPlanUtils.addAggOnDimension(plan, "deptname", "count", queryOrder);
       queryOrder = queryOrder + 1;
       
       QueryPlanUtils.isDetailedQuery(plan, false);
	   
	   MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);
	   
	   //Execution
       MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);
	   
	   //Validation
       List<String> expectedOutput = new LinkedList<String>();
       expectedOutput.add("network,3.0");
       expectedOutput.add("protocol,2.0");
       expectedOutput.add("configManagement,1.0");
       expectedOutput.add("security,2.0");
       expectedOutput.add("Learning,2.0");
       
       QueryUtils.validateQueryOutput(rowIterator, expectedOutput);
   }
   
   @After
   public void tearDown() throws Exception {
	// If some post condition to be run after each test method, add here
   }
   
   @AfterClass
   public static void tearDownAfterClass() throws Exception {
   	System.out.println("tearing down");
   	DropCubeUtils.dropCube("default", "alldatatypescube");
   }
}